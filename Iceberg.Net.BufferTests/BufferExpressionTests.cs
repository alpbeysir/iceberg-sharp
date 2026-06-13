using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using AwesomeAssertions;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.Arrow;
using Iceberg.Net.Query.Expressions;
using Iceberg.Net.Query.FastArrow;
using Iceberg.Net.Schemas;
using Iceberg.Net.Tests;
using Varena;
using ExecutionContext = Iceberg.Net.Query.Expressions.ExecutionContext;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Iceberg.Net.BufferTests;

public partial record TestNested
{
    public int C { get; set; }
}

public partial record TestRow
{
    public int? A { get; set; }
    public double B { get; set; }
    public IReadOnlyList<int> L { get; set; } = [];
    public IReadOnlyList<IReadOnlyList<int>> LNest { get; set; } = [];
    public TestNested N { get; set; } = new();
}

public record TestCase(LambdaExpression Expr, string Desc)
{
    public override string ToString()
    {
        return Desc;
    }
}


public class BufferExpressionTests
{
    private static readonly List<TestRow> Rows = TestData.GenerateRows();
    
    private static LambdaExpression L<T, T2>(Expression<Func<T, T2>> expr)
    {
        return expr;
    }

    public static TheoryData<TestCase> Expressions()
    {
        TheoryData<TestCase> data = [];
        data.AddRange(
            new TestCase(
                L((TestRow str) => new { b = str.B, c = str.N.C }),
                "simple projection"),
            new TestCase(
                L((TestRow str) => new { b = str.B, nested = new { c = str.N.C + 3 } }),
                "nested struct"),
            new TestCase(
                L((TestRow str) => str.N.C + 3),
                "struct-field add constant"),
            new TestCase(
                L((TestRow str) => str.L.Select(n => n + 3)),
                "list select"),
            new TestCase(
                L((TestRow str) => str.L.Any(n => n == 3)),
                "list any"),
            new TestCase(
                L((TestRow str) => str.L.All(n => n < 3)),
                "list all"),
            new TestCase(
                L((TestRow str) => str.LNest.Select(n => n.All(n2 => n2 > 127))),
                "list nested select all"),
            new TestCase(
                L((TestRow str) => str.LNest.Select(n => n.Any(n2 => n2 > 350))),
                "list nested select any"),
            new TestCase(
                L((TestRow str) => str.LNest.Select(n => n.Select(n2 => n2 + 350))),
                "list nested select select"),
            new TestCase(
                L((TestRow str) => str.LNest.Any(n => n.Any(n2 => n2 == 350))),
                "list nested any any"),
            new TestCase(
                L((TestRow str) => str.LNest.All(n => n.All(n2 => n2 == str.N.C))),
                "list nested all all"),
            new TestCase(
                L((TestRow str) =>
                    str.A > 300 &&
                    str.B < 500.0 &&
                    str.N.C > 12 &&
                    str.L.Select(n => n + 4).All(n => n > 5) &&
                    str.L.Any(n => n == 7)
                ),
                "complex boolean with list ops"),
            new TestCase(
                L((TestRow str) => str.L.Where(n => n > 500)),
                "list where"),
            new TestCase(
                L((TestRow str) => new { Filtered = str.L.Where(n => n > 500) }),
                "list where in projection"),
            new TestCase(
                L((TestRow str) => str.L.Where(n => n > 100 && n < 900)),
                "list where with boolean and"),
            new TestCase(
                L((TestRow str) => str.L.Where(n => n > str.N.C)),
                "list where with struct field"),
            new TestCase(
                L((TestRow str) => str.LNest.Select(n => n.Where(n2 => n2 > 500))),
                "list nested select where"),
            new TestCase(
                L((TestRow str) => new
                {
                    Low = str.L.Where(n => n < 300),
                    High = str.L.Where(n => n > 700)
                }),
                "multiple list where in projection"),
            new TestCase(
                L((TestRow str) => str.LNest.Select(n => n.Where(n2 => n2 > 100 && n2 < 900))),
                "list nested select where with boolean and"),
            new TestCase(
                L((TestRow str) => new
                {
                    Filtered = str.L.Where(n => n > 400),
                    Doubled = str.L.Select(n => n * 2)
                }),
                "list where and select in projection")
            // new TestCase(
            //     L((TestRow str) => str.L.Select(n => n + 3).GroupBy(n => n)),
            //     "group by")
        );
        return data;
    }

    [Theory]
    [MemberData(nameof(Expressions))]
    public void Execute_arrow_matches_linq(TestCase testCase)
    {
        Console.WriteLine(testCase.Desc);
        Run(testCase.Expr, Rows);
    }

    private static void Run(LambdaExpression expr, List<TestRow> list)
    {
        Console.WriteLine($"{expr}");

        Delegate arrow = CompileArrow(expr);
        Delegate linq = CompileLinq(expr);
        using StructArray inputBatch = ArrowFfiBridge.BuildRecordBatch(list).AsStructArray();
        MethodInfo method = typeof(BufferExpressionTests).GetMethod(nameof(Execute))!
            .MakeGenericMethod(typeof(TestRow), expr.ReturnType);
        for (var i = 0; i < 2; i++) method.Invoke(null, [linq, arrow, list, inputBatch]);
    }

    public static void Execute<T, T2>(
        Delegate linqCompiled,
        Delegate arrowCompiled,
        List<T> input,
        StructArray structArray)
    {
        using VirtualArenaManager manager = new();
        using VirtualBuffer buffer = manager.CreateBuffer("default", 4_000_000_000);
        UnsafeArenaMemoryAllocator allocator = new(buffer);
        ExecutionContext ctx = new() { Arena = buffer, ArrowAllocator = allocator };

        IArrowType outputType = ArrowSchema.FromIcebergType(CSharpSchema.ToIcebergType(typeof(T2), s => -1, ""));

        IArrowArrayBuilder<IArrowArray> builder = ArrowCompute.MakeBuilderFor(outputType, allocator);
        using (new MeasureTime("arrow"))
        {
            arrowCompiled.DynamicInvoke(ctx, new IdentityInput(structArray), builder);
        }

        Console.WriteLine($"arena used: {Utils.ToFileSize(buffer.AllocatedBytes)}");

        using IArrowArray output = ((dynamic)builder).Build();

        Func<T, T2> linqRunner = (Func<T, T2>)linqCompiled;
        List<T2> linqResult;
        using (new MeasureTime("linq"))
        {
            linqResult = input.Select(linqRunner).ToList();
        }

        IEnumerable<T2> arrowResult = ArrowReader.ReadRecordBatch<T2>(output);
        arrowResult.Should().BeEquivalentTo(linqResult);
    }

    private static Delegate CompileLinq(LambdaExpression expr)
    {
        return expr.Compile();
    }

    private static Delegate CompileArrow(LambdaExpression expr)
    {
        BufferTransformVisitor visitor = new();
        Expression? result = visitor.Visit(expr);
        Delegate? compiled = ((LambdaExpression)result).Compile();
        return compiled;
    }
}