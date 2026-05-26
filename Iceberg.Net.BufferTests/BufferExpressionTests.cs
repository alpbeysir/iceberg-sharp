using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using AwesomeAssertions;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.Arrow;
using Iceberg.Net.Query.Expressions;
using Iceberg.Net.Query.FastArrow;
using Iceberg.Net.Schemas;
using Varena;
using ZLinq;
using ExecutionContext = Iceberg.Net.Query.Expressions.ExecutionContext;

namespace Iceberg.Net.Tests;

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

public record TestCase(LambdaExpression Expr, string Desc);

public class BufferExpressionTests
{
    private static readonly List<TestRow> Rows =
    [
        new() { A = 5, B = 10.0, L = [1, 2, 3], LNest = [[78, 79], [80, 81]], N = new TestNested { C = 100 } },
        new() { A = 20, B = 20.0, L = [42], LNest = [[78, 79], [80, 81]], N = new TestNested { C = 200 } },
        new() { A = 3, B = 7.0, L = [1, 1, 1], LNest = [[78, 79], [80, 81]], N = new TestNested { C = 400 } },
        new() { A = 50, B = -5.0, L = [], LNest = [[78, 79], [80, 81]], N = new TestNested { C = 300 } },
        new() { A = 0, B = 0.0, L = [0, 0], LNest = [[78, 79], [80, 81]], N = new TestNested { C = 500 } }
    ];

    // preserves anonymous return types (no cast to object)
    private static LambdaExpression L<T, T2>(Expression<Func<T, T2>> expr)
    {
        return expr;
    }

    public static TheoryData<TestCase> Expressions()
    {
        var data = new TheoryData<TestCase>();
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
                L((TestRow str) =>
                    str.A > 300 &&
                    str.B < 500.0 &&
                    str.N.C > 12 &&
                    str.L.Select(n => n + 4).All(n => n > 5) &&
                    str.L.Any(n => n == 7)
                ),
                "complex boolean with list ops"),
            new TestCase(
                L((TestRow str) => str.L.Select(n => n + 3).GroupBy(n => n)),
                "group by"));
        return data;
    }

    [Theory]
    [MemberData(nameof(Expressions))]
    public void Execute_arrow_matches_linq(TestCase testCase)
    {
        var method = typeof(BufferExpressionTests)
            .GetMethod(nameof(Execute), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(TestRow), testCase.Expr.ReturnType);

        Console.WriteLine(testCase.Desc);
        method.Invoke(null, [testCase.Expr, Rows]);
    }

    private static void Execute<T, T2>(LambdaExpression expr, List<T> input)
    {
        var manager = new VirtualArenaManager();
        var buffer = manager.CreateBuffer("default", 1000_000_000);
        var allocator = new UnsafeArenaMemoryAllocator(buffer);
        var ctx = new ExecutionContext { Arena = buffer, ArrowAllocator = allocator };
        var inputBatch = ArrowFfiBridge.BuildRecordBatch(input).AsStructArray();

        BufferTransformVisitor visitor = new();
        var result = visitor.Visit(expr);

        var compiled = ((LambdaExpression)result).Compile();
        var outputType = ArrowSchema.FromIcebergType(CSharpSchema.ToIcebergType(typeof(T2), s => -1, ""));

        var builder = ArrowCompute.MakeBuilderFor(outputType, allocator);
        using (new MeasureTime("arrow"))
        {
            compiled.DynamicInvoke(ctx, inputBatch, builder);
        }

        ctx.Arena.Dispose();

        IArrowArray output = ((dynamic)builder).Build();
        var arrowResult = ArrowReader.ReadRecordBatch<T2>(output);

        var linqRunner = (Func<T, T2>)expr.Compile();
        List<T2> linqResult;
        using (new MeasureTime("linq"))
        {
            linqResult = input.AsValueEnumerable().Select(linqRunner).ToList();
        }

        arrowResult.Should().BeEquivalentTo(linqResult);
    }
}