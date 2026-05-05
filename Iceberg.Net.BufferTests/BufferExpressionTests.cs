using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using AwesomeAssertions;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.Arrow;
using Iceberg.Net.Query.Expressions;
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
    public List<int> L { get; set; } = [];
    public List<List<int>> LNest { get; set; } = [];
    public TestNested N { get; set; } = new();
}

public record TestCase(LambdaExpression Expr, string Desc);

public class BufferExpressionTests
{
    private static readonly List<TestRow> Rows =
    [
        new() { A = 5, B = 10.0, L = [1, 2, 3], N = new TestNested { C = 100 } },
        new() { A = 20, B = 20.0, L = [42], N = new TestNested { C = 200 } },
        new() { A = 3, B = 7.0, L = [1, 1, 1], N = new TestNested { C = 400 } },
        new() { A = 50, B = -5.0, L = [], N = new TestNested { C = 300 } },
        new() { A = 0, B = 0.0, L = [0, 0], N = new TestNested { C = 500 } }
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
                L((TestRow str) =>
                    str.A > 300 &&
                    str.B < 500.0 &&
                    str.N.C > 12 &&
                    str.L.Select(n => n + 3).All(n => n > 5) &&
                    str.L.Contains(65) &&
                    str.L.Any(n => n == 7)
                ),
                "complex boolean with list ops"));
        return data;
    }

    [Theory]
    [MemberData(nameof(Expressions))]
    public void Execute_arrow_matches_linq(TestCase testCase)
    {
        var method = typeof(BufferExpressionTests)
            .GetMethod(nameof(Execute), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(TestRow), testCase.Expr.ReturnType);

        var success = (bool)method.Invoke(null, [testCase.Expr, Rows])!;
        success.Should().BeTrue($"results should match for '{testCase.Desc}'");
    }

    // ── same pattern as BufferExpressions.Execute ────────────────

    private static bool Execute<T, T2>(LambdaExpression expr, List<T> input)
    {
        var manager = new VirtualArenaManager();
        var ctx = new ExecutionContext { Arena = manager.CreateBuffer("default", 100_000_000) };
        var inputBatch = ArrowFfiBridge.BuildRecordBatch(input).AsStructArray();

        BufferTransformVisitor visitor = new();
        var result = visitor.Visit(expr);

        var compiled = ((LambdaExpression)result).Compile();
        var outputType = ArrowSchema.FromIcebergType(CSharpSchema.ToIcebergType(typeof(T2), s => -1, ""));

        var builder = ArrowCompute.MakeBuilderFor(outputType);
        using (new MeasureTime("arrow"))
        {
            compiled.DynamicInvoke(ctx, inputBatch, builder);
        }

        IArrowArray output = ((dynamic)builder).Build();
        var arrowResult = ArrowReader.ReadRecordBatch<T2>(output);

        var linqRunner = (Func<T, T2>)expr.Compile();
        List<T2> linqResult;
        using (new MeasureTime("linq"))
        {
            linqResult = input.AsValueEnumerable().Select(linqRunner).ToList();
        }

        return arrowResult.SequenceEqual(linqResult);
    }
}