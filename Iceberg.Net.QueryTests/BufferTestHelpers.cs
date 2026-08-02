using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using AwesomeAssertions;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.Arrow;
using Iceberg.Net.Query.Expressions;
using Iceberg.Net.Query.FastArrow;
using Iceberg.Net.Schemas;
using Varena;
using ExecutionContext = Iceberg.Net.Query.Expressions.ExecutionContext;

namespace Iceberg.Net.QueryTests;

public static class BufferTestHelpers
{
    public static LambdaExpression L<T, T2>(Expression<Func<T, T2>> expr) => expr;

    public static void RunTest<T>(LambdaExpression expr, IReadOnlyList<T> rows, int iterations = 2)
    {
        Delegate arrow = CompileArrow(expr);
        Delegate linq = CompileLinq(expr);
        using StructArray inputBatch = ArrowFfiBridge.BuildRecordBatch(rows.Cast<object>().ToList()).AsStructArray();
        MethodInfo method = typeof(BufferTestHelpers).GetMethod(nameof(Execute))!
            .MakeGenericMethod(typeof(T), expr.ReturnType);
        for (int i = 0; i < iterations; i++)
            method.Invoke(null, [linq, arrow, rows, inputBatch]);
    }

    public static void Execute<T, T2>(
        Delegate linqCompiled,
        Delegate arrowCompiled,
        IReadOnlyList<T> input,
        StructArray structArray)
    {
        using VirtualArenaManager manager = new();
        using VirtualBuffer buffer = manager.CreateBuffer("default", 4_000_000_000);
        UnsafeArenaMemoryAllocator allocator = new(buffer);
        ExecutionContext ctx = new() { Arena = buffer, ArrowAllocator = allocator };

        Type resultType = Nullable.GetUnderlyingType(typeof(T2)) ?? typeof(T2);
        IArrowType outputType = ArrowSchema.FromIcebergType(CSharpSchema.ToIcebergType(resultType, _ => -1, ""));

        IArrowArrayBuilder<IArrowArray> builder = ArrowCompute.MakeBuilderFor(outputType, allocator);

        arrowCompiled.DynamicInvoke(ctx, new IdentityInput(structArray), builder);

        using IArrowArray output = builder.Build(MemoryAllocator.Default.Value);

        Func<T, T2> linqRunner = (Func<T, T2>)linqCompiled;
        List<T2> linqResult = input.Select(linqRunner).ToList();

        IEnumerable<T2> arrowResult = ArrowReader.ReadRecordBatch<T2>(output);
        arrowResult.Should().BeEquivalentTo(linqResult);
    }

    private static Delegate CompileLinq(LambdaExpression expr) => expr.Compile();

    private static Delegate CompileArrow(LambdaExpression expr)
    {
        BufferTransformVisitor visitor = new();
        Expression result = visitor.Visit(expr);
        return ((LambdaExpression)result).Compile();
    }
}