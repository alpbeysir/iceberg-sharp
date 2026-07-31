using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Serialization;
using Apache.Arrow.Types;
using DotNext.Linq.Expressions;
using DotNext.Metaprogramming;
using FastExpressionCompiler;
using FastExpressionCompiler.ImTools;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.Arrow;
using Iceberg.Net.Query.Expressions;
using Iceberg.Net.Query.FastArrow;
using Iceberg.Net.Schemas;
using Varena;
using ExecutionContext = Iceberg.Net.Query.Expressions.ExecutionContext;
using Schema = Apache.Arrow.Schema;

namespace Playground;

[ArrowSerializable]
public partial record MyStruct
{
    public int? A { get; init; } = 3;
    public double B { get; init; } = 5;
    public List<int> L { get; init; } = [3, 3];
    public List<List<int>> LNest { get; init; } = [[3, 3], [4, 4]];
    public Nested N { get; init; } = new() { C = 9 };
}

[ArrowSerializable]
public partial record Nested
{
    public int C { get; init; }
}


public class BufferExpressions
{
    public static void Main()
    {
        var a = new UsableExpression<int>(Expression.Parameter(typeof(int), "a"));
        var b = new UsableExpression<int>(Expression.Parameter(typeof(int), "b"));
        Expression test = ExpressionUtilities.Use(() => a.Value + b.Value);
        // ExpressionUtilities.EnableAsmPrint();
        var size = (int)Math.Pow(2, 18);
        Console.WriteLine($"size: {size}");
        List<MyStruct> list = Enumerable.Range(0, size).Select(_ => CreateRandom()).ToList();

        // LambdaExpression test2 = (MyStruct str) => new { b = str.B, a = str.A + str.B, Z = str.N.C };
        // Run(test2, list);
        //
        // LambdaExpression test4 = (MyStruct str) => str.L.Select(n => n + 3);
        // Run(test4, list);
        //
        // LambdaExpression test5 = (MyStruct str) => str.LNest.All(n => n.All(n2 => n2 > 0));
        // Run(test5, list);

        // LambdaExpression test6 = (MyStruct str) => str.L.Contains(65);
        // Run(test6, list);
        //
        // LambdaExpression test7 = (MyStruct str) => str.L.Contains(str.N.C);
        // Run(test7, list);

        // LambdaExpression test8 = (MyStruct str) => str.LNest.All(n => n.All(n2 => n2 <= str.N.C));
        // Run(test8, list);

        // LambdaExpression test9 = (MyStruct str) => new
        // {
        //     Output1 = str.LNest.Any(n => n.Any(n2 => n2 > 0)),
        //     Output2 = str.L.Select(n => n + str.B + 3),
        //     Output3 = str.L,
        //     Output4 = new { ZZZ = str.B + str.A + str.A }
        // };
        // Run(test9, list);

        // LambdaExpression test999 = (MyStruct str) => str;
        // Run(test999, list);
        //
        // LambdaExpression test1000 = (int num) => num;
        // Run(test1000, list);

        // LambdaExpression test11 = (MyStruct str) => str.L.Select(n => str.A + 3);
        // Run(test11, list);

        LambdaExpression test10 = (MyStruct str) => str.LNest.Select(n => n.Where(n2 => n2 > str.N.C));
        Run(test10, list);
    }

    private static void Run(LambdaExpression expr, List<MyStruct> list)
    {
        Console.WriteLine($"{expr}");

        Delegate arrow = CompileArrow(expr);
        Delegate linq = CompileLinq(expr);
        using StructArray inputBatch = ArrowFfiBridge.BuildRecordBatch(list).AsStructArray();
        MethodInfo method = typeof(BufferExpressions).GetMethod(nameof(Execute))!
            .MakeGenericMethod(typeof(MyStruct), expr.ReturnType);
        for (var i = 0; i < 100; i++)
        {
            Console.WriteLine($"Run {i}");
            method.Invoke(null, [linq, arrow, list, inputBatch]);
        }
    }

    private static MyStruct CreateRandom()
    {
        return new MyStruct
        {
            A = Random.Shared.Next() % 1000,
            B = Random.Shared.NextDouble() * 1000,
            L = Enumerable.Range(0, Random.Shared.Next() % 10).Select(_ => Random.Shared.Next() % 1000).ToList(),
            N = new Nested { C = Random.Shared.Next() % 10000 + 1000 },
            LNest = Enumerable.Range(0, Random.Shared.Next() % 10)
                .Select(_ => Enumerable.Range(0, Random.Shared.Next() % 10).ToList()).ToList()
        };
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
        using (new MeasureHeap("arrow"))
        using (new MeasureTime("arrow"))
        {
            arrowCompiled.DynamicInvoke(ctx, new IdentityInput(structArray), builder);
        }

        using IArrowArray output = builder.Build(MemoryAllocator.Default.Value);

        Console.WriteLine($"--- arrow arena: {Utils.ToFileSize(buffer.CommittedBytes)}");

        // Func<T, T2> linqRunner = (Func<T, T2>)linqCompiled;
        // List<T2> linqResult;
        // using (new MeasureHeap("linq"))
        // using (new MeasureTime("linq"))
        // {
        //     linqResult = input.Select(linqRunner).ToList();
        // }
        //
        // IEnumerable<T2> arrowResult = ArrowReader.ReadRecordBatch<T2>(output);
        // Debug.Assert(linqResult.Count == arrowResult.Count());
    }

    private static Delegate CompileLinq(LambdaExpression expr)
    {
        return expr.CompileFast();
    }

    private static Delegate CompileArrow(LambdaExpression expr)
    {
        BufferTransformVisitor visitor = new();
        ExpressionOptimizer optimizer = new(new SemanticProvider(), new EvaluatorFactory());
        Expression visited = visitor.Visit(expr);
        Expression result = optimizer.Visit(visited)!;
        Delegate compiled = ((LambdaExpression)result).Compile();
        return compiled;
    }

    public static void Show<T>(StructArray arr)
    {
        Schema schema = ArrowSchema.FromSchema(CSharpSchema.ToIcebergSchema(typeof(T), -1, s => -1));
        IEnumerable<T> list = ArrowReader.ReadRecordBatch<T>(arr.AsRecordBatch(schema));
        foreach (T l in list) Console.WriteLine(l);
    }
}