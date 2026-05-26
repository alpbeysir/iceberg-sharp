using System.Linq.Expressions;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.Arrow;
using Iceberg.Net.Query.Expressions;
using Iceberg.Net.Query.FastArrow;
using Iceberg.Net.Schemas;
using Varena;
using ExecutionContext = Iceberg.Net.Query.Expressions.ExecutionContext;

namespace Playground;

[ArrowSerializable]
public partial record MyStruct
{
    public int? A { get; set; } = 3;
    public double B { get; set; } = 5;
    public List<int> L { get; set; } = [3, 3];
    public List<List<int>> LNest { get; set; } = [[3, 3], [4, 4]];
    public Nested N { get; set; } = new() { C = 9 };
}

[ArrowSerializable]
public partial record Nested
{
    public int C { get; set; }
}

public class BufferExpressions
{
    public static void Main()
    {
        LambdaExpression test = (int a, int b) => a + b;
        //Do(test);   

        var list = Enumerable.Range(0, 500000).Select(_ => CreateRandom()).ToList();

        // LambdaExpression test2 = (MyStruct str) => new { b = str.B, a = str.A + str.B, Z = str.N.C };
        // method.MakeGenericMethod(typeof(MyStruct), test2.ReturnType).Invoke(null, [test2, list]);
        //
        // LambdaExpression test4 = (MyStruct str) => str.L.Select(n => n + 3);
        // method.MakeGenericMethod(typeof(MyStruct), test4.ReturnType).Invoke(null, [test4, list]);

        LambdaExpression test5 = (MyStruct str) => str.LNest.Select(n => n.All(n2 => n2 == 5));
        Run(test5, list);

        // LambdaExpression test3 = (MyStruct str) =>
        //     new
        //     {
        //         Res = str.A > 300 &&
        //               str.B < 500.0 &&
        //               str.N.C > 12 &&
        //               // str.LNest.Concat(new[] { new List<int> { 1, 2 } }).Select(l => l.Concat(new[] { str.A.Value, 2 }))
        //               //     .All(l => l.Contains(5)) &&
        //               str.L.Select(n => n + 3).All(n => n > 5) &&
        //               str.L.Any(n => n > 7)
        //     };
        // method.MakeGenericMethod(typeof(MyStruct), test3.ReturnType).Invoke(null, [test3, list]);
    }

    private static void Run(LambdaExpression test5, List<MyStruct> list)
    {
        var arrow = CompileArrow(test5);
        var linq = CompileLinq(test5);
        var inputBatch = ArrowFfiBridge.BuildRecordBatch(list).AsStructArray();
        var method = typeof(BufferExpressions).GetMethod(nameof(Execute))!
            .MakeGenericMethod(typeof(MyStruct), test5.ReturnType);
        for (var i = 0; i < 100000; i++) method.Invoke(null, [linq, arrow, list, inputBatch]);
    }

    private static MyStruct CreateRandom()
    {
        return new MyStruct
        {
            A = Random.Shared.Next() % 1000,
            B = Random.Shared.NextDouble() * 1000,
            L = Enumerable.Range(0, Random.Shared.Next() % 10).Select(_ => Random.Shared.Next() % 1000).ToList(),
            N = new Nested { C = Random.Shared.Next() % 10000 },
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
        var manager = new VirtualArenaManager();
        var buffer = manager.CreateBuffer("default", 1000_000_000);
        var allocator = new UnsafeArenaMemoryAllocator(buffer);
        var ctx = new ExecutionContext { Arena = buffer, ArrowAllocator = allocator };

        var outputType = ArrowSchema.FromIcebergType(CSharpSchema.ToIcebergType(typeof(T2), s => -1, ""));

        var builder = ArrowCompute.MakeBuilderFor(outputType, allocator);
        using (new MeasureTime("arrow"))
        {
            arrowCompiled.DynamicInvoke(ctx, structArray, builder);
        }

        IArrowArray output = ((dynamic)builder).Build();
        ctx.Arena.Dispose();
        output.Dispose();

        // var linqRunner = (Func<T, T2>)linqCompiled;
        // List<T2> linqResult;
        // using (new MeasureTime("linq"))
        // {
        //     linqResult = input.AsValueEnumerable().Select(linqRunner).ToList();
        // }

        // var arrowResult = ArrowReader.ReadRecordBatch<T2>(output);
        // Console.WriteLine(linqResult.Count == arrowResult.Count());
    }

    private static Delegate CompileLinq(LambdaExpression expr)
    {
        return expr.Compile();
    }

    private static Delegate CompileArrow(LambdaExpression expr)
    {
        BufferTransformVisitor visitor = new();
        var result = visitor.Visit(expr);

        var compiled = ((LambdaExpression)result).Compile();
        return compiled;
    }

    public static void Show<T>(StructArray arr)
    {
        var schema = ArrowSchema.FromSchema(CSharpSchema.ToIcebergSchema(typeof(T), -1, s => -1));
        var list = ArrowReader.ReadRecordBatch<T>(arr.AsRecordBatch(schema));
        foreach (var l in list) Console.WriteLine(l);
    }
}

