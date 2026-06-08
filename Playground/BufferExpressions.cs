using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using Apache.Arrow.Types;
using FastExpressionCompiler;
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

        var size = (int)Math.Pow(2, 16);
        Console.WriteLine($"size: {size}");
        List<MyStruct> list = Enumerable.Range(0, size).Select(_ => CreateRandom()).ToList();

        LambdaExpression test2 = (MyStruct str) => new { b = str.B, a = str.A + str.B, Z = str.N.C };
        Run(test2, list);

        LambdaExpression test4 = (MyStruct str) => str.L.Select(n => n + 3);
        Run(test4, list);

        LambdaExpression test5 = (MyStruct str) => str.LNest.Any(n => n.Any(n2 => n2 > 0));
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

    private static void Run(LambdaExpression expr, List<MyStruct> list)
    {
        Console.WriteLine($"{expr}");

        Delegate arrow = CompileArrow(expr);
        Delegate linq = CompileLinq(expr);
        using StructArray inputBatch = ArrowFfiBridge.BuildRecordBatch(list).AsStructArray();
        MethodInfo method = typeof(BufferExpressions).GetMethod(nameof(Execute))!
            .MakeGenericMethod(typeof(MyStruct), expr.ReturnType);
        for (var i = 0; i < 10; i++) method.Invoke(null, [linq, arrow, list, inputBatch]);
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
        using VirtualArenaManager manager = new();
        using VirtualBuffer buffer = manager.CreateBuffer("default", 4_000_000_000);
        UnsafeArenaMemoryAllocator allocator = new(buffer);
        ExecutionContext ctx = new() { Arena = buffer, ArrowAllocator = allocator };

        IArrowType outputType = ArrowSchema.FromIcebergType(CSharpSchema.ToIcebergType(typeof(T2), s => -1, ""));

        IArrowArrayBuilder<IArrowArray> builder = ArrowCompute.MakeBuilderFor(outputType, allocator);
        using (new MeasureTime("arrow"))
        {
            arrowCompiled.DynamicInvoke(ctx, new IdentityInput<StructArray>(structArray), builder);
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
        Console.WriteLine(linqResult.Count == arrowResult.Count());
    }

    private static Delegate CompileLinq(LambdaExpression expr)
    {
        return expr.Compile();
    }

    private static Delegate CompileArrow(LambdaExpression expr)
    {
        BufferTransformVisitor visitor = new();
        Expression? result = visitor.Visit(expr);
        Delegate? compiled = ((LambdaExpression)result).CompileFast();
        return compiled;
    }

    public static void Show<T>(StructArray arr)
    {
        Schema schema = ArrowSchema.FromSchema(CSharpSchema.ToIcebergSchema(typeof(T), -1, s => -1));
        IEnumerable<T> list = ArrowReader.ReadRecordBatch<T>(arr.AsRecordBatch(schema));
        foreach (T l in list) Console.WriteLine(l);
    }
}

