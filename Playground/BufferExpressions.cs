using System.Linq.Expressions;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using FastExpressionCompiler;
using Iceberg.Net.Query.Arrow;
using Iceberg.Net.Query.Expressions;
using Iceberg.Net.Schemas;
using ExecutionContext = Iceberg.Net.Query.Expressions.ExecutionContext;

namespace Playground;

[ArrowSerializable]
public partial record MyStruct
{
    public int A { get; set; } = 3;
    public double B { get; set; } = 5;
    public List<int> L { get; set; } = [3, 3];
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

        var batch = RecordBatchBuilder.FromObjects([new MyStruct { A = 99 }, new MyStruct { A = 100 }]);

        LambdaExpression test2 = (MyStruct str) => new { b = str.B, a = str.A + str.A, Z = str.N.C };
        Do(test2, batch);

        LambdaExpression test3 = (MyStruct str) =>
            str.A > 5 && str.B < 3 && str.N.C > 12 && str.L.Any(n => n == 7);
        // Do(test3, batch);
    }

    private static void Do(LambdaExpression expr, RecordBatch batch)
    {
        BufferTransformVisitor visitor = new();
        var result = visitor.Visit(expr);
        Console.WriteLine(ToCSharpPrinter.ToCSharpString(result));
        var func = (Func<ExecutionContext, StructArray, StructArray>)((LambdaExpression)result).Compile();

        var ctx = new ExecutionContext();
        var arr = func(ctx, batch.AsStructArray());
        typeof(BufferExpressions).GetMethod(nameof(Show))!.MakeGenericMethod(expr.ReturnType).Invoke(null, [arr]);
    }

    public static void Show<T>(StructArray arr)
    {
        var schema = ArrowSchema.FromSchema(CSharpSchema.ToIcebergSchema(typeof(T), -1, s => -1));
        var list = ArrowReader.ReadRecordBatch<T>(arr.AsRecordBatch(schema));
        foreach (var l in list) Console.WriteLine(l);
    }
}