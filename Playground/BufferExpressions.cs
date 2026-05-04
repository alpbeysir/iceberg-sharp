// using System.Linq.Expressions;
// using Apache.Arrow;
// using Apache.Arrow.Serialization;
// using FastExpressionCompiler;
// using Iceberg.Net.Misc;
// using Iceberg.Net.Query.Arrow;
// using Iceberg.Net.Query.Expressions;
// using Iceberg.Net.Schemas;
// using Varena;
// using ZLinq;
// using ArrowExtensions = Iceberg.Net.Query.Expressions.ArrowExtensions;
// using ExecutionContext = Iceberg.Net.Query.Expressions.ExecutionContext;
// using StructType = Apache.Arrow.Types.StructType;
//
// namespace Playground;
//
// [ArrowSerializable]
// public partial record MyStruct
// {
//     public int? A { get; set; } = 3;
//     public double B { get; set; } = 5;
//     public List<int> L { get; set; } = [3, 3];
//     public List<List<int>> LNest { get; set; } = [[3, 3], [4, 4]];
//     public Nested N { get; set; } = new() { C = 9 };
// }
//
// [ArrowSerializable]
// public partial record Nested
// {
//     public int C { get; set; }
// }
//
// public class BufferExpressions
// {
//     public static void Main()
//     {
//         var method = typeof(BufferExpressions).GetMethod(nameof(Execute))!;
//         LambdaExpression test = (int a, int b) => a + b;
//         //Do(test);   
//
//         var list = Enumerable.Range(0, 26921).Select(_ => CreateRandom()).ToList();
//
//         LambdaExpression test2 = (MyStruct str) => new { b = str.B, a = str.A + str.B, Z = str.N.C };
//         method.MakeGenericMethod(typeof(MyStruct), test2.ReturnType).Invoke(null, [test2, list]);
//
//         LambdaExpression test4 = (MyStruct str) => str.L.Select(n => n + 3);
//         method.MakeGenericMethod(typeof(MyStruct), test4.ReturnType).Invoke(null, [test4, list]);
//         
//         LambdaExpression test3 = (MyStruct str) =>
//             new
//             {
//                 Res = str.A > 300 &&
//                       str.B < 500.0 &&
//                       str.N.C > 12 &&
//                       // str.LNest.Concat(new[] { new List<int> { 1, 2 } }).Select(l => l.Concat(new[] { str.A.Value, 2 }))
//                       //     .All(l => l.Contains(5)) &&
//                       str.L.Select(n => n + 3).All(n => n > 5) &&
//                       str.L.Contains(65) &&
//                       str.L.Any(n => n == 7)
//             };
//         method.MakeGenericMethod(typeof(MyStruct), test3.ReturnType).Invoke(null, [test3, list]);
//     }
//
//     private static MyStruct CreateRandom()
//     {
//         return new MyStruct
//         {
//             A = Random.Shared.Next() % 1000,
//             B = Random.Shared.NextDouble() * 1000,
//             L = Enumerable.Range(0, Random.Shared.Next() % 10).Select(_ => Random.Shared.Next() % 1000).ToList(),
//             N = new Nested { C = Random.Shared.Next() % 10000 },
//             LNest = Enumerable.Range(0, Random.Shared.Next() % 10)
//                 .Select(_ => Enumerable.Range(0, Random.Shared.Next() % 10).ToList()).ToList()
//         };
//     }
//
//     public static void Execute<T, T2>(LambdaExpression expr, List<T> input)
//     {
//         var manager = new VirtualArenaManager();
//         var ctx = new ExecutionContext { Arena = manager.CreateBuffer("default", 100000000) };
//         var inputBatch = ArrowFfiBridge.BuildRecordBatch(input).AsStructArray();
//         BufferTransformVisitor visitor = new();
//         var result = visitor.Visit(expr);
//         Console.WriteLine(ToCSharpPrinter.ToCSharpString(result));
//
//         var compiled = ((LambdaExpression)result).Compile();
//
//         StructArray outputBatch;
//         using (new MeasureTime("arrow"))
//         {
//             var output = compiled.DynamicInvoke(ctx, inputBatch);
//             outputBatch = output switch
//             {
//                 StructArray sa => sa,
//                 IArrowArray arr => ArrowExtensions.MakeStructArray(
//                     new StructType(
//                         [new Field("result", arr.Data.DataType, true)]),
//                     [arr]),
//                 _ => throw new InvalidOperationException($"Unexpected result: {output?.GetType()}")
//             };
//         }
//
//         var schema = ArrowSchema.FromSchema(CSharpSchema.ToIcebergSchema(typeof(T2), -1, _ => -1));
//         var arrowResult = ArrowReader.ReadRecordBatch<T2>(outputBatch.AsRecordBatch(schema));
//         
//         var linqRunner = (Func<T, T2>)expr.Compile();
//         List<T2> linqResult;
//         using (new MeasureTime("linq"))
//         {
//             linqResult = input.AsValueEnumerable().Select(linqRunner).ToList();
//         }
//
//         Console.WriteLine(arrowResult.SequenceEqual(linqResult));
//     }
//
//     public static void Show<T>(StructArray arr)
//     {
//         var schema = ArrowSchema.FromSchema(CSharpSchema.ToIcebergSchema(typeof(T), -1, s => -1));
//         var list = ArrowReader.ReadRecordBatch<T>(arr.AsRecordBatch(schema));
//         foreach (var l in list) Console.WriteLine(l);
//     }
// }

