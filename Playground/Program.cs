// // See https://aka.ms/new-console-template for more information
//
// using System.Threading.Channels;
// using Apache.Arrow;
// using Apache.Arrow.Serialization;
// using Iceberg.Net.Catalog;
// using Iceberg.Net.DuckUtils;
// using Iceberg.Net.Misc;
// using Iceberg.Net.Storage;
// using ZLinq.Simd;
// using Table = Iceberg.Net.Catalog.Table;
// using Transaction = Iceberg.Net.Catalog.Transaction;
//
// namespace Playground;
//
// [ArrowSerializable]
// public partial record MyRow
// {
//     public int Num { get; init; }
//     public double Num2 { get; init; }
//
//     public string Str { get; init; }
//
//     public List<int> Arr { get; init; }
//
//     public Dictionary<int, int> Map { get; init; }
//     // public decimal? Unused { get; init; }
//
//     public MyNestedRow Nested { get; init; }
// }
//
// [ArrowSerializable]
// public partial record MyNestedRow
// {
//     public long Num2 { get; init; }
//     public double Num3 { get; init; }
// }
//
// public static class Program
// {
//     private static readonly Identifier Playground = ["playground"];
//
//     private static bool MyThing(this string a)
//     {
//         return a.StartsWith("bba");
//     }
//
//     public static async Task Main(string[] argv)
//     {
//         if (argv.Length < 3)
//         {
//             Console.WriteLine("Usage: <RowsPerFile> <FileCount> <ReadCount>");
//             return;
//         }
//
//         var args = new Args(
//             int.Parse(argv[0]),
//             int.Parse(argv[1]),
//             int.Parse(argv[2])
//         );
//
//         var userConfig = new UserConfig
//         {
//             BaseUrl = "http://localhost:8181/v1",
//             StorageConfig = new S3Config
//             {
//                 Endpoint = "http://127.0.0.1:8333",
//                 AccessKeyId = "admin",
//                 SecretAccessKey = "key"
//             }
//         };
//         var catalog = await RestCatalog.Create(userConfig);
//
//         await catalog.CreateNamespaceIfNotExistsAsync(Playground);
//
//         var rand = new Random();
//         Identifier identifier = [..Playground, $"test_write{rand.NextInt64()}"];
//         await Append(catalog, identifier, rand, args);
//
//         var transaction = new Transaction(await catalog.LoadTableAsync(identifier));
//         await DuckDb(identifier);
//
//         // await ChannelRead(transaction);
//         //
//         // Enumerate(transaction);
//
//         using (new MeasureTime("Queryable"))
//         {
//             var queryable = await transaction.ReadQueryable<MyRow>();
//             // var result = queryable.Where(row => row.Arr.Contains(5))
//             //     .Select(row => new { row, myNum = 50 })
//             //     .Select(t => new { t, myNum2 = 50 })
//             //     .Where(t => t.t.row.Num < t.t.myNum)
//             //     .Where(t => t.t.row.Num2 > 0.05)
//             //     .Select(t => new { Zort = t.t.row.Num, Zort2 = t.t.row.Nested.Num3, t.t.myNum });
//             // Console.WriteLine(result.ToList());
//             //
//             // var result2 = queryable.Select(row => new { A = row.Num2, B = row.Num2, C = row.Map });
//             // Console.WriteLine(result2.ToList());
//             //
//             // var result3 = queryable.Join(
//             //     Enumerable.Range(0, 10),
//             //     row => row.Num,
//             //     i => i,
//             //     (row, i) => new { row, i, Z = i - 3 });
//             // Console.WriteLine(result3.ToList());
//
//             var result4 = queryable.GroupBy(x => 1)
//                 .Select(g => new
//                 {
//                     TotalStrLength = g.Select(x => new { x.Arr }), TotalNum = g.Sum(x => x.Num), Count = g.Count()
//                 }).GroupJoin([1, 2, 3], arg => arg.TotalNum, inner => inner, (arg1, ints) => arg1.Count);
//             Console.WriteLine(result4.ToList());
//         }
//
//         await transaction.DisposeAsync();
//         // await catalog.DropNamespaceAsync(Playground, true);
//     }
//
//     private static async Task DuckDb(Identifier identifier)
//     {
//         var duckDb = new DuckDbCatalog();
//         await duckDb.InitializeAsync();
//
//         using (new MeasureTime("DuckDb"))
//         {
//             await using var reader =
//                 await duckDb.ExecuteQuery(
//                     $"SELECT SUM(length(Str)), SUM(Num), COUNT(*) FROM {duckDb.CatalogName}.{identifier};");
//             await reader.ReadAsync();
//             var total = reader.GetInt64(0);
//             var total2 = reader.GetInt64(1);
//             var count = reader.GetInt64(2);
//             Console.WriteLine($"Total={total} Total2={total2} Count={count}");
//         }
//     }
//
//     private static void Enumerate(Transaction transaction)
//     {
//         using (new MeasureTime("Enumerate"))
//         {
//             var readRows = transaction.ReadRows<MyRow>();
//             var result = (from x in readRows
//                 group x by 1
//                 into g
//                 select new
//                 {
//                     TotalStrLength = g.Sum(x => x.Str.Length),
//                     TotalNum = g.Sum(x => x.Num),
//                     Count = g.Count()
//                 }).FirstOrDefault();
//             Console.WriteLine(result);
//         }
//     }
//
//     private static async Task ChannelRead(Transaction transaction)
//     {
//         using (new MeasureTime("ChannelRead"))
//         {
//             var columnBuffers = Channel.CreateBounded<RecordBatch>(
//                 new BoundedChannelOptions(16384)
//                 {
//                     FullMode = BoundedChannelFullMode.Wait
//                 });
//
//             var read = transaction.Read(null, columnBuffers);
//
//             long total = 0;
//             long total2 = 0;
//             long count = 0;
//             var consumers = Parallel.ForEachAsync(
//                 columnBuffers.Reader.ReadAllAsync(),
//                 new ParallelOptions { MaxDegreeOfParallelism = 32 },
//                 (set, token) =>
//                 {
//                     var strBuf = set.Column("Str");
//                     var numBuf = set.Column("Num");
//                     strBuf.Accept(
//                         new InlineArrowVisitor(array =>
//                         {
//                             if (array is StringArray stringArray)
//                             {
//                                 long t = 0;
//                                 for (var j = 0; j < stringArray.Length; j++)
//                                 {
//                                     var str = stringArray.GetString(j);
//                                     t += str.Length;
//                                 }
//
//                                 Interlocked.Add(ref total, t);
//                             }
//                             else
//                             {
//                                 throw new InvalidOperationException();
//                             }
//                         }));
//
//                     numBuf.Accept(
//                         new InlineArrowVisitor(array =>
//                         {
//                             if (array is Int32Array int32Array)
//                             {
//                                 var t = int32Array.Values.AsVectorizable().Sum();
//                                 Interlocked.Add(ref total2, t);
//                             }
//                             else
//                             {
//                                 throw new InvalidOperationException();
//                             }
//                         }));
//
//                     Interlocked.Add(ref count, numBuf.Length);
//
//                     set.Dispose();
//                     return ValueTask.CompletedTask;
//                 });
//
//             await read;
//             columnBuffers.Writer.Complete();
//
//             await consumers;
//
//             Console.WriteLine($"Total={total} Total2={total2} Count={count}");
//         }
//     }
//
//     private static async Task Append(RestCatalog catalog, Identifier identifier, Random rand, Args args)
//     {
//         var transaction = new Transaction(new Table(identifier, catalog));
//
//         var list = new List<int>(Enumerable.Range(0, 10));
//         var map = new Dictionary<int, int>(new Dictionary<int, int> { { 1, 2 }, { 3, 4 } });
//         var randNum = rand.NextDouble() % 1000.0f;
//
//         var rowCount = args.RowsPerFile;
//         var rows = Enumerable.Range(0, rowCount)
//             .Select(i => new MyRow
//             {
//                 Num = rand.Next() % 1000,
//                 Num2 = randNum,
//                 Str = Utils.RandomCreateString(rand.Next() % 25),
//                 Arr = list,
//                 Map = map,
//                 Nested = new MyNestedRow { Num2 = rand.Next() % 200, Num3 = randNum + 4 }
//             }).ToList();
//
//         using (new MeasureTime("Append"))
//         {
//             await transaction.AppendRows(rows);
//             await transaction.Commit();
//         }
//     }
//
//     private class InlineArrowVisitor(Action<IArrowArray> func) : IArrowArrayVisitor
//     {
//         public void Visit(IArrowArray array)
//         {
//             func(array);
//         }
//     }
//
//     private record Args(int RowsPerFile, int FileCount, int ReadCount);
// }
//
// public class PolarsTest
// {
//     public string AAA { get; set; }
//     public int ZZZ { get; set; }
// }

