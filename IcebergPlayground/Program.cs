using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using Iceberg.Net.Azure;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.EngineeredWoodParquet;
using Iceberg.Net.GCS;
using Iceberg.Net.Parquet;
using Iceberg.Net.S3;
using Iceberg.Net.Storage;
using Microsoft.Extensions.Logging;

internal static class Program
{
    private static async Task Main()
    {
        TableFileSystemRegistry.Register<S3TableFileSystemFactory>();
        TableFileSystemRegistry.Register<AzureTableFileSystemFactory>();
        TableFileSystemRegistry.Register<GcsTableFileSystemFactory>();
        DataFileFormatRegistry.Register<ParquetDataFileFormat>();

        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder
                .SetMinimumLevel(LogLevel.Debug)
                .AddFilter(
                    "Iceberg.Net.Parquet.ParquetDataFileFormat",
                    LogLevel.Trace)
                .AddSimpleConsole(options =>
                {
                    options.IncludeScopes = true;
                    options.SingleLine = true;
                    options.TimestampFormat = "HH:mm:ss ";
                }));

        (ICatalog catalog, Identifier root) = await CreateFoundryCatalog(loggerFactory);

        long count = await OneFile(root, catalog);

        Console.WriteLine(count);
    }

    private static async Task<long> OneFile(Identifier root, ICatalog catalog)
    {
        Identifier table = [.. root, "one_file"];
        TableOperations ops = await catalog.OperationsAsync(table);

        var list = Enumerable.Range(0, 10000)
            .Select(n => new MyStruct { A = n, B = n * 3.0, L = [n, n + 1, n + 2], LNest = [[1, n + 3], [1, 5]] });

        await ops.FastAppendRowsAot(list);

        return await CountRowsAsync(ops);
    }

    private static async Task<long> Small(Identifier root, ICatalog catalog)
    {
        Identifier table = [.. root, "small_files2"];
        TableOperations ops = await catalog.OperationsAsync(table);

        var list = Enumerable.Range(0, 10000)
            .Select(n => new MyStruct { A = n, B = n * 3.0, L = [n, n + 1, n + 2], LNest = [[1, n + 3], [1, 5]] })
            .ToList();

        for (int i = 0; i < 100; i++)
        {
            await ops.FastAppendRowsAot(list);
        }

        return await CountRowsAsync(ops);
    }

    private static async Task<long> CountRowsAsync(
        TableOperations operations,
        CancellationToken cancellationToken = default)
    {
        Channel<RecordBatch> batches = Channel.CreateBounded<RecordBatch>(
            new BoundedChannelOptions(512)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true
            });
        Task read = ReadArrowAndCompleteAsync(
            operations,
            batches.Writer,
            cancellationToken);

        long count = 0;
        try
        {
            await foreach (RecordBatch batch in batches.Reader.ReadAllAsync(cancellationToken))
            {
                using (batch)
                    count += batch.Length;
            }
        }
        finally
        {
            await read;
        }

        return count;
    }

    private static async Task ReadArrowAndCompleteAsync(
        TableOperations operations,
        ChannelWriter<RecordBatch> writer,
        CancellationToken cancellationToken)
    {
        Exception? failure = null;
        try
        {
            await operations.ReadArrowAsync(writer, cancellationToken: cancellationToken);
        }
        catch (Exception exception)
        {
            failure = exception;
            throw;
        }
        finally
        {
            writer.TryComplete(failure);
        }
    }

    private static async Task<(ICatalog catalog, Identifier root)> CreateLocalCatalog(
        ILoggerFactory loggerFactory)
    {
        S3Config storageConfig = new()
        {
            Endpoint = "http://127.0.0.1:8333",
            AccessKeyId = "admin",
            SecretAccessKey = "key",
            ForcePathStyle = true
        };
        UserConfig userConfig = new()
        {
            BaseUrl = "http://localhost:8181/v1",
            LoggerFactory = loggerFactory
        };
        foreach (KeyValuePair<string, string> property in storageConfig.ToProperties())
            userConfig.CatalogConfig[property.Key] = property.Value;

        var catalog = await RestCatalog.Create(userConfig);
        var root = new Identifier(["test"]);
        await catalog.CreateNamespaceIfNotExistsAsync(root);

        return (catalog, root);
    }

    private static async Task<(ICatalog catalog, Identifier root)> CreateFoundryCatalog(
        ILoggerFactory loggerFactory)
    {
        string foundryToken = GetRequiredEnvironmentVariable("FOUNDRY_TOKEN");
        string foundryCatalog = GetRequiredEnvironmentVariable("FOUNDRY_ICEBERG_CATALOG");

        UserConfig userConfig = new()
        {
            BaseUrl = foundryCatalog,
            LoggerFactory = loggerFactory,
            RequestHeaders = { { "Authorization", $"Bearer {foundryToken}" } }
        };

        return (await RestCatalog.Create(userConfig), new Identifier(["Mint", "MMDP", "abeysir", "garbage"]));
    }

    private static string GetRequiredEnvironmentVariable(string name)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        return !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException(
                $"The {name} environment variable must be set to use the Foundry catalog.");
    }
}

[ArrowSerializable]
public partial record MyStruct
{
    public int? A { get; init; } = 3;
    public double B { get; init; } = 5;
    public List<int> L { get; init; } = [3, 3];
    public List<List<int>> LNest { get; init; } = [[3, 3], [4, 4]];
}

[ArrowSerializable]
public partial record SmallFiles
{
    public int NumCol { get; init; } = 1;
    public string Name { get; init; } = "test";
}
