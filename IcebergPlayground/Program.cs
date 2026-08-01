using Iceberg.Net.Catalog;
using Iceberg.Net.Storage;

ICatalog catalog = await RestCatalog.Create(new UserConfig
{
    BaseUrl = "http://localhost:8181/v1",
    StorageConfig = new S3Config
    {
        Endpoint = "http://127.0.0.1:8333",
        AccessKeyId = "admin",
        SecretAccessKey = "key",
        ForcePathStyle = true
    }
});

await catalog.CreateNamespaceIfNotExistsAsync(["test"]);
Identifier ident = ["test", "test"];

var table = await catalog.LoadTableAsync(ident);
Console.WriteLine(table.Metadata!.ToString());