using Iceberg.Net.Catalog;
using Iceberg.Net.S3;
using Iceberg.Net.Storage;

ObjectStorageRegistry.Register<S3ObjectStorage>();
S3Config storageConfig = new()
{
    Endpoint = "http://127.0.0.1:8333",
    AccessKeyId = "admin",
    SecretAccessKey = "key",
    ForcePathStyle = true
};
UserConfig userConfig = new() { BaseUrl = "http://localhost:8181/v1" };
foreach (KeyValuePair<string, string> property in storageConfig.ToProperties())
    userConfig.CatalogConfig[property.Key] = property.Value;

ICatalog catalog = await RestCatalog.Create(userConfig);

await catalog.CreateNamespaceIfNotExistsAsync(["test"]);
Identifier ident = ["test", "test"];

Table table = await catalog.LoadTableAsync(ident);
Console.WriteLine(table.Metadata!.ToString());
