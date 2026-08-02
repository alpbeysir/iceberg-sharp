using Iceberg.Net.Storage;

namespace Iceberg.Net.Azure;

public sealed record ADLSConfig
{
    public string? ConnectionString { get; private init; }

    public string? SasToken { get; private init; }

    public string? SharedKeyAccountName { get; private init; }

    public string? SharedKeyAccountKey { get; private init; }

    public static ADLSConfig FromResolver(string account, PropertyResolver resolve)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(account);

        return new ADLSConfig
        {
            ConnectionString = ResolveAccountProperty(resolve, "adls.connection-string", account),
            SasToken = ResolveAccountProperty(resolve, "adls.sas-token", account),
            SharedKeyAccountName = resolve("adls.auth.shared-key.account.name"),
            SharedKeyAccountKey = resolve("adls.auth.shared-key.account.key")
        };
    }

    private static string? ResolveAccountProperty(PropertyResolver resolve, string key, string account) =>
        resolve($"{key}.{account}") ?? resolve(key);
}