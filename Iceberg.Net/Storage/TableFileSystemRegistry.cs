using System.Collections.Concurrent;
using EngineeredWood.IO;
using Iceberg.Net.Rest;

namespace Iceberg.Net.Storage;

public static class TableFileSystemRegistry
{
    private static readonly ConcurrentDictionary<string, Registration> Registrations =
        new(StringComparer.OrdinalIgnoreCase);

    public static void Register<TFactory>() where TFactory : ITableFileSystemFactory
    {
        foreach (string scheme in TFactory.Schemes)
        {
            if (string.IsNullOrWhiteSpace(scheme))
                throw new InvalidOperationException($"{typeof(TFactory).FullName} declared an empty URI scheme");

            Registration registration = new(typeof(TFactory), TFactory.Create);
            Registrations.AddOrUpdate(
                scheme,
                registration,
                (_, existing) => existing.FactoryType == typeof(TFactory)
                    ? existing
                    : throw new InvalidOperationException(
                        $"URI scheme '{scheme}' is already registered by {existing.FactoryType.FullName}"));
        }
    }

    public static ITableFileSystem Resolve(
        Uri uri,
        PropertyResolver resolver,
        IReadOnlyList<StorageCredential>? credentials = null)
    {
        if (!uri.IsAbsoluteUri)
            throw new ArgumentException("Table file paths must be absolute URIs", nameof(uri));

        if (!Registrations.TryGetValue(uri.Scheme, out Registration? registration))
            throw new NotSupportedException(
                $"No table file system provider is registered for URI scheme '{uri.Scheme}'");

        StorageCredential? credential = credentials?
            .Where(candidate => uri.AbsoluteUri.StartsWith(candidate.Prefix, StringComparison.Ordinal))
            .MaxBy(candidate => candidate.Prefix.Length);

        if (credential is null) return registration.Create(uri, resolver);

        PropertyResolver fallback = resolver;
        resolver = key => credential.Config.TryGetValue(key, out string? value) ? value : fallback(key);

        return registration.Create(uri, resolver);
    }

    private sealed record Registration(
        Type FactoryType,
        Func<Uri, PropertyResolver, ITableFileSystem> Create);
}
