using System.Collections.Concurrent;
using Iceberg.Net.Rest;

namespace Iceberg.Net.Storage;

public static class ObjectStorageRegistry
{
    private static readonly ConcurrentDictionary<string, Registration> Registrations =
        new(StringComparer.OrdinalIgnoreCase);

    public static void Register<TStorage>() where TStorage : IObjectStorage
    {
        foreach (string scheme in TStorage.Schemes)
        {
            if (string.IsNullOrWhiteSpace(scheme))
                throw new InvalidOperationException($"{typeof(TStorage).FullName} declared an empty URI scheme");

            Registration registration = new(typeof(TStorage), TStorage.Create);
            Registrations.AddOrUpdate(
                scheme,
                registration,
                (_, existing) => existing.StorageType == typeof(TStorage)
                    ? existing
                    : throw new InvalidOperationException(
                        $"URI scheme '{scheme}' is already registered by {existing.StorageType.FullName}"));
        }
    }

    public static IObjectStorage Resolve(
        Uri uri,
        PropertyResolver resolver,
        IReadOnlyList<StorageCredential>? credentials = null)
    {
        if (!uri.IsAbsoluteUri)
            throw new ArgumentException("Object storage paths must be absolute URIs", nameof(uri));

        if (!Registrations.TryGetValue(uri.Scheme, out Registration? registration))
            throw new NotSupportedException(
                $"No object storage provider is registered for URI scheme '{uri.Scheme}'");

        StorageCredential? credential = credentials?
            .Where(candidate => uri.AbsoluteUri.StartsWith(candidate.Prefix, StringComparison.Ordinal))
            .MaxBy(candidate => candidate.Prefix.Length);

        if (credential is null) return registration.Create(resolver);

        PropertyResolver fallback = resolver;
        resolver = key => credential.Config.TryGetValue(key, out string? value) ? value : fallback(key);

        return registration.Create(resolver);
    }

    private sealed record Registration(
        Type StorageType,
        Func<PropertyResolver, IObjectStorage> Create);
}