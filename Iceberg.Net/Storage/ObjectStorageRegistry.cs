using System.Collections.Concurrent;
using Iceberg.Net.Rest;

namespace Iceberg.Net.Storage;

public static class ObjectStorageRegistry
{
    private static readonly ConcurrentDictionary<string, Registration> Registrations =
        new(StringComparer.OrdinalIgnoreCase);

    public static void Register<TStorage>() where TStorage : IObjectStorage
    {
        foreach (var scheme in TStorage.Schemes)
        {
            if (string.IsNullOrWhiteSpace(scheme))
                throw new InvalidOperationException($"{typeof(TStorage).FullName} declared an empty URI scheme");

            var registration = new Registration(typeof(TStorage), TStorage.Create);
            Registrations.AddOrUpdate(
                scheme,
                registration,
                (_, existing) => existing.StorageType == typeof(TStorage)
                    ? existing
                    : throw new InvalidOperationException(
                        $"URI scheme '{scheme}' is already registered by {existing.StorageType.FullName}"));
        }
    }

    public static IObjectStorage CreateRouter(
        IReadOnlyDictionary<string, string>? properties = null,
        IReadOnlyList<StorageCredential>? credentials = null)
    {
        return new RoutingObjectStorage(
            properties ?? new Dictionary<string, string>(),
            credentials ?? []);
    }

    private sealed record Registration(
        Type StorageType,
        Func<IReadOnlyDictionary<string, string>, IObjectStorage> Create);

    private sealed class RoutingObjectStorage(
        IReadOnlyDictionary<string, string> properties,
        IReadOnlyList<StorageCredential> credentials) : IObjectStorage
    {
        private readonly ConcurrentDictionary<string, Lazy<IObjectStorage>> _storage =
            new(StringComparer.Ordinal);

        public static IReadOnlySet<string> Schemes { get; } = new HashSet<string>();

        public static IObjectStorage Create(IReadOnlyDictionary<string, string> properties)
        {
            throw new NotSupportedException("The registry router cannot be registered as a storage provider");
        }

        public ValueTask<Stream> Open(
            Uri uri,
            FileMode fileMode = FileMode.Open,
            CancellationToken cancellationToken = default)
        {
            if (!uri.IsAbsoluteUri)
                throw new ArgumentException("Object storage paths must be absolute URIs", nameof(uri));

            if (!Registrations.TryGetValue(uri.Scheme, out Registration? registration))
                throw new NotSupportedException(
                    $"No object storage provider is registered for URI scheme '{uri.Scheme}'");

            StorageCredential? credential = credentials
                .Where(candidate => uri.AbsoluteUri.StartsWith(candidate.Prefix, StringComparison.Ordinal))
                .MaxBy(candidate => candidate.Prefix.Length);

            string cacheKey = $"{uri.Scheme}\0{credential?.Prefix}";
            IObjectStorage storage = _storage.GetOrAdd(
                    cacheKey,
                    _ => new Lazy<IObjectStorage>(
                        () => registration.Create(MergeProperties(properties, credential)),
                        LazyThreadSafetyMode.ExecutionAndPublication))
                .Value;

            return storage.Open(uri, fileMode, cancellationToken);
        }

        private static IReadOnlyDictionary<string, string> MergeProperties(
            IReadOnlyDictionary<string, string> defaults,
            StorageCredential? credential)
        {
            var merged = new Dictionary<string, string>(defaults, StringComparer.Ordinal);
            if (credential is null) return merged;
            foreach (KeyValuePair<string, string> property in credential.Config)
                merged[property.Key] = property.Value;
            return merged;
        }
    }
}