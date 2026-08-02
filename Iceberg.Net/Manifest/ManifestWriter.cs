using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Encoding;

namespace Iceberg.Net.Metadata;

internal sealed class ManifestWriter<T> : IAsyncDisposable
{
    private const int TargetBlockSizeBytes = 64 * 1024;

    private readonly OcfWriterAsync _container;
    private readonly AvroBinaryWriter _binaryWriter = new(4096);
    private readonly IAvroSerializer<T> _serializer;
    private int _objectCount;
    private bool _disposed;

    private ManifestWriter(
        OcfWriterAsync container,
        IAvroSerializer<T> serializer)
    {
        _container = container;
        _serializer = serializer;
    }

    internal static async ValueTask<ManifestWriter<T>> CreateAsync(
        Stream stream,
        IReadOnlyDictionary<string, byte[]> metadata,
        IAvroSerializer<T> serialization,
        AvroCodec codec,
        int? compressionLevel,
        CancellationToken cancellationToken = default)
    {
        OcfWriterAsync container = new(
            stream,
            codec,
            customLevel: compressionLevel);
        await container.WriteHeaderAsync(
            serialization.Schema,
            metadata,
            cancellationToken);
        return new ManifestWriter<T>(container, serialization);
    }

    internal async ValueTask AppendAsync(
        T value,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _serializer.Write(_binaryWriter, value);
        _objectCount++;

        if (_binaryWriter.Length >= TargetBlockSizeBytes)
            await FlushBlockAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await FlushBlockAsync();
        await _container.DisposeAsync();
    }

    private async ValueTask FlushBlockAsync(CancellationToken cancellationToken = default)
    {
        if (_objectCount == 0) return;

        await _container.WriteBlockAsync(
            _binaryWriter.WrittenMemory,
            _objectCount,
            cancellationToken);
        _binaryWriter.Reset();
        _objectCount = 0;
    }
}
