using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Encoding;

namespace Iceberg.Net.Metadata;

internal sealed class ManifestWriter<T> : IDisposable
{
    private readonly OcfWriter _container;
    private readonly AvroBinaryWriter _binaryWriter = new(4096);
    private readonly Action<AvroBinaryWriter, T> _write;
    private bool _disposed;

    internal ManifestWriter(
        Stream stream,
        AvroSchema schema,
        IReadOnlyDictionary<string, byte[]> metadata,
        Action<AvroBinaryWriter, T> write)
    {
        _container = new OcfWriter(stream, AvroCodec.Null);
        _container.WriteHeader(schema, metadata);
        _write = write;
    }

    internal void Append(T value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _binaryWriter.Reset();
        _write(_binaryWriter, value);
        _container.WriteBlock(_binaryWriter.WrittenSpan, 1);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _container.Dispose();
    }
}