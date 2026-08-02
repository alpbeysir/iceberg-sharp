using System.Buffers;
using EngineeredWood.IO;

namespace Iceberg.Net.Storage;

internal sealed class RandomAccessFileStream(IRandomAccessFile file, bool ownsFile = false) : Stream
{
    private long? _length;
    private long _position;
    private bool _disposed;

    public override bool CanRead => !_disposed;
    public override bool CanSeek => !_disposed;
    public override bool CanWrite => false;
    public override long Length => GetLength();

    public override long Position
    {
        get => _position;
        set
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _position = value;
        }
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return Read(buffer.AsSpan(offset, count));
    }

    public override int Read(Span<byte> buffer)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return ReadCoreAsync(buffer.Length, CancellationToken.None)
            .AsTask()
            .GetAwaiter()
            .GetResult()
            .CopyTo(buffer);
    }

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using ReadResult result = await ReadCoreAsync(buffer.Length, cancellationToken);
        result.Memory.CopyTo(buffer);
        return result.Length;
    }

    public override async Task<int> ReadAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        return await ReadAsync(buffer.AsMemory(offset, count), cancellationToken);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        long position = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => checked(_position + offset),
            SeekOrigin.End => checked(GetLength() + offset),
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };
        Position = position;
        return position;
    }

    public override void Flush()
    {
    }

    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
        {
            _disposed = true;
            if (ownsFile) file.Dispose();
        }

        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        if (ownsFile) await file.DisposeAsync();
    }

    private long GetLength()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _length ??= file.GetLengthAsync().AsTask().GetAwaiter().GetResult();
    }

    private async ValueTask<ReadResult> ReadCoreAsync(
        int requestedLength,
        CancellationToken cancellationToken)
    {
        long length = _length ??= await file.GetLengthAsync(cancellationToken);
        int count = checked((int)Math.Min(requestedLength, Math.Max(0, length - _position)));
        if (count == 0) return default;

        IMemoryOwner<byte> owner = await file.ReadAsync(
            new FileRange(_position, count),
            cancellationToken);
        _position += count;
        return new ReadResult(owner, count);
    }

    private readonly struct ReadResult(IMemoryOwner<byte>? owner, int length) : IDisposable
    {
        public int Length => length;
        public ReadOnlyMemory<byte> Memory => owner?.Memory[..length] ?? ReadOnlyMemory<byte>.Empty;

        public int CopyTo(Span<byte> destination)
        {
            using (owner)
            {
                Memory.Span.CopyTo(destination);
                return length;
            }
        }

        public void Dispose() => owner?.Dispose();
    }
}