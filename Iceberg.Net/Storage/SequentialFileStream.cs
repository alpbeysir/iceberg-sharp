using EngineeredWood.IO;

namespace Iceberg.Net.Storage;

internal sealed class SequentialFileStream(ISequentialFile file, bool ownsFile = false) : Stream
{
    private bool _disposed;

    public override bool CanRead => false;
    public override bool CanSeek => !_disposed;
    public override bool CanWrite => !_disposed;
    public override long Length => Position;

    public override long Position
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return file.Position;
        }
        set
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (value != file.Position) throw new NotSupportedException("Sequential files cannot seek.");
        }
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        Write(buffer.AsSpan(offset, count));
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        file.WriteAsync(buffer.ToArray()).AsTask().GetAwaiter().GetResult();
    }

    public override ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return file.WriteAsync(buffer, cancellationToken);
    }

    public override async Task WriteAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        await file.WriteAsync(buffer.AsMemory(offset, count), cancellationToken);
    }

    public override void Flush()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        file.FlushAsync().AsTask().GetAwaiter().GetResult();
    }

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return file.FlushAsync(cancellationToken).AsTask();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (offset == 0 && origin is SeekOrigin.Current or SeekOrigin.End) return file.Position;
        if (origin == SeekOrigin.Begin && offset == file.Position) return file.Position;
        throw new NotSupportedException("Sequential files cannot seek.");
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

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
}