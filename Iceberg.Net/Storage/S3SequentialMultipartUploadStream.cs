using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Apache.Arrow.Memory;
using Iceberg.Net.Misc;

namespace Iceberg.Net.Storage;

// TODO figure out a way to abort without changing Stream semantics
public sealed class S3SequentialMultipartUploadStream : Stream
{
    // 5 MB
    private const int MinPartSize = 5 * 1024 * 1024;

    // 256 MB
    private const int MaxPartSize = 64 * 1024 * 1024;
    private readonly NativeMemoryAllocator _allocator = new();

    private readonly AmazonS3Client _client;
    private readonly AmazonS3Uri _s3Uri;
    private readonly string _uploadId;

    private readonly List<Task<UploadPartResponse>> _uploadTasks = [];

    private IMemoryOwner<byte> _buffer;

    private int _bufferPosition;

    private int _currentBufferSize = MinPartSize;
    private int _currentPartNumber = 1;
    private bool _open = true;

    private long _position;

    private S3SequentialMultipartUploadStream(AmazonS3Client client, AmazonS3Uri s3Uri, string uploadId)
    {
        _client = client;
        _s3Uri = s3Uri;
        _uploadId = uploadId;
        _buffer = MemoryPool<byte>.Shared.Rent(_currentBufferSize);
    }

    public override bool CanRead => false;
    public override bool CanSeek => true;
    public override bool CanWrite => true;
    public override long Length => Position;

    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    public static async Task<S3SequentialMultipartUploadStream> Create(
        AmazonS3Client client,
        AmazonS3Uri s3Uri,
        CancellationToken cancellationToken = default)
    {
        InitiateMultipartUploadRequest request = new()
        {
            BucketName = s3Uri.Bucket,
            ExpectedBucketOwner = null,
            Key = s3Uri.Key
        };
        InitiateMultipartUploadResponse? response =
            await client.InitiateMultipartUploadAsync(request, cancellationToken);
        return new S3SequentialMultipartUploadStream(client, s3Uri, response.UploadId);
    }

    private void InitiatePartUpload(IMemoryOwner<byte> buf, int count, bool last)
    {
        UnmanagedMemoryStream stream = StreamFromUnmanagedMemory(buf, count);
        UploadPartRequest request = new()
        {
            InputStream = stream,
            BucketName = _s3Uri.Bucket,
            Key = _s3Uri.Key,
            PartNumber = _currentPartNumber++,
            UploadId = _uploadId,
            PartSize = stream.Length,
            IsLastPart = last
            // StreamTransferProgress = null
        };
        Task<UploadPartResponse>? task = _client.UploadPartAsync(request);
        _uploadTasks.Add(task);
        _ = task.ContinueWith(_ =>
        {
            Console.WriteLine($"MultipartUpload {_s3Uri.Key} {Utils.ToFileSize(count)} {(last ? "LastPart" : "")}");
            stream.Dispose();
            buf.Dispose();
        });
    }

    private static unsafe UnmanagedMemoryStream StreamFromUnmanagedMemory(IMemoryOwner<byte> buf, int count)
    {
        Span<byte> span = buf.Memory.Span;
        UnmanagedMemoryStream stream = new(
            (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(span)),
            count);
        return stream;
    }

    protected override void Dispose(bool disposing)
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_open) throw new InvalidOperationException("stream is disposed");
        _open = false;

        if (_uploadTasks.Count > 0)
        {
            // last part
            InitiatePartUpload(_buffer, _bufferPosition, true);
            await FlushAsync();

            CompleteMultipartUploadRequest request = new()
            {
                BucketName = _s3Uri.Bucket,
                ChecksumType = null,
                MpuObjectSize = _position,
                Key = _s3Uri.Key,
                PartETags = _uploadTasks.Select(task => new PartETag(task.Result)).ToList(),
                UploadId = _uploadId
            };
            Console.WriteLine($"CompleteMultipartUpload {_s3Uri.Key} {Utils.ToFileSize(_position)}");
            // TODO we don't really need to wait for this
            await _client.CompleteMultipartUploadAsync(request);
        }
        else
        {
            // minimum size never reached, do single part upload
            UnmanagedMemoryStream stream = StreamFromUnmanagedMemory(_buffer, _bufferPosition);
            PutObjectRequest request = new()
            {
                BucketName = _s3Uri.Bucket,
                Key = _s3Uri.Key,
                InputStream = stream,
                AutoCloseStream = false,
                AutoResetStreamPosition = false
                // StreamTransferProgress = null,
            };
            await _client.PutObjectAsync(request);
            Console.WriteLine($"PutObject {_s3Uri.Key} {Utils.ToFileSize(stream.Length)}");
            _buffer.Dispose();
        }
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        await Task.WhenAll(_uploadTasks);
    }

    public override void Flush()
    {
        Task.WaitAll(_uploadTasks.ToArray());
    }

    public override void Write(ReadOnlySpan<byte> span)
    {
        while (!span.IsEmpty)
        {
            if (_bufferPosition == _buffer.Memory.Length)
            {
                // buffer filled, pass 'ownership' to upload part
                InitiatePartUpload(_buffer, _bufferPosition, false);

                // TODO try GC.UninitializedArray() etc.
                _buffer = _allocator.Allocate(_currentBufferSize);
                _currentBufferSize = Math.Min(_currentBufferSize * 2, MaxPartSize);

                _currentPartNumber++;
                _bufferPosition = 0;
            }

            var copySize = Math.Min(span.Length, _buffer.Memory.Length - _bufferPosition);
            Span<byte> bufferSlice = _buffer.Memory.Span.Slice(_bufferPosition, copySize);
            ReadOnlySpan<byte> sourceSlice = span[..copySize];
            sourceSlice.CopyTo(bufferSlice);
            span = span[copySize..];
            _bufferPosition += copySize;
            _position += copySize;
        }
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        Span<byte> slice = buffer.AsSpan(offset, count);
        Write(slice);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotImplementedException();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotImplementedException();
    }

    public override void SetLength(long value)
    {
        throw new NotImplementedException();
    }
}