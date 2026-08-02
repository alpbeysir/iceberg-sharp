using System.Buffers;
using System.Threading.Channels;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Apache.Arrow.Memory;

namespace Iceberg.Net.S3;

// MIT License
//
// Copyright (c) 2021 Lee Harding (mlhpdx)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
//     of this software and associated documentation files (the "Software"), to deal
//     in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
//     furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
//     copies or substantial portions of the Software.
//
//     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//     IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//     FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//     AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//     LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
public sealed class S3SeekableReadStream : Stream
{
    private readonly NativeMemoryAllocator _allocator = new();
    private readonly List<StreamSegment> _cache = [];
    private readonly AmazonS3Client _client;
    private readonly CancellationTokenSource _cts = new();
    private readonly int _maxSegments;
    private readonly Channel<long> _prefetchChannel = Channel.CreateUnbounded<long>();
    private readonly int _prefetchCount;
    private readonly AmazonS3Uri _s3Uri;
    private readonly long _segmentSize;

    private int downloaded;

    private int evict;

    private int miss;

    private S3SeekableReadStream(
        AmazonS3Client client,
        AmazonS3Uri s3Uri,
        long length,
        int maxSegments,
        int prefetchCount,
        long segmentSize,
        int concurrentWorkers)
    {
        _client = client;
        _s3Uri = s3Uri;
        Length = length;
        _maxSegments = maxSegments;
        _prefetchCount = prefetchCount;
        _segmentSize = segmentSize;

        // Start background consumers
        for (int i = 0; i < concurrentWorkers; i++)
            _ = Task.Run(ProcessPrefetchQueueAsync);
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length { get; }
    public override long Position { get; set; }

    public static async Task<S3SeekableReadStream> Create(
        AmazonS3Client client,
        AmazonS3Uri s3Uri,
        CancellationToken cancellationToken = default)
    {
        GetObjectMetadataResponse? response = await client.GetObjectMetadataAsync(
            s3Uri.Bucket,
            s3Uri.Key,
            cancellationToken);

        const int chunkSize = 16 * 1024 * 1024;
        const int maxSegments = 8;
        const int prefetchCount = 4;
        const int workers = 2;
        long fileSize = response.ContentLength;

        return new S3SeekableReadStream(client, s3Uri, fileSize, maxSegments, prefetchCount, chunkSize, workers);
    }

    public override int Read(Span<byte> buffer)
    {
        if (Position >= Length) return 0;

        int totalRead = 0;
        int bytesToRead = (int)Math.Min(buffer.Length, Length - Position);

        while (totalRead < bytesToRead)
        {
            StreamSegment? segment = GetSegment(Position);

            if (segment == null)
            {
                miss++;

                // Signal prefetch for subsequent segments
                for (int i = 1; i <= _prefetchCount; i++)
                {
                    long nextPos = Position + i * _segmentSize;
                    if (nextPos < Length) _prefetchChannel.Writer.TryWrite(nextPos);
                }

                // Blocking read for the missing chunk
                segment = DownloadSegmentAsync(Position).GetAwaiter().GetResult();
            }

            segment.LastAccess = DateTime.UtcNow.Ticks;

            int offsetInSegment = (int)(Position - segment.Start);
            int availableInSegment = (int)(segment.End - Position);
            int toCopy = Math.Min(availableInSegment, bytesToRead - totalRead);

            segment.Data.Memory.Span.Slice(offsetInSegment, toCopy)
                .CopyTo(buffer.Slice(totalRead, toCopy));

            Position += toCopy;
            totalRead += toCopy;
        }

        return totalRead;
    }

    private StreamSegment? GetSegment(long pos)
    {
        lock (_cache)
        {
            return _cache.FirstOrDefault(s => s.Contains(pos));
        }
    }

    private async Task<StreamSegment> DownloadSegmentAsync(long pos)
    {
        // Check if another thread/prefetcher filled it while we waited
        StreamSegment? existing = GetSegment(pos);
        if (existing != null) return existing;

        long start = pos / _segmentSize * _segmentSize;
        long end = Math.Min(start + _segmentSize, Length);

        GetObjectRequest request = new()
        {
            BucketName = _s3Uri.Bucket,
            Key = _s3Uri.Key,
            ByteRange = new ByteRange(start, end - 1)
        };

        using GetObjectResponse? response = await _client.GetObjectAsync(request);
        IMemoryOwner<byte>? owner = _allocator.Allocate((int)response.ContentLength);

        downloaded += (int)response.ContentLength;

        await response.ResponseStream.ReadExactlyAsync(owner.Memory[..(int)response.ContentLength]);

        StreamSegment newSegment = new(start, end, owner);

        lock (_cache)
        {
            if (_cache.Count >= _maxSegments)
            {
                evict++;
                StreamSegment lru = _cache.OrderBy(s => s.LastAccess).First();
                _cache.Remove(lru);
                lru.Dispose();
            }

            _cache.Add(newSegment);
        }

        return newSegment;
    }

    private async Task ProcessPrefetchQueueAsync()
    {
        await foreach (long pos in _prefetchChannel.Reader.ReadAllAsync(_cts.Token))
            try
            {
                if (GetSegment(pos) == null) await DownloadSegmentAsync(pos);
            }
            catch (Exception e)
            {
                if (e is not OperationCanceledException)
                    throw;
            }
    }

    // --- Standard Overrides ---

    public override int Read(byte[] buffer, int offset, int count)
    {
        return Read(buffer.AsSpan(offset, count));
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        Position = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => Position + offset,
            SeekOrigin.End => Length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };
        return Position;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _cts.Cancel();
            lock (_cache)
            {
                foreach (StreamSegment s in _cache) s.Dispose();
                _cache.Clear();
            }
        }

        // Console.WriteLine(
        //     $"Missed: {miss} Evicted: {evict} Downloaded: {Utils.ToFileSize(downloaded)} File: {Utils.ToFileSize(Length)}");

        base.Dispose(disposing);
    }

    public override void Flush()
    {
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    private record StreamSegment(long Start, long End, IMemoryOwner<byte> Data) : IDisposable
    {
        public long LastAccess { get; set; } = DateTime.UtcNow.Ticks;

        public void Dispose()
        {
            Data.Dispose();
        }

        public bool Contains(long pos)
        {
            return pos >= Start && pos < End;
        }
    }
}
