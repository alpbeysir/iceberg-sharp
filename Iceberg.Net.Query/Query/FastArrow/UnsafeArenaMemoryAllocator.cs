using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow.Memory;
using Varena;

namespace Iceberg.Net.Query.FastArrow;

public class UnsafeArenaMemoryAllocator(VirtualBuffer buffer) : MemoryAllocator
{
    private sealed unsafe class ArenaMemoryManager(byte* pointer, int length) : MemoryManager<byte>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override Span<byte> GetSpan()
        {
            return new Span<byte>(pointer, length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            return new MemoryHandle(pointer + elementIndex);
        }

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
        }
    }

    private sealed class ArenaMemoryOwner(Memory<byte> memory) : IMemoryOwner<byte>
    {
        public void Dispose()
        {
        }

        public Memory<byte> Memory { get; } = memory;
    }

    protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
    {
        // Console.WriteLine($"arrow alloc {Utils.ToFileSize(length)}");
        bytesAllocated = length;
        Span<byte> span = buffer.AllocateRange(length);
        unsafe
        {
            var ptr = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(span));
            ArenaMemoryManager manager = new(ptr, length);
            return new ArenaMemoryOwner(manager.Memory);
        }
    }
}