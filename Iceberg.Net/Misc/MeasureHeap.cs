namespace Iceberg.Net.Misc;

public readonly struct MeasureHeap : IDisposable
{
    private readonly string _label;
    private readonly long _bytesBefore;

    public MeasureHeap(string label)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        _bytesBefore = GC.GetAllocatedBytesForCurrentThread();
        
        _label = label;
    }

    public void Dispose()
    {
        long bytesAfter = GC.GetAllocatedBytesForCurrentThread();
        long bytesAllocated = bytesAfter - _bytesBefore;
        Console.WriteLine($"--- {_label} heap: {Utils.ToFileSize(bytesAllocated)}");
    }
}