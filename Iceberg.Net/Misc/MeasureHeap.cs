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

        Console.WriteLine($"--- {label} heap");
        _label = label;
    }

    public void Dispose()
    {
        var bytesAfter = GC.GetAllocatedBytesForCurrentThread();
        var bytesAllocated = bytesAfter - _bytesBefore;
        Console.WriteLine($"--- {_label} heap: {Utils.ToFileSize(bytesAllocated)}");
    }
}