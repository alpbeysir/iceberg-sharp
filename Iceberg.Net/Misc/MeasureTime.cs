using System.Diagnostics;

namespace Iceberg.Net.Misc;

public readonly struct MeasureTime : IDisposable
{
    private readonly string _label;
    private readonly Stopwatch _stopwatch = new();

    public MeasureTime(string label)
    {
        Console.WriteLine($"Start {label}");
        _label = label;
        _stopwatch.Start();
    }

    public void Dispose()
    {
        _stopwatch.Stop();
        Console.WriteLine($"{_label} Elapsed={_stopwatch.Elapsed.TotalMilliseconds}ms");
    }
}