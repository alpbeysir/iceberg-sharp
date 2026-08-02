using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Channels;

namespace Iceberg.Net.Diagnostics;

public enum PipelineStage
{
    ArrowConversion,
    DataFileWrite,
    ManifestWrite,
    ManifestRead,
    ManifestListRead,
    DataFileRead
}

public static class PipelineMetrics
{
    public const string MeterName = "Iceberg.Net";
    public const string ChannelWriteWaitName = "iceberg.pipeline.channel.write.wait";

    private static readonly Meter Meter = new(MeterName);
    private static readonly Histogram<double> ChannelWriteWait = Meter.CreateHistogram<double>(
        ChannelWriteWaitName,
        "s",
        "Time a pipeline producer was blocked waiting for channel capacity.");

    public static async ValueTask WriteAsync<T>(
        ChannelWriter<T> writer,
        T value,
        PipelineStage stage,
        CancellationToken cancellationToken = default)
    {
        ValueTask write = writer.WriteAsync(value, cancellationToken);
        if (write.IsCompletedSuccessfully) return;

        long started = Stopwatch.GetTimestamp();
        await write;

        TagList tags = default;
        tags.Add("stage", StageName(stage));
        ChannelWriteWait.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, tags);
    }

    private static string StageName(PipelineStage stage) => stage switch
    {
        PipelineStage.ArrowConversion => "arrow_conversion",
        PipelineStage.DataFileWrite => "data_file_write",
        PipelineStage.ManifestWrite => "manifest_write",
        PipelineStage.ManifestRead => "manifest_read",
        PipelineStage.ManifestListRead => "manifest_list_read",
        PipelineStage.DataFileRead => "data_file_read",
        _ => throw new ArgumentOutOfRangeException(nameof(stage), stage, null)
    };
}
