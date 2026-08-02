using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Iceberg.Net.Diagnostics;

namespace Iceberg.Net.Tests;

public class PipelineMetricsTests
{
    [Fact]
    public async Task RecordsChannelBackpressureByPipelineStage()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        TaskCompletionSource<(double Wait, string? Stage)> measurement = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using MeterListener listener = new()
        {
            InstrumentPublished = (instrument, configuredListener) =>
            {
                if (instrument.Meter.Name == PipelineMetrics.MeterName &&
                    instrument.Name == PipelineMetrics.ChannelWriteWaitName)
                    configuredListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((_, wait, tags, _) =>
        {
            string? stage = null;
            foreach (KeyValuePair<string, object?> tag in tags)
                if (tag.Key == "stage") stage = tag.Value as string;
            measurement.TrySetResult((wait, stage));
        });
        listener.Start();

        Channel<int> channel = Channel.CreateBounded<int>(1);
        await channel.Writer.WriteAsync(1, cancellationToken);

        ValueTask blockedWrite = PipelineMetrics.WriteAsync(
            channel.Writer,
            2,
            PipelineStage.DataFileRead,
            cancellationToken);
        Assert.False(blockedWrite.IsCompleted);

        Assert.Equal(1, await channel.Reader.ReadAsync(cancellationToken));
        await blockedWrite;

        (double wait, string? stage) = await measurement.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            cancellationToken);
        Assert.True(wait > 0);
        Assert.Equal("data_file_read", stage);
    }
}
