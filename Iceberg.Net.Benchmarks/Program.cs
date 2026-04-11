using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using ConfigOptions = BenchmarkDotNet.Configs.ConfigOptions;
using DefaultConfig = BenchmarkDotNet.Configs.DefaultConfig;
using ManualConfig = BenchmarkDotNet.Configs.ManualConfig;

namespace Iceberg.Net.Benchmarks;

internal static class Program
{
    public static int Main(string[] args)
    {
        Console.WriteLine("Working directory: {0}", Environment.CurrentDirectory);

        var config = ManualConfig.Create(DefaultConfig.Instance);


#if DEBUG
        var job = Job.MediumRun
            .WithLaunchCount(1)
            .WithToolchain(InProcessEmitToolchain.Instance)
            .WithWarmupCount(1)
            .WithIterationCount(3);

        config.WithOptions(ConfigOptions.DisableOptimizationsValidator);
#else
        var job = Job.Dry;
#endif

        config
            .WithOptions(ConfigOptions.Default | ConfigOptions.StopOnFirstError)
            .AddDiagnoser(MemoryDiagnoser.Default)
            .AddJob(job);

        BenchmarkRunner.Run<DataLayouts>(config);

        return 0;
    }
}