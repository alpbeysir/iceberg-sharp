using Iceberg.Net.Data;

namespace Iceberg.Net.Parquet;

internal static class ModuleInitializer
{
#pragma warning disable CA2255
    [System.Runtime.CompilerServices.ModuleInitializer]
#pragma warning restore CA2255
    internal static void Initialize()
    {
        DataFileFormatRegistry.Register<ParquetDataFileFormat>();
    }
}
