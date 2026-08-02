using System.Runtime.CompilerServices;
using Iceberg.Net.Storage;

namespace Iceberg.Net.S3;

internal static class ModuleInitializer
{
#pragma warning disable CA2255
    [System.Runtime.CompilerServices.ModuleInitializer]
#pragma warning restore CA2255
    internal static void Initialize()
    {
        ObjectStorageRegistry.Register<S3ObjectStorage>();
    }
}
