using EngineeredWood.IO;

namespace Iceberg.Net.Storage;

public delegate string? PropertyResolver(string key);

public interface ITableFileSystemFactory
{
    static virtual IReadOnlySet<string> Schemes { get; } = new HashSet<string>();

    static virtual ITableFileSystem Create(Uri uri, PropertyResolver resolve)
    {
        throw new NotSupportedException("The file system implementation does not provide a static factory");
    }
}
