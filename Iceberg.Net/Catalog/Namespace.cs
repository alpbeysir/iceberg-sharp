namespace Iceberg.Net.Catalog;

public record Namespace(Identifier Identifier, IAsyncEnumerable<INode> Children) : INode
{
    public override string ToString()
    {
        return Identifier.GetLastIdentifier();
    }
}