using ParquetSharp.Schema;

namespace Iceberg.Net.Parquet;

public static class ParquetSchemaExtensions
{
    public static void Visit(this Node node, Action<Node> visitor)
    {
        visitor(node);
        if (node is not GroupNode groupNode) return;

        foreach (Node child in groupNode.Fields)
        {
            child.Visit(visitor);
            child.Dispose();
        }
    }
}
