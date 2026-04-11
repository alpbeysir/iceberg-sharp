using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     Reference to one or more levels of a namespace
/// </summary>
public class Namespace : List<string>
{
    [JsonConstructor]
    public Namespace()
    {
    }

    public Namespace(IList<string> parts) : base(parts)
    {
    }

    public string ToString(char namespaceSeparator = (char)0x1F)
    {
        return string.Join(namespaceSeparator, this);
    }
}