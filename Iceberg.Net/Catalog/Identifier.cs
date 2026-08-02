using System.Collections;
namespace Iceberg.Net.Catalog;

public readonly struct Identifier : IEnumerable<string>, IEquatable<Identifier>
{
    private const char DefaultNamespaceSeparator = (char)0x1f;
    private const char HumanReadableNamespaceSeparator = '.';

    public Identifier(IEnumerable<string> parts)
    {
        Parts = new List<string>(parts);
    }

    public Identifier()
    {
        Parts = [];
    }

    public static Identifier? Root => null;

    private List<string> Parts { get; }

    public IEnumerator<string> GetEnumerator()
    {
        return Parts.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public string GetEncoded(char separator = DefaultNamespaceSeparator)
    {
        return string.Join(separator, Parts);
    }

    public string GetEncoded(string separator)
    {
        return string.Join(separator, Parts);
    }

    public static Identifier FromEncoded(string encoded, char separator = DefaultNamespaceSeparator)
    {
        return new Identifier(encoded.Split(separator));
    }

    public Identifier GetParent()
    {
        return new Identifier(Parts.SkipLast(1));
    }

    public string GetLastIdentifier()
    {
        return Parts.Last();
    }

    public string GetTableName()
    {
        return GetLastIdentifier();
    }

    public override string ToString()
    {
        return string.Join(HumanReadableNamespaceSeparator, Parts);
    }


    public void Add(string val)
    {
        Parts.Add(val);
    }

    public static bool operator ==(Identifier? x, Identifier? y)
    {
        if (x is null || y is null)
            return false;

        return x.Value.Parts.SequenceEqual(y.Value.Parts);
    }

    public static bool operator !=(Identifier? x, Identifier? y)
    {
        return !(x == y);
    }

    public bool Equals(Identifier other)
    {
        return this == other;
    }

    public override bool Equals(object? obj)
    {
        return obj is Identifier other && Equals(other);
    }

    public override int GetHashCode()
    {
        return Parts.GetHashCode();
    }
}
