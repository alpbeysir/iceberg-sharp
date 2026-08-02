using Iceberg.Net.Catalog;

namespace Iceberg.Net.Rest;

internal static class IdentifierExtensions
{
    public static Identifier ToIdentifier(this TableIdentifier identifier)
    {
        return new Identifier(identifier.Ns.Append(identifier.Name));
    }

    public static TableIdentifier ToTableIdentifier(this Identifier identifier)
    {
        return new TableIdentifier(identifier.GetLastIdentifier(), new Namespace(identifier.GetParent().ToList()));
    }
}
