namespace Iceberg.Net.Schemas;

public class IcebergTypeComparer : IEqualityComparer<IIcebergType>
{
    public bool Equals(IIcebergType? x, IIcebergType? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x == null || y == null) return false;
        if (x.GetType() != y.GetType()) return false;

        return x switch
        {
            PrimitiveType p => p.Equals(y),
            StructType s => s.Fields.SequenceEqual(((StructType)y).Fields, new StructFieldComparer()),
            ListType l => l.ElementId == ((ListType)y).ElementId &&
                          l.ElementRequired == ((ListType)y).ElementRequired &&
                          Equals(l.Element, ((ListType)y).Element),
            MapType m => m.KeyId == ((MapType)y).KeyId &&
                         m.ValueId == ((MapType)y).ValueId &&
                         m.ValueRequired == ((MapType)y).ValueRequired &&
                         Equals(m.Key, ((MapType)y).Key) &&
                         Equals(m.Value, ((MapType)y).Value),
            _ => x.Equals(y)
        };
    }

    public int GetHashCode(IIcebergType obj)
    {
        var hc = new HashCode();
        hc.Add(obj.GetType());
        switch (obj)
        {
            case StructType s:
                foreach (var f in s.Fields) hc.Add(f, new StructFieldComparer());
                break;
            case ListType l:
                hc.Add(l.ElementId);
                hc.Add(l.Element, this);
                break;
            case MapType m:
                hc.Add(m.KeyId);
                hc.Add(m.Key, this);
                hc.Add(m.Value, this);
                break;
            default: hc.Add(obj); break;
        }

        return hc.ToHashCode();
    }
}

public class StructFieldComparer : IEqualityComparer<StructField>
{
    public bool Equals(StructField? x, StructField? y)
    {
        return x?.Id == y?.Id && x?.Name == y?.Name && x?.Required == y?.Required &&
               new IcebergTypeComparer().Equals(x?.FieldType, y?.FieldType);
    }

    public int GetHashCode(StructField obj)
    {
        return HashCode.Combine(obj.Id, obj.Name, obj.Required, new IcebergTypeComparer().GetHashCode(obj.FieldType));
    }
}