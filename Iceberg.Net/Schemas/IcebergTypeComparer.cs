namespace Iceberg.Net.Schemas;

public class IcebergTypeComparer : IEqualityComparer<IIcebergType>
{
    public bool Equals(IIcebergType? x, IIcebergType? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x == null || y == null) return false;

        return (x, y) switch
        {
            (PrimitiveType xPrimitive, PrimitiveType yPrimitive) =>
                PrimitiveType.Parse(xPrimitive.Name).Name == PrimitiveType.Parse(yPrimitive.Name).Name,
            (StructType xStruct, StructType yStruct) =>
                xStruct.Fields.SequenceEqual(yStruct.Fields, new StructFieldComparer()),
            (ListType xList, ListType yList) =>
                xList.ElementRequired == yList.ElementRequired &&
                Equals(xList.Element, yList.Element),
            (MapType xMap, MapType yMap) =>
                xMap.ValueRequired == yMap.ValueRequired &&
                Equals(xMap.Key, yMap.Key) &&
                Equals(xMap.Value, yMap.Value),
            _ => false
        };
    }

    public int GetHashCode(IIcebergType obj)
    {
        HashCode hc = new();
        switch (obj)
        {
            case PrimitiveType primitiveType:
                hc.Add(typeof(PrimitiveType));
                hc.Add(PrimitiveType.Parse(primitiveType.Name).Name);
                break;
            case StructType s:
                hc.Add(typeof(StructType));
                foreach (StructField f in s.Fields) hc.Add(f, new StructFieldComparer());
                break;
            case ListType l:
                hc.Add(typeof(ListType));
                hc.Add(l.ElementRequired);
                hc.Add(l.Element, this);
                break;
            case MapType m:
                hc.Add(typeof(MapType));
                hc.Add(m.ValueRequired);
                hc.Add(m.Key, this);
                hc.Add(m.Value, this);
                break;
            default:
                hc.Add(obj.GetType());
                hc.Add(obj);
                break;
        }

        return hc.ToHashCode();
    }
}

public class StructFieldComparer : IEqualityComparer<StructField>
{
    public bool Equals(StructField? x, StructField? y)
    {
        return x?.Name == y?.Name && x?.Required == y?.Required &&
               new IcebergTypeComparer().Equals(x?.FieldType, y?.FieldType);
    }

    public int GetHashCode(StructField obj)
    {
        return HashCode.Combine(obj.Name, obj.Required, new IcebergTypeComparer().GetHashCode(obj.FieldType));
    }
}
