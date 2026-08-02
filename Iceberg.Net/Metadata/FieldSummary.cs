using Avro.IO;

namespace Iceberg.Net.Metadata;

public readonly record struct FieldSummary(bool? ContainsNan, bool ContainsNull, byte[]? LowerBound, byte[]? UpperBound)
{
    internal static FieldSummary Read(Decoder decoder)
    {
        bool containsNull = decoder.ReadBoolean();
        bool? containsNan = decoder.ReadOptional(d => d.ReadBoolean());
        byte[]? lowerBound = decoder.ReadOptional(d => d.ReadBytes());
        byte[]? upperBound = decoder.ReadOptional(d => d.ReadBytes());
        return new FieldSummary(containsNan, containsNull, lowerBound, upperBound);
    }

    internal static void Write(Encoder encoder, FieldSummary instance)
    {
        encoder.WriteBoolean(instance.ContainsNull);
        if (instance.ContainsNan == null)
        {
            encoder.WriteUnionIndex(0);
        }
        else
        {
            encoder.WriteUnionIndex(1);
            encoder.WriteBoolean(instance.ContainsNan.Value);
        }

        if (instance.LowerBound == null)
        {
            encoder.WriteUnionIndex(0);
        }
        else
        {
            encoder.WriteUnionIndex(1);
            encoder.WriteBytes(instance.LowerBound);
        }

        if (instance.UpperBound == null)
        {
            encoder.WriteUnionIndex(0);
        }
        else
        {
            encoder.WriteUnionIndex(1);
            encoder.WriteBytes(instance.UpperBound);
        }
    }
}