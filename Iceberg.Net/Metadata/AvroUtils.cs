using System.Collections.Immutable;
using Avro;
using Avro.IO;

namespace Iceberg.Net.Metadata;

internal static class AvroUtils
{
    // Iceberg expects custom element-id for arrays
    public class AvroSchemaWrapper(
        Schema wrapped,
        string schemaJson) : Schema(wrapped.Tag, [])
    {
        public override string Name => wrapped.Name;

        public override string ToString()
        {
            return schemaJson;
        }
    }

    extension(Decoder decoder)
    {
        internal T? ReadOptionalStruct<T>(Func<Decoder, T> read) where T : struct
        {
            var unionIndex = decoder.ReadUnionIndex();
            if (unionIndex == 1) return read(decoder);
            return null;
        }

        internal T? ReadOptional<T>(Func<Decoder, T> read)
        {
            var unionIndex = decoder.ReadUnionIndex();
            if (unionIndex == 1) return read(decoder);
            return default;
        }

        internal ImmutableArray<T> ReadArray<T>(Func<Decoder, T> read)
        {
            ImmutableArray<T>.Builder builder = ImmutableArray.CreateBuilder<T>();
            for (var n = decoder.ReadArrayStart(); n > 0; n = decoder.ReadArrayNext())
                builder.Add(read(decoder));
            return builder.ToImmutable();
        }

        internal ImmutableDictionary<TKey, TValue> ReadMap<TKey, TValue>(
            Func<Decoder, TKey> readKey,
            Func<Decoder, TValue> readValue) where TKey : notnull
        {
            ImmutableDictionary<TKey, TValue>.Builder map = ImmutableDictionary.CreateBuilder<TKey, TValue>();
            for (var n = decoder.ReadMapStart(); n > 0; n = decoder.ReadMapNext())
            for (var i = 0; i < n; i++)
                map.Add(readKey(decoder), readValue(decoder));
            return map.ToImmutable();
        }
    }

    extension(Encoder encoder)
    {
        internal void WriteOptionalStruct<T>(T? value, Action<Encoder, T> writeValue) where T : struct
        {
            if (!value.HasValue)
            {
                encoder.WriteUnionIndex(0);
            }
            else
            {
                encoder.WriteUnionIndex(1);
                writeValue(encoder, value.Value);
            }
        }

        internal void WriteOptional<T>(T? value, Action<Encoder, T> writeValue)
        {
            if (value is null)
            {
                encoder.WriteUnionIndex(0);
            }
            else
            {
                encoder.WriteUnionIndex(1);
                writeValue(encoder, value);
            }
        }

        internal void WriteMap<TKey, TValue>(
            ImmutableDictionary<TKey, TValue> map,
            Action<Encoder, TKey> writeKey,
            Action<Encoder, TValue> writeValue) where TKey : notnull
        {
            encoder.WriteMapStart();
            encoder.SetItemCount(map.Count);
            foreach (KeyValuePair<TKey, TValue> kvp in map)
            {
                encoder.StartItem();
                writeKey(encoder, kvp.Key);
                writeValue(encoder, kvp.Value);
            }

            encoder.WriteMapEnd();
        }

        internal void WriteArray<T>(ImmutableArray<T> array, Action<Encoder, T> writeItem)
        {
            encoder.WriteArrayStart();
            encoder.SetItemCount(array.Length);
            foreach (T item in array)
            {
                encoder.StartItem();
                writeItem(encoder, item);
            }

            encoder.WriteArrayEnd();
        }
    }
}