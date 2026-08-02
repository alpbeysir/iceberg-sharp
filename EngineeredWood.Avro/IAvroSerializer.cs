using EngineeredWood.Avro.Encoding;

namespace EngineeredWood.Avro;

public interface IAvroSerializer<T>
{
    AvroSchema Schema { get; }

    void Write(AvroBinaryWriter writer, T value);

    T Read(ref AvroBinaryReader reader);
}