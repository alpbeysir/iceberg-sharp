// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Security.Cryptography;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;
using EngineeredWood.Compression;
using EngineeredWood.Encodings;

namespace EngineeredWood.Avro.Container;

/// <summary>
/// Writes Avro Object Container Format (OCF) files: header + data blocks.
/// </summary>
public sealed class OcfWriter : IDisposable
{
    private static readonly byte[] Magic = "Obj\x01"u8.ToArray();

    private readonly Stream _stream;
    private readonly AvroCodec _codec;
    private readonly BlockCompressionLevel? _level;
    private readonly int? _customLevel;
    private readonly byte[] _syncMarker;
    private readonly GrowableBuffer _blockBuffer = new(4096);
    private readonly GrowableBuffer _compressBuffer = new(4096);
    private long _blockObjectCount;
    private bool _headerWritten;
    private bool _finished;

    public OcfWriter(
        Stream stream, AvroCodec codec,
        BlockCompressionLevel? level = null, int? customLevel = null)
    {
        _stream = stream;
        _codec = codec;
        _level = level;
        _customLevel = customLevel;
#if NET6_0_OR_GREATER
        _syncMarker = RandomNumberGenerator.GetBytes(16);
#else
        _syncMarker = new byte[16];
        using (var rng = RandomNumberGenerator.Create())
            rng.GetBytes(_syncMarker);
#endif
    }

    internal void WriteHeader(AvroRecordSchema schema)
    {
        WriteHeader(AvroSchemaWriter.ToJson(schema), null);
    }

    /// <summary>
    /// Writes an OCF header using the schema JSON exactly as supplied and includes
    /// arbitrary application metadata alongside the reserved Avro entries.
    /// </summary>
    public void WriteHeader(
        AvroSchema schema,
        IReadOnlyDictionary<string, byte[]>? metadata = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        if (schema.Parsed is not AvroRecordSchema)
            throw new ArgumentException("Top-level Avro schema must be a record.", nameof(schema));
        WriteHeader(schema.Json, metadata);
    }

    private void WriteHeader(
        string schemaJson,
        IReadOnlyDictionary<string, byte[]>? metadata)
    {
        if (_headerWritten) throw new InvalidOperationException("Header already written.");
        var entries = CreateMetadata(schemaJson, metadata);
        _headerWritten = true;

        // Magic
        _stream.Write(Magic, 0, Magic.Length);

        // Metadata map
        if (_codec != AvroCodec.Null)
            entries["avro.codec"] = System.Text.Encoding.UTF8.GetBytes(AvroCompression.CodecName(_codec));

        WriteMetadataMap(_stream, entries);

        // Sync marker
        _stream.Write(_syncMarker, 0, _syncMarker.Length);
    }

    private static Dictionary<string, byte[]> CreateMetadata(
        string schemaJson,
        IReadOnlyDictionary<string, byte[]>? metadata)
    {
        var entries = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        if (metadata is not null)
        {
            foreach (KeyValuePair<string, byte[]> entry in metadata)
                entries.Add(entry.Key, entry.Value);
        }

        if (entries.ContainsKey("avro.schema") || entries.ContainsKey("avro.codec"))
            throw new ArgumentException(
                "Metadata cannot contain the reserved keys 'avro.schema' or 'avro.codec'.",
                nameof(metadata));

        entries["avro.schema"] = System.Text.Encoding.UTF8.GetBytes(schemaJson);
        return entries;
    }

    /// <summary>
    /// Writes a complete block of pre-encoded data with a known object count.
    /// </summary>
    public void WriteBlock(ReadOnlySpan<byte> encodedData, int objectCount)
    {
        WriteVarLong(_stream, objectCount);
        if (_codec == AvroCodec.Null)
        {
            WriteVarLong(_stream, encodedData.Length);
            WriteSpan(_stream, encodedData);
        }
        else
        {
            _compressBuffer.Reset();
            AvroCompression.Compress(_codec, encodedData, _compressBuffer, _level, _customLevel);
            WriteVarLong(_stream, _compressBuffer.Length);
            WriteSpan(_stream, _compressBuffer.WrittenSpan);
        }
        _stream.Write(_syncMarker, 0, _syncMarker.Length);
    }

    /// <summary>
    /// Appends raw encoded record bytes to the current block buffer.
    /// </summary>
    public void AppendRecord(ReadOnlySpan<byte> encodedRecord)
    {
        _blockBuffer.Write(encodedRecord);
        _blockObjectCount++;
    }

    /// <summary>
    /// Flushes the current block to the output stream.
    /// </summary>
    public void FlushBlock()
    {
        if (_blockObjectCount == 0) return;

        var rawData = _blockBuffer.WrittenSpan;

        // Object count (long varint)
        WriteVarLong(_stream, _blockObjectCount);
        if (_codec == AvroCodec.Null)
        {
            WriteVarLong(_stream, rawData.Length);
            WriteSpan(_stream, rawData);
        }
        else
        {
            _compressBuffer.Reset();
            AvroCompression.Compress(_codec, rawData, _compressBuffer);
            WriteVarLong(_stream, _compressBuffer.Length);
            WriteSpan(_stream, _compressBuffer.WrittenSpan);
        }
        // Sync marker
        _stream.Write(_syncMarker, 0, _syncMarker.Length);

        _blockBuffer.Reset();
        _blockObjectCount = 0;
    }

    public void Finish()
    {
        if (_finished) return;
        _finished = true;
        FlushBlock();
        _stream.Flush();
    }

    public void Dispose()
    {
        if (!_finished) Finish();
    }

    private static void WriteMetadataMap(Stream stream, Dictionary<string, byte[]> entries)
    {
        if (entries.Count > 0)
        {
            // Write block count
            WriteVarLong(stream, entries.Count);
            foreach (var kvp in entries)
            {
                WriteAvroString(stream, kvp.Key);
                WriteAvroBytes(stream, kvp.Value);
            }
        }
        // Terminate map with 0-count block
        WriteVarLong(stream, 0);
    }

    private static void WriteAvroString(Stream stream, string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        WriteVarLong(stream, bytes.Length);
        stream.Write(bytes, 0, bytes.Length);
    }

    private static void WriteAvroBytes(Stream stream, ReadOnlySpan<byte> value)
    {
        WriteVarLong(stream, value.Length);
        WriteSpan(stream, value);
    }

    private static void WriteSpan(Stream stream, ReadOnlySpan<byte> data)
    {
#if NETSTANDARD2_0
        stream.Write(data.ToArray(), 0, data.Length);
#else
        stream.Write(data);
#endif
    }

    private static void WriteVarLong(Stream stream, long value)
    {
        Varint.WriteSigned(stream, value);
    }
}
