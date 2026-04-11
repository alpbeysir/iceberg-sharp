// /*
//  * Licensed to the Apache Software Foundation (ASF) under one
//  * or more contributor license agreements.  See the NOTICE file
//  * distributed with this work for additional information
//  * regarding copyright ownership.  The ASF licenses this file
//  * to you under the Apache License, Version 2.0 (the
//  * "License"); you may not use this file except in compliance
//  * with the License.  You may obtain a copy of the License at
//  *
//  *   http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing,
//  * software distributed under the License is distributed on an
//  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  * KIND, either express or implied.  See the License for the
//  * specific language governing permissions and limitations
//  * under the License.
//  */
//
// namespace Org.Apache.Iceberg.Puffin;
//
// internal sealed class PuffinFormat
// {
//     private static readonly int FOOTER_START_MAGIC_OFFSET = 0;
//
//     private static readonly int FOOTER_START_MAGIC_LENGTH = GetMagic().length;
//
//     // "Footer struct" denotes the fixed-length portion of the Footer
//     private static readonly int FOOTER_STRUCT_PAYLOAD_SIZE_OFFSET = 0;
//     private static readonly int FOOTER_STRUCT_FLAGS_OFFSET = FOOTER_STRUCT_PAYLOAD_SIZE_OFFSET + 4;
//     private static readonly int FOOTER_STRUCT_FLAGS_LENGTH = 4;
//     private static readonly int FOOTER_STRUCT_MAGIC_OFFSET = FOOTER_STRUCT_FLAGS_OFFSET + FOOTER_STRUCT_FLAGS_LENGTH;
//     private static readonly int FOOTER_STRUCT_LENGTH = FOOTER_STRUCT_MAGIC_OFFSET + GetMagic().length;
//     private static readonly PuffinCompressionCodec FOOTER_COMPRESSION_CODEC = PuffinCompressionCodec.LZ4;
//
//     private PuffinFormat()
//     {
//     }
//
//     private static byte[] GetMagic()
//     {
//         return new byte[]
//         {
//             0x50,
//             0x46,
//             0x41,
//             0x31
//         };
//     }
//
//     private static void WriteIntegerLittleEndian(OutputStream outputStream, int value)
//     {
//         outputStream.Write(0xFF & value);
//         outputStream.Write(0xFF & (value >> 8));
//         outputStream.Write(0xFF & (value >> 16));
//         outputStream.Write(0xFF & (value >> 24));
//     }
//
//     private static int ReadIntegerLittleEndian(byte[] data, int offset)
//     {
//         return byte.ToUnsignedInt(data[offset]) | (byte.ToUnsignedInt(data[offset + 1]) << 8) |
//                (byte.ToUnsignedInt(data[offset + 2]) << 16) | (byte.ToUnsignedInt(data[offset + 3]) << 24);
//     }
//
//     private static ByteBuffer Compress(PuffinCompressionCodec codec, ByteBuffer input)
//     {
//         switch (codec)
//         {
//             case NONE:
//                 return input.Duplicate();
//             case LZ4:
//
//                 // TODO requires LZ4 frame compressor, e.g.
//                 // https://github.com/airlift/aircompressor/pull/142
//                 break;
//             case ZSTD:
//                 return Compress(new ZstdCompressor(), input);
//         }
//
//         throw new NotSupportedException("Unsupported codec: " + codec);
//     }
//
//     private static ByteBuffer Compress(Compressor compressor, ByteBuffer input)
//     {
//         ByteBuffer output = ByteBuffer.Allocate(compressor.MaxCompressedLength(input.Remaining()));
//         compressor.Compress(input.Duplicate(), output);
//         output.Flip();
//         return output;
//     }
//
//     private static ByteBuffer Decompress(PuffinCompressionCodec codec, ByteBuffer input)
//     {
//         switch (codec)
//         {
//             case NONE:
//                 return input.Duplicate();
//             case LZ4:
//
//                 // TODO requires LZ4 frame decompressor, e.g.
//                 // https://github.com/airlift/aircompressor/pull/142
//                 break;
//             case ZSTD:
//                 return DecompressZstd(input);
//         }
//
//         throw new NotSupportedException("Unsupported codec: " + codec);
//     }
//
//     private static ByteBuffer DecompressZstd(ByteBuffer input)
//     {
//         byte[] inputBytes;
//         int inputOffset;
//         int inputLength;
//         if (input.HasArray())
//         {
//             inputBytes = input.Array();
//             inputOffset = input.ArrayOffset();
//             inputLength = input.Remaining();
//         }
//         else
//         {
//             // TODO implement ZstdDecompressor.getDecompressedSize for ByteBuffer to avoid copying
//             inputBytes = ByteBuffers.ToByteArray(input);
//             inputOffset = 0;
//             inputLength = inputBytes.length;
//         }
//
//         var decompressed =
//             new byte[Math.ToIntExact(ZstdDecompressor.GetDecompressedSize(inputBytes, inputOffset, inputLength))];
//         int decompressedLength = new ZstdDecompressor().Decompress(
//             inputBytes,
//             inputOffset,
//             inputLength,
//             decompressed,
//             0,
//             decompressed.length);
//         Preconditions.CheckState(decompressedLength == decompressed.length, "Invalid decompressed length");
//         return ByteBuffer.Wrap(decompressed);
//     }
//
//     private enum Flag
//     {
//         // FOOTER_PAYLOAD_COMPRESSED(0, 0)
//         FOOTER_PAYLOAD_COMPRESSED
//
//         // --------------------
//         // TODO enum body members
//         // /**/
//         // private static final Map<Pair<Integer, Integer>, Flag> BY_BYTE_AND_BIT = Stream.of(values()).collect(ImmutableMap.toImmutableMap(flag -> Pair.of(flag.byteNumber(), flag.bitNumber()), Function.identity()));
//         // private final int byteNumber;
//         // private final int bitNumber;
//         // Flag(int byteNumber, int bitNumber) {
//         //     Preconditions.checkArgument(0 <= byteNumber && byteNumber < PuffinFormat.FOOTER_STRUCT_FLAGS_LENGTH, "Invalid byteNumber");
//         //     Preconditions.checkArgument(0 <= bitNumber && bitNumber < Byte.SIZE, "Invalid bitNumber");
//         //     this.byteNumber = byteNumber;
//         //     this.bitNumber = bitNumber;
//         // }
//         // @Nullable
//         // static Flag fromBit(int byteNumber, int bitNumber) {
//         //     return BY_BYTE_AND_BIT.get(Pair.of(byteNumber, bitNumber));
//         // }
//         // public int byteNumber() {
//         //     return byteNumber;
//         // }
//         // public int bitNumber() {
//         //     return bitNumber;
//         // }
//         // --------------------
//     }
// }

