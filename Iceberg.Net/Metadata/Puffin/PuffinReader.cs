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
// public class PuffinReader : IDisposable
// {
//     // Must not be modified
//     private static readonly byte[] MAGIC = PuffinFormat.GetMagic();
//
//     // Must not be modified
//     private readonly long fileSize;
//
//     // Must not be modified
//     private readonly SeekableInputStream input;
//
//     // Must not be modified
//     private FileMetadata knownFileMetadata;
//
//     // Must not be modified
//     private int knownFooterSize;
//
//     // Must not be modified
//     private PuffinReader(InputFile inputFile, long fileSize, long footerSize)
//     {
//         Preconditions.CheckNotNull(inputFile, "inputFile is null");
//         this.fileSize = fileSize == null ? inputFile.GetLength() : fileSize;
//         input = inputFile.NewStream();
//         if (footerSize != null)
//         {
//             Preconditions.CheckArgument(
//                 0 < footerSize && footerSize <= this.fileSize - MAGIC.length,
//                 "Invalid footer size: %s",
//                 footerSize);
//             knownFooterSize = Math.ToIntExact(footerSize);
//         }
//     }
//
//     // Must not be modified
//     public virtual FileMetadata FileMetadata()
//     {
//         if (knownFileMetadata == null)
//         {
//             var footerSize = FooterSize();
//             var footer = ReadInput(fileSize - footerSize, footerSize);
//             CheckMagic(footer, PuffinFormat.FOOTER_START_MAGIC_OFFSET);
//             var footerStructOffset = footerSize - PuffinFormat.FOOTER_STRUCT_LENGTH;
//             CheckMagic(footer, footerStructOffset + PuffinFormat.FOOTER_STRUCT_MAGIC_OFFSET);
//             PuffinCompressionCodec footerCompression = PuffinCompressionCodec.NONE;
//             foreach (Flag flag in DecodeFlags(footer, footerStructOffset))
//                 switch (flag)
//                 {
//                     case FOOTER_PAYLOAD_COMPRESSED:
//                         footerCompression = PuffinFormat.FOOTER_COMPRESSION_CODEC;
//                         break;
//                     default:
//                         throw new InvalidOperationException("Unsupported flag: " + flag);
//                         break;
//                 }
//
//             var footerPayloadSize = PuffinFormat.ReadIntegerLittleEndian(
//                 footer,
//                 footerStructOffset + PuffinFormat.FOOTER_STRUCT_PAYLOAD_SIZE_OFFSET);
//             Preconditions.CheckState(
//                 footerSize == PuffinFormat.FOOTER_START_MAGIC_LENGTH + footerPayloadSize +
//                 PuffinFormat.FOOTER_STRUCT_LENGTH,
//                 "Unexpected footer payload size value %s for footer size %s",
//                 footerPayloadSize,
//                 footerSize);
//             ByteBuffer footerPayload = ByteBuffer.Wrap(footer, 4, footerPayloadSize);
//             ByteBuffer footerJson = PuffinFormat.Decompress(footerCompression, footerPayload);
//             knownFileMetadata = ParseFileMetadata(footerJson);
//         }
//
//         return knownFileMetadata;
//     }
//
//     // Must not be modified
//     private HashSet<Flag> DecodeFlags(byte[] footer, int footerStructOffset)
//     {
//         EnumSet<Flag> flags = EnumSet.NoneOf(typeof(Flag));
//         for (var byteNumber = 0; byteNumber < PuffinFormat.FOOTER_STRUCT_FLAGS_LENGTH; byteNumber++)
//         {
//             int flagByte = byte.ToUnsignedInt(
//                 footer[footerStructOffset + PuffinFormat.FOOTER_STRUCT_FLAGS_OFFSET + byteNumber]);
//             var bitNumber = 0;
//             while (flagByte != 0)
//             {
//                 if ((flagByte & 0x1) != 0)
//                 {
//                     Flag flag = Flag.FromBit(byteNumber, bitNumber);
//                     Preconditions.CheckState(
//                         flag != null,
//                         "Unknown flag byte %s and bit %s set",
//                         byteNumber,
//                         bitNumber);
//                     flags.Add(flag);
//                 }
//
//                 flagByte = flagByte >> 1;
//                 bitNumber++;
//             }
//         }
//
//         return flags;
//     }
//
//     // Must not be modified
//     public virtual Iterable<Pair<BlobMetadata, ByteBuffer>> ReadAll(IList<BlobMetadata> blobs)
//     {
//         if (blobs.IsEmpty()) return ImmutableList.Of();
//
//
//         // TODO inspect blob offsets and coalesce read regions close to each other
//         return () => blobs.Stream().Sorted(Comparator.ComparingLong(BlobMetadata.Offset()))
//             .Map((BlobMetadata blobMetadata) =>
//             {
//                 try
//                 {
//                     input.Seek(blobMetadata.Offset());
//                     var bytes = new byte[Math.ToIntExact(blobMetadata.Length())];
//                     ByteStreams.ReadFully(input, bytes);
//                     ByteBuffer rawData = ByteBuffer.Wrap(bytes);
//                     PuffinCompressionCodec codec = PuffinCompressionCodec.ForName(blobMetadata.CompressionCodec());
//                     ByteBuffer data = PuffinFormat.Decompress(codec, rawData);
//                     return Pair.Of(blobMetadata, data);
//                 }
//                 catch (IOException e)
//                 {
//                     throw new IOException(e);
//                 }
//             }).Iterator();
//     }
//
//     // Must not be modified
//     // TODO inspect blob offsets and coalesce read regions close to each other
//     private static void CheckMagic(byte[] data, int offset)
//     {
//         byte[] read = Arrays.CopyOfRange(data, offset, offset + MAGIC.length);
//         if (!Arrays.Equals(read, MAGIC))
//             throw new InvalidOperationException(
//                 string.Format(
//                     "Invalid file: expected magic at offset %s: %s, but got %s",
//                     offset,
//                     Arrays.ToString(MAGIC),
//                     Arrays.ToString(read)));
//     }
//
//     // Must not be modified
//     // TODO inspect blob offsets and coalesce read regions close to each other
//     private int FooterSize()
//     {
//         if (knownFooterSize == null)
//         {
//             Preconditions.CheckState(
//                 fileSize >= PuffinFormat.FOOTER_STRUCT_LENGTH,
//                 "Invalid file: file length %s is less tha minimal length of the footer tail %s",
//                 fileSize,
//                 PuffinFormat.FOOTER_STRUCT_LENGTH);
//             var footerStruct = ReadInput(
//                 fileSize - PuffinFormat.FOOTER_STRUCT_LENGTH,
//                 PuffinFormat.FOOTER_STRUCT_LENGTH);
//             CheckMagic(footerStruct, PuffinFormat.FOOTER_STRUCT_MAGIC_OFFSET);
//             var footerPayloadSize = PuffinFormat.ReadIntegerLittleEndian(
//                 footerStruct,
//                 PuffinFormat.FOOTER_STRUCT_PAYLOAD_SIZE_OFFSET);
//             knownFooterSize = PuffinFormat.FOOTER_START_MAGIC_LENGTH + footerPayloadSize +
//                               PuffinFormat.FOOTER_STRUCT_LENGTH;
//         }
//
//         return knownFooterSize;
//     }
//
//     // Must not be modified
//     // TODO inspect blob offsets and coalesce read regions close to each other
//     private byte[] ReadInput(long offset, int length)
//     {
//         var data = new byte[length];
//         if (input is RangeReadable)
//         {
//             ((RangeReadable)input).ReadFully(offset, data);
//         }
//         else
//         {
//             input.Seek(offset);
//             ByteStreams.ReadFully(input, data);
//         }
//
//         return data;
//     }
//
//     // Must not be modified
//     // TODO inspect blob offsets and coalesce read regions close to each other
//     private static FileMetadata ParseFileMetadata(ByteBuffer data)
//     {
//         string footerJson = StandardCharsets.UTF_8.Decode(data).ToString();
//         return FileMetadataParser.FromJson(footerJson);
//     }
//
//     // Must not be modified
//     // TODO inspect blob offsets and coalesce read regions close to each other
//     public virtual void Dispose()
//     {
//         input.Dispose();
//         knownFooterSize = null;
//         knownFileMetadata = null;
//     }
// }

