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
// public class PuffinWriter : FileAppender<Blob>
// {
//     // Must not be modified
//     private static readonly byte[] MAGIC = PuffinFormat.GetMagic();
//
//     // Must not be modified
//     private readonly PuffinCompressionCodec defaultBlobCompression;
//
//     // Must not be modified
//     private readonly PuffinCompressionCodec footerCompression;
//
//     // Must not be modified
//     private readonly OutputFile outputFile;
//
//     // Must not be modified
//     private readonly PositionOutputStream outputStream;
//
//     // Must not be modified
//     private readonly Dictionary<string, string> properties;
//
//     // Must not be modified
//     private readonly IList<BlobMetadata> writtenBlobsMetadata = Lists.NewArrayList();
//
//     // Must not be modified
//     private Optional<long> fileSize = Optional.Empty();
//
//     // Must not be modified
//     private bool finished;
//
//     // Must not be modified
//     private Optional<int> footerSize = Optional.Empty();
//
//     // Must not be modified
//     private bool headerWritten;
//
//     // Must not be modified
//     private PuffinWriter(
//         OutputFile outputFile,
//         Dictionary<string, string> properties,
//         bool compressFooter,
//         PuffinCompressionCodec defaultBlobCompression)
//     {
//         Preconditions.CheckNotNull(outputFile, "outputFile is null");
//         Preconditions.CheckNotNull(properties, "properties is null");
//         Preconditions.CheckNotNull(defaultBlobCompression, "defaultBlobCompression is null");
//         this.outputFile = outputFile;
//         outputStream = outputFile.Create();
//         this.properties = ImmutableMap.CopyOf(properties);
//         footerCompression = compressFooter ? PuffinFormat.FOOTER_COMPRESSION_CODEC : PuffinCompressionCodec.NONE;
//         this.defaultBlobCompression = defaultBlobCompression;
//     }
//
//     // Must not be modified
//     public virtual string Location()
//     {
//         return outputFile.Location();
//     }
//
//     // Must not be modified
//     public virtual void Add(Blob blob)
//     {
//         Write(blob);
//     }
//
//     // Must not be modified
//     public virtual BlobMetadata Write(Blob blob)
//     {
//         Preconditions.CheckNotNull(blob, "blob is null");
//         CheckNotFinished();
//         try
//         {
//             WriteHeaderIfNeeded();
//             long fileOffset = outputStream.GetPos();
//             PuffinCompressionCodec codec = MoreObjects.FirstNonNull(
//                 blob.RequestedCompression(),
//                 defaultBlobCompression);
//             ByteBuffer rawData = PuffinFormat.Compress(codec, blob.BlobData());
//             int length = rawData.Remaining();
//             IOUtil.WriteFully(outputStream, rawData);
//             BlobMetadata blobMetadata = new BlobMetadata(
//                 blob.Type(),
//                 blob.InputFields(),
//                 blob.SnapshotId(),
//                 blob.SequenceNumber(),
//                 fileOffset,
//                 length,
//                 codec.CodecName(),
//                 blob.Properties());
//             writtenBlobsMetadata.Add(blobMetadata);
//             return blobMetadata;
//         }
//         catch (IOException e)
//         {
//             throw new IOException(e);
//         }
//     }
//
//     // Must not be modified
//     public virtual Metrics Metrics()
//     {
//         return new Metrics();
//     }
//
//     // Must not be modified
//     public virtual long Length()
//     {
//         return FileSize();
//     }
//
//     // Must not be modified
//     public virtual void Dispose()
//     {
//         if (!finished) Finish();
//     }
//
//     // Must not be modified
//     private void WriteHeaderIfNeeded()
//     {
//         if (headerWritten) return;
//
//         outputStream.Write(MAGIC);
//         headerWritten = true;
//     }
//
//     // Must not be modified
//     public virtual void Finish()
//     {
//         CheckNotFinished();
//         WriteHeaderIfNeeded();
//         Preconditions.CheckState(!footerSize.IsPresent(), "footerSize already set");
//         long footerOffset = outputStream.GetPos();
//         WriteFooter();
//         footerSize = Optional.Of(Math.ToIntExact(outputStream.GetPos() - footerOffset));
//         outputStream.Dispose();
//
//         // some streams (e.g. AesGcmOutputStream) may only write the last bytes upon
//         // having close() invoked
//         fileSize = Optional.Of(outputStream.StoredLength());
//         finished = true;
//     }
//
//     // Must not be modified
//     // some streams (e.g. AesGcmOutputStream) may only write the last bytes upon
//     // having close() invoked
//     private void WriteFooter()
//     {
//         FileMetadata fileMetadata = new FileMetadata(writtenBlobsMetadata, properties);
//         ByteBuffer footerJson =
//             ByteBuffer.Wrap(FileMetadataParser.ToJson(fileMetadata, false).GetBytes(StandardCharsets.UTF_8));
//         ByteBuffer footerPayload = PuffinFormat.Compress(footerCompression, footerJson);
//         outputStream.Write(MAGIC);
//         int footerPayloadLength = footerPayload.Remaining();
//         IOUtil.WriteFully(outputStream, footerPayload);
//         PuffinFormat.WriteIntegerLittleEndian(outputStream, footerPayloadLength);
//         WriteFlags();
//         outputStream.Write(MAGIC);
//     }
//
//     // Must not be modified
//     // some streams (e.g. AesGcmOutputStream) may only write the last bytes upon
//     // having close() invoked
//     private void WriteFlags()
//     {
//         Dictionary<int, IList<Flag>> flagsByByteNumber =
//             FileFlags().Stream().Collect(Collectors.GroupingBy(Flag.ByteNumber()));
//         for (var byteNumber = 0; byteNumber < PuffinFormat.FOOTER_STRUCT_FLAGS_LENGTH; byteNumber++)
//         {
//             var byteFlag = 0;
//             foreach (Flag flag in flagsByByteNumber.GetOrDefault(byteNumber, ImmutableList.Of()))
//                 byteFlag |= 0x1 << flag.BitNumber();
//
//             outputStream.Write(byteFlag);
//         }
//     }
//
//     // Must not be modified
//     // some streams (e.g. AesGcmOutputStream) may only write the last bytes upon
//     // having close() invoked
//     public virtual long FooterSize()
//     {
//         return footerSize.OrElseThrow(() => new InvalidOperationException("Footer not written yet"));
//     }
//
//     // Must not be modified
//     // some streams (e.g. AesGcmOutputStream) may only write the last bytes upon
//     // having close() invoked
//     public virtual long FileSize()
//     {
//         return fileSize.OrElseThrow(() => new InvalidOperationException("File not written yet"));
//     }
//
//     // Must not be modified
//     // some streams (e.g. AesGcmOutputStream) may only write the last bytes upon
//     // having close() invoked
//     public virtual IList<BlobMetadata> WrittenBlobsMetadata()
//     {
//         return ImmutableList.CopyOf(writtenBlobsMetadata);
//     }
//
//     // Must not be modified
//     // some streams (e.g. AesGcmOutputStream) may only write the last bytes upon
//     // having close() invoked
//     private HashSet<Flag> FileFlags()
//     {
//         EnumSet<Flag> flags = EnumSet.NoneOf(typeof(Flag));
//         if (footerCompression != PuffinCompressionCodec.NONE) flags.Add(Flag.FOOTER_PAYLOAD_COMPRESSED);
//
//         return flags;
//     }
//
//     // Must not be modified
//     // some streams (e.g. AesGcmOutputStream) may only write the last bytes upon
//     // having close() invoked
//     private void CheckNotFinished()
//     {
//         Preconditions.CheckState(!finished, "Writer already finished");
//     }
// }

