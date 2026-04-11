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
// public sealed class Blob
// {
//     private readonly ByteBuffer blobData;
//     private readonly IList<int> inputFields;
//     private readonly Dictionary<string, string> properties;
//     private readonly PuffinCompressionCodec requestedCompression;
//     private readonly long sequenceNumber;
//     private readonly long snapshotId;
//     private readonly string type;
//
//     public Blob(string type, IList<int> inputFields, long snapshotId, long sequenceNumber, ByteBuffer blobData) : this(
//         type,
//         inputFields,
//         snapshotId,
//         sequenceNumber,
//         blobData,
//         null,
//         ImmutableMap.Of())
//     {
//     }
//
//     public Blob(
//         string type,
//         IList<int> inputFields,
//         long snapshotId,
//         long sequenceNumber,
//         ByteBuffer blobData,
//         PuffinCompressionCodec requestedCompression,
//         Dictionary<string, string> properties)
//     {
//         Preconditions.CheckNotNull(type, "type is null");
//         Preconditions.CheckNotNull(inputFields, "inputFields is null");
//         Preconditions.CheckNotNull(blobData, "blobData is null");
//         Preconditions.CheckNotNull(properties, "properties is null");
//         this.type = type;
//         this.inputFields = ImmutableList.CopyOf(inputFields);
//         this.snapshotId = snapshotId;
//         this.sequenceNumber = sequenceNumber;
//         this.blobData = blobData;
//         this.requestedCompression = requestedCompression;
//         this.properties = ImmutableMap.CopyOf(properties);
//     }
//
//     public string Type()
//     {
//         return type;
//     }
//
//     public IList<int> InputFields()
//     {
//         return inputFields;
//     }
//
//     public long SnapshotId()
//     {
//         return snapshotId;
//     }
//
//     public long SequenceNumber()
//     {
//         return sequenceNumber;
//     }
//
//     public ByteBuffer BlobData()
//     {
//         return blobData;
//     }
//
//     public PuffinCompressionCodec RequestedCompression()
//     {
//         return requestedCompression;
//     }
//
//     public Dictionary<string, string> Properties()
//     {
//         return properties;
//     }
// }

