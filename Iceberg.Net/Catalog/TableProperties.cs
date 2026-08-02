namespace Iceberg.Net.Catalog;

public static class TableProperties
{
    /**
     * Reserved table property for table format version.
     *
     * Iceberg will default a new table's format version to the latest stable and recommended
     * version. This reserved property keyword allows users to override the Iceberg format version of
     * the table metadata.
     *
     * If this table property exists when creating a table, the table will use the specified format
     * version. If a table updates this property, it will try to upgrade to the specified format
     * version.
     *
     * Note: incomplete or unstable versions cannot be selected using this property.
     */
    public static readonly string FormatVersion = "format-version";

    /** Reserved table property for table UUID. */
    public static readonly string Uuid = "uuid";

    /** Reserved table property for the total number of snapshots. */
    public static readonly string SnapshotCount = "snapshot-count";

    /** Reserved table property for current snapshot summary. */
    public static readonly string CurrentSnapshotSummary = "current-snapshot-summary";

    /** Reserved table property for current snapshot id. */
    public static readonly string CurrentSnapshotId = "current-snapshot-id";

    /** Reserved table property for current snapshot timestamp. */
    public static readonly string CurrentSnapshotTimestamp = "current-snapshot-timestamp-ms";

    /** Reserved table property for the JSON representation of current schema. */
    public static readonly string CurrentSchema = "current-schema";

    /** Reserved table property for the JSON representation of current(default) partition spec. */
    public static readonly string DefaultPartitionSpec = "default-partition-spec";

    /** Reserved table property for the JSON representation of current(default) sort order. */
    public static readonly string DefaultSortOrder = "default-sort-order";

    /**
     * Reserved Iceberg table properties list.
     *
     * Reserved table properties are only used to control behaviors when creating or updating a
     * table. The value of these properties are not persisted as a part of the table metadata.
     */
    public static readonly IReadOnlySet<string> ReservedProperties =
        new HashSet<string>
        {
            FormatVersion,
            Uuid,
            SnapshotCount,
            CurrentSnapshotId,
            CurrentSnapshotSummary,
            CurrentSnapshotTimestamp,
            CurrentSchema,
            DefaultPartitionSpec,
            DefaultSortOrder
        };

    /** A table property that documents the business meaning and usage context of this table. */
    public static readonly string Comment = "comment";

    public static readonly string CommitNumRetries = "commit.retry.num-retries";
    public static readonly int CommitNumRetriesDefault = 4;

    public static readonly string CommitMinRetryWaitMs = "commit.retry.min-wait-ms";
    public static readonly int CommitMinRetryWaitMsDefault = 100;

    public static readonly string CommitMaxRetryWaitMs = "commit.retry.max-wait-ms";
    public static readonly int CommitMaxRetryWaitMsDefault = 60 * 1000; // 1 minute

    public static readonly string CommitTotalRetryTimeMs = "commit.retry.total-timeout-ms";
    public static readonly int CommitTotalRetryTimeMsDefault = 30 * 60 * 1000; // 30 minutes

    public static readonly string CommitNumStatusChecks = "commit.status-check.num-retries";
    public static readonly int CommitNumStatusChecksDefault = 3;

    public static readonly string CommitStatusChecksMinWaitMs = "commit.status-check.min-wait-ms";
    public static readonly long CommitStatusChecksMinWaitMsDefault = 1000; // 1 second

    public static readonly string CommitStatusChecksMaxWaitMs = "commit.status-check.max-wait-ms";
    public static readonly long CommitStatusChecksMaxWaitMsDefault = 60 * 1000; // 1 minute

    public static readonly string CommitStatusChecksTotalWaitMs =
        "commit.status-check.total-timeout-ms";

    public static readonly long CommitStatusChecksTotalWaitMsDefault =
        30 * 60 * 1000; // 30 minutes

    public static readonly string ManifestTargetSizeBytes = "commit.manifest.target-size-bytes";
    public static readonly long ManifestTargetSizeBytesDefault = 8 * 1024 * 1024; // 8 MB

    public static readonly string ManifestMinMergeCount = "commit.manifest.min-count-to-merge";
    public static readonly int ManifestMinMergeCountDefault = 100;

    public static readonly string ManifestMergeEnabled = "commit.manifest-merge.enabled";
    public static readonly bool ManifestMergeEnabledDefault = true;

    public static readonly string DefaultFileFormat = "write.format.default";
    public static readonly string DeleteDefaultFileFormat = "write.delete.format.default";
    public static readonly string DefaultFileFormatDefault = "parquet";

    public static readonly string ParquetRowGroupSizeBytes = "write.parquet.row-group-size-bytes";

    public static readonly string DeleteParquetRowGroupSizeBytes =
        "write.delete.parquet.row-group-size-bytes";

    public static readonly int ParquetRowGroupSizeBytesDefault = 128 * 1024 * 1024; // 128 MB

    public static readonly string ParquetPageSizeBytes = "write.parquet.page-size-bytes";

    public static readonly string DeleteParquetPageSizeBytes =
        "write.delete.parquet.page-size-bytes";

    public static readonly int ParquetPageSizeBytesDefault = 1024 * 1024; // 1 MB

    public static readonly string ParquetPageVersion = "write.parquet.page-version";
    public static readonly string DeleteParquetPageVersion = "write.delete.parquet.page-version";
    public static readonly string ParquetPageVersionDefault = "v1";

    public static readonly string ParquetPageRowLimit = "write.parquet.page-row-limit";
    public static readonly string DeleteParquetPageRowLimit = "write.delete.parquet.page-row-limit";
    public static readonly int ParquetPageRowLimitDefault = 20_000;

    public static readonly string ParquetDictSizeBytes = "write.parquet.dict-size-bytes";

    public static readonly string DeleteParquetDictSizeBytes =
        "write.delete.parquet.dict-size-bytes";

    public static readonly int ParquetDictSizeBytesDefault = 2 * 1024 * 1024; // 2 MB

    public static readonly string ParquetCompression = "write.parquet.compression-codec";
    public static readonly string DeleteParquetCompression = "write.delete.parquet.compression-codec";
    public static readonly string ParquetCompressionDefault = "gzip";
    public static readonly string ParquetCompressionDefaultSince140 = "zstd";

    public static readonly string ParquetCompressionLevel = "write.parquet.compression-level";

    public static readonly string DeleteParquetCompressionLevel =
        "write.delete.parquet.compression-level";

    public static readonly string? ParquetCompressionLevelDefault = null;

    public static readonly string ParquetShredVariants = "write.parquet.shred-variants";
    public static readonly bool ParquetShredVariantsDefault = false;

    public static readonly string ParquetVariantBufferSize =
        "write.parquet.variant-inference-buffer-size";

    public static readonly int ParquetVariantBufferSizeDefault = 100;

    public static readonly string ParquetRowGroupCheckMinRecordCount =
        "write.parquet.row-group-check-min-record-count";

    public static readonly string DeleteParquetRowGroupCheckMinRecordCount =
        "write.delete.parquet.row-group-check-min-record-count";

    public static readonly int ParquetRowGroupCheckMinRecordCountDefault = 100;

    public static readonly string ParquetRowGroupCheckMaxRecordCount =
        "write.parquet.row-group-check-max-record-count";

    public static readonly string DeleteParquetRowGroupCheckMaxRecordCount =
        "write.delete.parquet.row-group-check-max-record-count";

    public static readonly int ParquetRowGroupCheckMaxRecordCountDefault = 10000;

    public static readonly string ParquetRowGroupSizeTrackUncompressed =
        "write.parquet.row-group-size-track-uncompressed";

    public static readonly bool ParquetRowGroupSizeTrackUncompressedDefault = false;

    public static readonly string ParquetBloomFilterMaxBytes =
        "write.parquet.bloom-filter-max-bytes";

    public static readonly int ParquetBloomFilterMaxBytesDefault = 1024 * 1024;

    public static readonly string ParquetBloomFilterAdaptiveEnabled =
        "write.parquet.bloom-filter-adaptive-enabled";

    public static readonly bool ParquetBloomFilterAdaptiveEnabledDefault = false;

    public static readonly string ParquetBloomFilterColumnFppPrefix =
        "write.parquet.bloom-filter-fpp.column.";

    public static readonly double ParquetBloomFilterColumnFppDefault = 0.01;

    public static readonly string ParquetBloomFilterColumnNdvPrefix =
        "write.parquet.bloom-filter-ndv.column.";

    public static readonly string ParquetBloomFilterColumnEnabledPrefix =
        "write.parquet.bloom-filter-enabled.column.";

    public static readonly string ParquetColumnStatsEnabledPrefix =
        "write.parquet.stats-enabled.column.";

    public static readonly string ParquetDictEncodingEnabledColumnPrefix =
        "write.parquet.dict-encoding-enabled.column.";

    public static readonly string AvroCompression = "write.avro.compression-codec";
    public static readonly string DeleteAvroCompression = "write.delete.avro.compression-codec";
    public static readonly string AvroCompressionDefault = "gzip";

    public static readonly string AvroCompressionLevel = "write.avro.compression-level";
    public static readonly string DeleteAvroCompressionLevel = "write.delete.avro.compression-level";
    public static readonly string? AvroCompressionLevelDefault = null;

    public static readonly string ManifestCompression = "write.manifest.compression-codec";
    public static readonly string ManifestCompressionDefault = "gzip";

    public static readonly string ManifestCompressionLevel = "write.manifest.compression-level";
    public static readonly string? ManifestCompressionLevelDefault = null;

    public static readonly string OrcStripeSizeBytes = "write.orc.stripe-size-bytes";

    public static readonly string OrcBloomFilterColumns = "write.orc.bloom.filter.columns";
    public static readonly string OrcBloomFilterColumnsDefault = "";

    public static readonly string OrcBloomFilterFpp = "write.orc.bloom.filter.fpp";
    public static readonly double OrcBloomFilterFppDefault = 0.05;

    public static readonly string DeleteOrcStripeSizeBytes = "write.delete.orc.stripe-size-bytes";
    public static readonly long OrcStripeSizeBytesDefault = 64L * 1024 * 1024; // 64 MB

    public static readonly string OrcBlockSizeBytes = "write.orc.block-size-bytes";
    public static readonly string DeleteOrcBlockSizeBytes = "write.delete.orc.block-size-bytes";
    public static readonly long OrcBlockSizeBytesDefault = 256L * 1024 * 1024; // 256 MB

    public static readonly string OrcWriteBatchSize = "write.orc.vectorized.batch-size";
    public static readonly string DeleteOrcWriteBatchSize = "write.delete.orc.vectorized.batch-size";
    public static readonly int OrcWriteBatchSizeDefault = 1024;

    public static readonly string OrcCompression = "write.orc.compression-codec";
    public static readonly string DeleteOrcCompression = "write.delete.orc.compression-codec";
    public static readonly string OrcCompressionDefault = "zlib";

    public static readonly string OrcCompressionStrategy = "write.orc.compression-strategy";

    public static readonly string DeleteOrcCompressionStrategy =
        "write.delete.orc.compression-strategy";

    public static readonly string OrcCompressionStrategyDefault = "speed";

    public static readonly string SplitSize = "read.split.target-size";
    public static readonly long SplitSizeDefault = 128 * 1024 * 1024; // 128 MB

    public static readonly string MetadataSplitSize = "read.split.metadata-target-size";
    public static readonly long MetadataSplitSizeDefault = 32 * 1024 * 1024; // 32 MB

    public static readonly string SplitLookback = "read.split.planning-lookback";
    public static readonly int SplitLookbackDefault = 10;

    public static readonly string SplitOpenFileCost = "read.split.open-file-cost";
    public static readonly long SplitOpenFileCostDefault = 4 * 1024 * 1024; // 4MB

    public static readonly string AdaptiveSplitSizeEnabled = "read.split.adaptive-size.enabled";
    public static readonly bool AdaptiveSplitSizeEnabledDefault = true;

    public static readonly string ParquetVectorizationEnabled = "read.parquet.vectorization.enabled";
    public static readonly bool ParquetVectorizationEnabledDefault = true;

    public static readonly string ParquetBatchSize = "read.parquet.vectorization.batch-size";
    public static readonly int ParquetBatchSizeDefault = 5000;

    public static readonly string OrcVectorizationEnabled = "read.orc.vectorization.enabled";
    public static readonly bool OrcVectorizationEnabledDefault = false;

    public static readonly string OrcBatchSize = "read.orc.vectorization.batch-size";
    public static readonly int OrcBatchSizeDefault = 5000;

    public static readonly string DataPlanningMode = "read.data-planning-mode";
    public static readonly string DeletePlanningMode = "read.delete-planning-mode";
    // public static readonly String PlanningModeDefault = PlanningMode.AUTO.modeName();

    /**
     * When true, declares that the table's identifier fields can be relied upon as a primary key by
     * query engines for optimization purposes (e.g. eliminating redundant joins or distinct). This is
     * not enforced at write time and does not validate existing data.
     */
    public static readonly string IdentifierFieldsRely = "identifier-fields.rely";

    public static readonly bool IdentifierFieldsRelyDefault = false;

    public static readonly string ObjectStoreEnabled = "write.object-storage.enabled";
    public static readonly bool ObjectStoreEnabledDefault = false;

    // Excludes the partition values in the path when set to true and object store is enabled
    public static readonly string WriteObjectStorePartitionedPaths =
        "write.object-storage.partitioned-paths";

    public static readonly bool WriteObjectStorePartitionedPathsDefault = true;


    public static readonly string WriteLocationProviderImpl = "write.location-provider.impl";

    // This only applies to files written after this property is set. Files previously written aren't
    // relocated to reflect this parameter.
    // If not set, defaults to a "data" folder underneath the root path of the table.
    public static readonly string WriteDataLocation = "write.data.path";

    // This only applies to files written after this property is set. Files previously written aren't
    // relocated to reflect this parameter.
    // If not set, defaults to a "metadata" folder underneath the root path of the table.
    public static readonly string WriteMetadataLocation = "write.metadata.path";

    public static readonly string WritePartitionSummaryLimit = "write.summary.partition-limit";
    public static readonly int WritePartitionSummaryLimitDefault = 0;

    /**
     * @deprecated will be removed in 1.12.0, writing manifest lists is always enabled
     */
    [Obsolete] public static readonly string ManifestListsEnabled = "write.manifest-lists.enabled";

    /**
     * @deprecated will be removed in 1.12.0, writing manifest lists is always enabled
     */
    [Obsolete] public static readonly bool ManifestListsEnabledDefault = true;

    public static readonly string MetadataCompression = "write.metadata.compression-codec";
    public static readonly string MetadataCompressionDefault = "none";

    public static readonly string MetadataPreviousVersionsMax =
        "write.metadata.previous-versions-max";

    public static readonly int MetadataPreviousVersionsMaxDefault = 100;

    // This enables to delete the oldest metadata file after commit.
    public static readonly string MetadataDeleteAfterCommitEnabled =
        "write.metadata.delete-after-commit.enabled";

    public static readonly bool MetadataDeleteAfterCommitEnabledDefault = false;

    public static readonly string MetricsMaxInferredColumnDefaults =
        "write.metadata.metrics.max-inferred-column-defaults";

    public static readonly int MetricsMaxInferredColumnDefaultsDefault = 100;

    public static readonly string MetricsModeColumnConfPrefix = "write.metadata.metrics.column.";
    public static readonly string DefaultWriteMetricsMode = "write.metadata.metrics.default";
    public static readonly string DefaultWriteMetricsModeDefault = "truncate(16)";

    public static readonly string DefaultNameMapping = "schema.name-mapping.default";

    public static readonly string WriteAuditPublishEnabled = "write.wap.enabled";
    public static readonly string WriteAuditPublishEnabledDefault = "false";

    public static readonly string WriteTargetFileSizeBytes = "write.target-file-size-bytes";
    public static readonly long WriteTargetFileSizeBytesDefault = 512 * 1024 * 1024; // 512 MB

    public static readonly string DeleteTargetFileSizeBytes = "write.delete.target-file-size-bytes";
    public static readonly long DeleteTargetFileSizeBytesDefault = 64 * 1024 * 1024; // 64 MB

    /**
     * @deprecated will be removed in 1.14.0, use
     *     SparkTableProperties.WRITE_PARTITIONED_FANOUT_ENABLED in iceberg-spark instead.
     */
    [Obsolete] public static readonly string SparkWritePartitionedFanoutEnabled = "write.spark.fanout.enabled";

    /**
     * @deprecated will be removed in 1.14.0, use
     *     SparkTableProperties.WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT in iceberg-spark instead.
     */
    [Obsolete] public static readonly bool SparkWritePartitionedFanoutEnabledDefault = false;

    /**
     * @deprecated will be removed in 1.14.0, use SparkTableProperties.WRITE_ACCEPT_ANY_SCHEMA in
     *     iceberg-spark instead.
     */
    [Obsolete] public static readonly string SparkWriteAcceptAnySchema = "write.spark.accept-any-schema";

    /**
     * @deprecated will be removed in 1.14.0, use SparkTableProperties.WRITE_ACCEPT_ANY_SCHEMA_DEFAULT
     *     in iceberg-spark instead.
     */
    [Obsolete] public static readonly bool SparkWriteAcceptAnySchemaDefault = false;

    /**
     * @deprecated will be removed in 1.14.0, use SparkTableProperties.WRITE_AUTO_SCHEMA_EVOLUTION in
     *     iceberg-spark instead.
     */
    [Obsolete] public static readonly string SparkWriteAutoSchemaEvolution =
        "write.spark.auto-schema-evolution.enabled";

    /**
     * @deprecated will be removed in 1.14.0, use
     *     SparkTableProperties.WRITE_AUTO_SCHEMA_EVOLUTION_DEFAULT in iceberg-spark instead.
     */
    [Obsolete] public static readonly bool SparkWriteAutoSchemaEvolutionDefault = true;

    /**
     * @deprecated will be removed in 1.14.0, use
     *     SparkTableProperties.WRITE_ADVISORY_PARTITION_SIZE_BYTES in iceberg-spark instead.
     */
    [Obsolete] public static readonly string SparkWriteAdvisoryPartitionSizeBytes =
        "write.spark.advisory-partition-size-bytes";

    public static readonly string SnapshotIdInheritanceEnabled =
        "compatibility.snapshot-id-inheritance.enabled";

    public static readonly bool SnapshotIdInheritanceEnabledDefault = false;

    public static readonly string EngineHiveEnabled = "engine.hive.enabled";
    public static readonly bool EngineHiveEnabledDefault = false;

    public static readonly string HiveLockEnabled = "engine.hive.lock-enabled";
    public static readonly bool HiveLockEnabledDefault = true;

    public static readonly string WriteDistributionMode = "write.distribution-mode";
    public static readonly string WriteDistributionModeNone = "none";
    public static readonly string WriteDistributionModeHash = "hash";
    public static readonly string WriteDistributionModeRange = "range";

    public static readonly string GcEnabled = "gc.enabled";
    public static readonly bool GcEnabledDefault = true;

    public static readonly string MaxSnapshotAgeMs = "history.expire.max-snapshot-age-ms";
    public static readonly long MaxSnapshotAgeMsDefault = 5 * 24 * 60 * 60 * 1000; // 5 days

    public static readonly string MinSnapshotsToKeep = "history.expire.min-snapshots-to-keep";
    public static readonly int MinSnapshotsToKeepDefault = 1;

    public static readonly string MaxRefAgeMs = "history.expire.max-ref-age-ms";
    public static readonly long MaxRefAgeMsDefault = long.MaxValue;

    // public static readonly String DeleteGranularity = "write.delete.granularity";
    // public static readonly String DeleteGranularityDefault = DeleteGranularity.PARTITION.toString();

    public static readonly string DeleteIsolationLevel = "write.delete.isolation-level";
    public static readonly string DeleteIsolationLevelDefault = "serializable";

    // public static readonly String DeleteMode = "write.delete.mode";
    // public static readonly String DeleteModeDefault = RowLevelOperationMode.COPY_ON_WRITE.modeName();

    public static readonly string DeleteDistributionMode = "write.delete.distribution-mode";

    public static readonly string UpdateIsolationLevel = "write.update.isolation-level";
    public static readonly string UpdateIsolationLevelDefault = "serializable";

    // public static readonly String UpdateMode = "write.update.mode";
    // public static readonly String UpdateModeDefault = RowLevelOperationMode.COPY_ON_WRITE.modeName();

    public static readonly string UpdateDistributionMode = "write.update.distribution-mode";

    public static readonly string MergeIsolationLevel = "write.merge.isolation-level";
    public static readonly string MergeIsolationLevelDefault = "serializable";

    // public static readonly String MergeMode = "write.merge.mode";
    // public static readonly String MergeModeDefault = RowLevelOperationMode.COPY_ON_WRITE.modeName();

    public static readonly string MergeDistributionMode = "write.merge.distribution-mode";

    public static readonly string UpsertEnabled = "write.upsert.enabled";
    public static readonly bool UpsertEnabledDefault = false;

    public static readonly string EncryptionTableKey = "encryption.key-id";

    public static readonly string EncryptionDekLength = "encryption.data-key-length";
    public static readonly int EncryptionDekLengthDefault = 16;

    public static readonly int EncryptionAadLengthDefault = 16;
}