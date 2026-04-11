namespace Iceberg.Net.Query;

public readonly record struct DataFileWriteResult(Uri Location, long RecordCount, long FileSize);

public readonly record struct ManifestFileWriteResult(
    Uri Location,
    long FileSize,
    long AddedRowsCount,
    int AddedFileCount,
    long AddedFilesSize);