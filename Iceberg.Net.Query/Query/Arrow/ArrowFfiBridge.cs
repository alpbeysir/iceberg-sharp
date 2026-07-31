// Polars.NET.Core / Arrow / ArrowFfiBridge.cs

using Apache.Arrow;
using Apache.Arrow.Types;

namespace Iceberg.Net.Query.Arrow;

public static class ArrowFfiBridge
{
    /// <summary>
    ///     Helper: Convert IEnumerable
    ///     <T>
    ///         directly to RecordBatch
    ///         This bridges the gap between ArrowConverter (returns Array) and ImportDataFrame (needs Batch).
    /// </summary>
    public static RecordBatch BuildRecordBatch<T>(IEnumerable<T> data)
    {
        // Use ArrowConverter to build StructArray
        IArrowArray arrowArray = ArrowConverter.Build(data);

        if (arrowArray is not StructArray structArray)
            throw new ArgumentException(
                $"Type {typeof(T).Name} did not result in a StructArray. Is it a primitive type? DataFrame.ofRecords expects objects/records.");

        // Unbox StructArray as RecordBatch
        StructType? structType = (StructType)structArray.Data.DataType;

        // Build Schema
        Schema schema = new(structType.Fields, null); // null for metadata

        // Build RecordBatch
        return new RecordBatch(schema, structArray.Fields, structArray.Length);
    }
}

public static class ArrowStreamingExtensions
{
    /// <summary>
    ///     Divide huge dataflow as multi-RecordBatch to save memory
    /// </summary>
    /// <param name="source">Source DataFlow </param>
    /// <param name="batchSize"> the Size of each Batch </param>
    public static IEnumerable<RecordBatch> ToArrowBatches<T>(
        this IEnumerable<T> source,
        int batchSize = 100_000)
    {
        // Prepare buffer
        List<T> buffer = new(batchSize);

        foreach (T item in source)
        {
            buffer.Add(item);

            if (buffer.Count >= batchSize)
            {
                yield return BuildBatchFromBuffer(buffer);

                buffer.Clear();
            }
        }

        if (buffer.Count > 0) yield return BuildBatchFromBuffer(buffer);
    }

    private static RecordBatch BuildBatchFromBuffer<T>(List<T> buffer)
    {
        return ArrowFfiBridge.BuildRecordBatch(buffer);
    }
}