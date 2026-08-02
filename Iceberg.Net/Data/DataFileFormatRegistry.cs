using System.Collections.Concurrent;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.Data;

public static class DataFileFormatRegistry
{
    private static readonly ConcurrentDictionary<string, Registration> Registrations =
        new(StringComparer.OrdinalIgnoreCase);

    public static void Register<TFormat>() where TFormat : IDataFileFormat
    {
        foreach (string format in TFormat.Formats)
        {
            if (string.IsNullOrWhiteSpace(format))
                throw new InvalidOperationException(
                    $"{typeof(TFormat).FullName} declared an empty data file format");

            Registration registration = new(typeof(TFormat), TFormat.Create);
            Registrations.AddOrUpdate(
                format,
                registration,
                (_, existing) => existing.FormatType == typeof(TFormat)
                    ? existing
                    : throw new InvalidOperationException(
                        $"Data file format '{format}' is already registered by {existing.FormatType.FullName}"));
        }
    }

    public static IDataFileFormat Resolve(
        string format,
        TablePropertyResolver properties)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(format);

        if (!Registrations.TryGetValue(format, out Registration? registration))
            throw new NotSupportedException(
                $"No data file format provider is registered for '{format}'");

        return registration.Create(properties);
    }

    private sealed record Registration(
        Type FormatType,
        Func<TablePropertyResolver, IDataFileFormat> Create);
}
