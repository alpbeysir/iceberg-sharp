using System.Collections.Concurrent;
using Iceberg.Net.Catalog;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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

            Registration registration = new(
                typeof(TFormat),
                (properties, loggerFactory) => TFormat.Create(properties, loggerFactory));
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
        TablePropertyResolver properties,
        ILoggerFactory? loggerFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(format);

        if (!Registrations.TryGetValue(format, out Registration? registration))
            throw new NotSupportedException(
                $"No data file format provider is registered for '{format}'");

        return (IDataFileFormat)registration.Create(
            properties,
            loggerFactory ?? NullLoggerFactory.Instance);
    }

    private delegate object CreateFormat(
        TablePropertyResolver properties,
        ILoggerFactory loggerFactory);

    private sealed record Registration(
        Type FormatType,
        CreateFormat Create);
}
