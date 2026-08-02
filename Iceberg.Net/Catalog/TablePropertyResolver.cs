using System.Globalization;

namespace Iceberg.Net.Catalog;

public sealed class TablePropertyResolver(
    IReadOnlyDictionary<string, string>? properties)
{
    private readonly IReadOnlyDictionary<string, string> _properties =
        properties ?? new Dictionary<string, string>();

    public string? GetString(string property)
    {
        return _properties.GetValueOrDefault(property);
    }

    public string GetString(string property, string defaultValue)
    {
        return GetString(property) ?? defaultValue;
    }

    public bool GetBoolean(string property, bool defaultValue)
    {
        string? value = GetString(property);
        if (value is null) return defaultValue;
        return bool.TryParse(value, out bool parsed) ? parsed : throw InvalidValue(property, value, "a boolean");
    }

    public int GetInt32(string property, int defaultValue)
    {
        return TryGetInt32(property, out int value) ? value : defaultValue;
    }

    public bool TryGetInt32(string property, out int value)
    {
        string? configuredValue = GetString(property);
        if (configuredValue is null)
        {
            value = 0;
            return false;
        }

        return int.TryParse(
            configuredValue,
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out value)
            ? true
            : throw InvalidValue(property, configuredValue, "a 32-bit integer");
    }

    public long GetInt64(string property, long defaultValue)
    {
        string? value = GetString(property);
        if (value is null) return defaultValue;
        return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed)
            ? parsed
            : throw InvalidValue(property, value, "a 64-bit integer");
    }

    public double GetDouble(string property, double defaultValue)
    {
        string? value = GetString(property);
        if (value is null) return defaultValue;
        return double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double parsed)
            ? parsed
            : throw InvalidValue(property, value, "a number");
    }

    public IEnumerable<KeyValuePair<string, string>> GetByPrefix(string prefix)
    {
        foreach (KeyValuePair<string, string> property in _properties)
        {
            if (!property.Key.StartsWith(prefix, StringComparison.Ordinal)) continue;
            yield return new KeyValuePair<string, string>(property.Key[prefix.Length..], property.Value);
        }
    }

    private static FormatException InvalidValue(string property, string value, string expected)
    {
        return new FormatException(
            $"Table property '{property}' has invalid value '{value}'; expected {expected}.");
    }
}