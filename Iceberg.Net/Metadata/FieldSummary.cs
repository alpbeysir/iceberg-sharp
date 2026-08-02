using EngineeredWood.Expressions;

namespace Iceberg.Net.Metadata;

public readonly record struct FieldSummary(
    bool? ContainsNan,
    bool ContainsNull,
    LiteralValue? LowerBound,
    LiteralValue? UpperBound);
