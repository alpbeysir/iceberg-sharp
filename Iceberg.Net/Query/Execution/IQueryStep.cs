using System.Reflection;
using System.Threading.Channels;
using Iceberg.Net.Query.Expressions;

namespace Iceberg.Net.Query.Execution;

internal record LinqConstructionContext
{
    public required MethodInfo MethodInfo { get; init; }
    public required IReadOnlyList<ColumnExpressions> ColumnExpressionsList { get; init; }
}

internal interface IQueryStep
{
    internal Task ExecuteAsync(
        Channel<ColumnBufferSet> outputs,
        CancellationToken cancellationToken = default);
}

internal interface IBufferTransform
{
    internal ColumnBufferSet Execute(ColumnBufferSet input);
}