using System.Linq.CompilerServices;
using System.Linq.Expressions;
using System.Threading.Channels;
using Apache.Arrow.Serialization;
using Iceberg.Net.Query.Execution;

namespace Iceberg.Net.Query.Expressions;

internal class QueryStepVisitor : CustomExpressionVisitor<IQueryStep?>
{
    private readonly BufferTransformVisitor _bufferTransformVisitor = new();
    private readonly HashSet<Type> _splittableTypes;
    private int _nestedLambda;

    internal QueryStepVisitor(HashSet<Type> splittableTypes)
    {
        _splittableTypes = splittableTypes;
    }

    protected override IQueryStep? VisitConstant(ConstantExpression node)
    {
        throw new NotImplementedException();
    }

    protected override IQueryStep? VisitParameter(ParameterExpression node)
    {
        throw new NotImplementedException();
    }

    protected override IQueryStep? VisitBinary(BinaryExpression node)
    {
        throw new NotImplementedException();
    }

    protected override IQueryStep? VisitMember(MemberExpression node)
    {
        throw new NotImplementedException();
    }

    protected override IQueryStep? VisitMethodCall(MethodCallExpression node)
    {
        var methodInfo = node.Method;
        var queryable = node.Arguments[0];

        Console.WriteLine(methodInfo.Name);

        foreach (var arg in node.Arguments)
            if (arg is UnaryExpression { NodeType: ExpressionType.Quote } unary)
            {
                var lambda = unary.Unquote();
                Console.WriteLine(lambda.ToString());
            }

        // LambdaDeconstructor.Deconstruct(arg as LambdaExpression, _splittableTypes);
        var expr = Visit(queryable);
        return expr;
    }

    protected override IQueryStep? VisitLambda(LambdaExpression node)
    {
        var transform = _bufferTransformVisitor.Visit(node);
        throw new NotImplementedException();
    }

    protected override IQueryStep VisitUnary(UnaryExpression node)
    {
        throw new NotImplementedException();
    }

    protected override IQueryStep VisitConditional(ConditionalExpression node)
    {
        throw new NotImplementedException();
    }

    protected override IQueryStep VisitNew(NewExpression node)
    {
        throw new NotImplementedException();
    }

    protected override IQueryStep VisitMemberInit(MemberInitExpression node)
    {
        throw new NotImplementedException();
    }

    protected override IQueryStep VisitNewArray(NewArrayExpression node)
    {
        throw new NotImplementedException();
    }

    private static Channel<ColumnBufferSet> TryConvertToBuffers<T>(IEnumerable<T> source)
    {
        var channel = Channel.CreateBounded<ColumnBufferSet>(128);
        if (source is IcebergQueryable<T> icebergQueryable)
        {
            // TODO directly execute and return buffers
        }
        else
        {
            Task.Run(async () =>
            {
                foreach (var chunk in source.Chunk(16384))
                {
                    var batch = RecordBatchBuilder.FromObjects(chunk);
                    // TODO
                    // await channel.Writer.WriteAsync(new ColumnBufferSet(batch));
                }
            });
        }

        return channel;
    }

    // private IQueryStep ConstructStep()
    // {
    //     var currentMethodName = _linqMethods.Peek().Name;
    //     var columnExpression = _currentLambdas
    //         .Select(lambda => LambdaDeconstructor.Deconstruct(lambda, _splittableTypes)).ToList();
    //     var ctx = new LinqConstructionContext
    //         { ColumnExpressionsList = columnExpression, MethodInfo = _linqMethods.Peek() };
    //     var step = currentMethodName switch
    //     {
    //         "Select" or "SelectMany" or "GroupBy" => Project.FromLinq(ctx),
    //         "Where" => Filter.FromLinq(ctx),
    //         _ => throw new ArgumentOutOfRangeException(nameof(currentMethodName), currentMethodName)
    //     };
    //     return step;
    // }

    // protected override Expression VisitLambda<T>(Expression<T> node)
    // {
    //     if (_nestedLambda == 0) _currentLambdas.Add(node);
    //
    //     _nestedLambda++;
    //
    //     var expr = base.VisitLambda(node);
    //
    //     _nestedLambda--;
    //     return expr;
    // }
}