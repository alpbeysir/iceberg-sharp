using System.Collections;
using System.Linq.CompilerServices.Optimizers;
using System.Linq.Expressions;
using Apache.Arrow.Serialization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Misc;

namespace Iceberg.Net.Query;

public class IcebergQueryProvider<TRow>(Transaction transaction) : IQueryProvider
    where TRow : IArrowSerializer<TRow>
{
    public IQueryable CreateQuery(Expression expression)
    {
        throw new NotImplementedException();
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new IcebergQueryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        throw new NotImplementedException();
    }

    public TResult Execute<TResult>(Expression expression)
    {
        // Console.WriteLine($"Execute {typeof(TResult).PrettyPrint()} {expression}");
        expression = Optimize(expression);

        // var steps = QueryStepVisitor.ConstructSteps(expression, [typeof(TResult), typeof(TRow)]);

        IQueryable<TRow> sourceQueryable = transaction.ReadRows<TRow>().AsQueryable();
        Expression rewritten = ExpressionReplacer<TRow>.Replace(expression, sourceQueryable);
        return sourceQueryable.Provider.Execute<TResult>(rewritten);
    }

    private static Expression Optimize(Expression expression)
    {
        QueryTree? queryTree = new QueryableToQueryTreeConverter().Convert(expression);
        CoalescingOptimizer coalescingOptimizer = new();
        LetOptimizer letOptimizer = new();

        IOptimizer? total = coalescingOptimizer.FixedPoint().Then(letOptimizer).FixedPoint();
        Expression? tree = total.Optimize(queryTree).Reduce();
        return expression;
    }
}

public class IcebergQueryable<T> : IOrderedQueryable<T>
{
    public IcebergQueryable(IQueryProvider provider, Expression? maybeExpression)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression expression = maybeExpression ?? Expression.Constant(this);
        Type type = expression.Type;

        if (!typeof(IQueryable<T>).IsAssignableFrom(type) &&
            !typeof(IEnumerable<T>).IsAssignableFrom(type))
            throw new ArgumentOutOfRangeException(
                nameof(expression),
                "Expression must represent a sequence of the requested element type.");

        Expression = expression;
    }

    public Expression Expression { get; }
    public Type ElementType => typeof(T);

    public IEnumerator<T> GetEnumerator()
    {
        IEnumerable<T> result = Provider.Execute<IEnumerable<T>>(Expression);
        return result.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public IQueryProvider Provider { get; }

    public override string ToString()
    {
        return $"IcebergQueryable<{typeof(T).PrettyPrint()}>";
    }
}

internal sealed class ExpressionReplacer<TRoot> : ExpressionVisitor
{
    private readonly IQueryable _replacement;

    private ExpressionReplacer(IQueryable replacement)
    {
        _replacement = replacement;
    }

    public static Expression Replace(Expression expression, IQueryable replacement)
    {
        return new ExpressionReplacer<TRoot>(replacement).Visit(expression);
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        if (node.Value is IcebergQueryable<TRoot>) return Expression.Constant(_replacement, _replacement.GetType());

        return base.VisitConstant(node);
    }
}