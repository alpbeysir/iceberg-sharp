using System.Linq.Expressions;

namespace Iceberg.Net.Query.Expressions;

public abstract class CustomExpressionVisitor<TReturn>
{
    internal TReturn Visit(Expression node)
    {
        return node switch
        {
            // Core Leaf Nodes
            ConstantExpression c => VisitConstant(c),
            ParameterExpression p => VisitParameter(p),

            // Branching Nodes
            BinaryExpression b => VisitBinary(b),
            UnaryExpression u => VisitUnary(u),
            MemberExpression m => VisitMember(m),
            MethodCallExpression mc => VisitMethodCall(mc),
            LambdaExpression l => VisitLambda(l),

            // Advanced Nodes
            ConditionalExpression cond => VisitConditional(cond),
            NewExpression n => VisitNew(n),
            MemberInitExpression mi => VisitMemberInit(mi),
            NewArrayExpression na => VisitNewArray(na),
            _ => ThrowNotSupported(node)
        };
    }

    protected abstract TReturn VisitConstant(ConstantExpression node);
    protected abstract TReturn VisitParameter(ParameterExpression node);
    protected abstract TReturn VisitBinary(BinaryExpression node);
    protected abstract TReturn VisitMember(MemberExpression node);
    protected abstract TReturn VisitMethodCall(MethodCallExpression node);
    protected abstract TReturn VisitLambda(LambdaExpression node);
    protected abstract TReturn VisitUnary(UnaryExpression node);
    protected abstract TReturn VisitConditional(ConditionalExpression node);
    protected abstract TReturn VisitNew(NewExpression node);
    protected abstract TReturn VisitMemberInit(MemberInitExpression node);
    protected abstract TReturn VisitNewArray(NewArrayExpression node);

    private static TReturn ThrowNotSupported(Expression node)
    {
        throw new NotSupportedException($"The type {node.GetType().Name} is not handled by this visitor.");
    }
}