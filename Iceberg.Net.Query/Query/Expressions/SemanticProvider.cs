using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using Iceberg.Net.Misc;

namespace Iceberg.Net.Query.Expressions;

public class SemanticProvider : MetadataSemanticProvider
{
    public SemanticProvider()
    {
        PureMembers = PureMemberCatalog.All;
        ConstParameters = ConstParameterCatalog.All;
        ImmutableTypes = ImmutableTypeCatalog.All;
    }
    
    public override bool IsPure(Expression expression)
    {
        if (expression.NodeType == ExpressionType.Convert) return true;
        switch (expression)
        {
            case MethodCallExpression methodCallExpression when
                methodCallExpression.Method.Name.Contains("AccessStructField"):
            case MemberExpression memberExpression when
                memberExpression.Expression.Type.ImplementsInterface(typeof(IInput<>)) &&
                memberExpression.Member.Name == "Array":
                return true;
            case MemberExpression memberExpression when
                memberExpression.Expression.Type.ImplementsInterface(typeof(IArrowArray)) &&
                memberExpression.Member.Name == "Values":
                return true;
            case MethodCallExpression methodCallExpression when
                methodCallExpression.Method.Name.Contains("Slice"):
                return true;
            case MethodCallExpression methodCallExpression when
                methodCallExpression.Method.Name.Contains("Apply"):
                return true;
            case MethodCallExpression methodCallExpression when
                methodCallExpression.Method.Name.Contains("AsWritable"):
                return true;
            case MethodCallExpression methodCallExpression when
                methodCallExpression.Method.Name.Contains("ValueAt"):
                return true;
            case MethodCallExpression methodCallExpression when
                methodCallExpression.Method.Name.Contains("ZipScalarRight"):
                return true;
            case MethodCallExpression methodCallExpression when
                methodCallExpression.Method.Name.Contains("AsReadOnlySpan"):
                return true;
        }
        return base.IsPure(expression);
    }

    public override bool IsConst(ParameterInfo parameter)
    {
        return base.IsConst(parameter);
    }

    public override bool IsImmutable(Type type)
    {
        if (type.ImplementsInterface(typeof(IArrowArray))) return true;
        if (type.Name.Contains("Span")) return true;
        if (type.Name.Contains("Arena")) return true;
        return base.IsImmutable(type);
    }
}