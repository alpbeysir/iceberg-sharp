using System.Linq.Expressions;
using System.Reflection;

namespace Iceberg.Net.Query.Expressions;

public static class ExpressionUtilities
{
    public static MethodCallExpression CallStatic(
        this MethodInfo info,
        Type[] generics,
        Expression[] args)
    {
        var instantiated = generics.Length > 0 ? info.MakeGenericMethod(generics.ToArray()) : info;
        return Expression.Call(null, instantiated, args);
    }
}