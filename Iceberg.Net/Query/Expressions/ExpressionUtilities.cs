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
        MethodInfo instantiated = generics.Length > 0 ? info.MakeGenericMethod(generics.ToArray()) : info;
        return Expression.Call(null, instantiated, args);
    }

    public static void EnableAsmPrint()
    {
        Environment.SetEnvironmentVariable("DOTNET_JitDisasm", "QueryMethod_*");
    }
}