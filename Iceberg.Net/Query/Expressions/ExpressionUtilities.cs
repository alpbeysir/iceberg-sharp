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

    public static Expression ForExpression(
        ParameterExpression loopVar,
        Expression initValue,
        Expression condition,
        Expression increment,
        Expression loopContent)
    {
        BinaryExpression initAssign = Expression.Assign(loopVar, initValue);
        LabelTarget breakLabel = Expression.Label("LoopBreak");

        return Expression.Block(
            [loopVar],
            initAssign,
            Expression.Loop(
                Expression.IfThenElse(
                    condition,
                    Expression.Block(
                        loopContent,
                        increment),
                    Expression.Break(breakLabel)),
                breakLabel)
        );
    }
}