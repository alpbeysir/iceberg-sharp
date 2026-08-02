using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using DotNext;
using DotNext.Numerics;

namespace Iceberg.Net.Query.Expressions;

public sealed class UsableExpression<T> : Expression
{
    public T Value => throw new NotImplementedException();

    public UsableExpression(Expression expr)
    {
        if (expr.Type != typeof(T)) throw new InvalidOperationException();
        Expression = expr;
    }

    public static implicit operator T(UsableExpression<T> _) => throw new InvalidOperationException();

    public Expression Expression { get; init; }
}

public class UseVisitor : ExpressionVisitor
{
    [return: NotNullIfNotNull("node")]
    public override Expression? Visit(Expression? node)
    {
        if (node is MemberExpression me && me.Expression!.Type.Name.Contains("Usable"))
        {
            dynamic param = ((dynamic)me.Expression!.Evaluate()).Expression;
            return param;
        }

        if (node is MethodCallExpression mc && mc.Method.Name.Contains("As"))
        {
            dynamic param = (dynamic)mc.Arguments[0].Evaluate();
            return param;
        }

        return base.Visit(node);
    }
}

public static class ExpressionUtilities
{
    public static Expression Use(Expression<Action> action)
    {
        UseVisitor visitor = new UseVisitor();
        return visitor.Visit(action);
    }

    public static Expression Use<T>(Expression<Func<T>> action)
    {
        UseVisitor visitor = new UseVisitor();
        return ((LambdaExpression)visitor.Visit(action)).Body;
    }

    extension(Expression expression)
    {
        public T As<T>()
        {
            throw new NotImplementedException();
        }

        public UsableExpression<T> AsUsable<T>()
        {
            return new UsableExpression<T>(expression);
        }
    }

    public static MethodCallExpression CallStatic(
        this MethodInfo info,
        Type[] generics,
        Expression[] args)
    {
        MethodInfo instantiated = generics.Length > 0 ? info.MakeGenericMethod(generics.ToArray()) : info;
        return Expression.Call(null, instantiated, args);
    }

    public static LambdaExpression WithName(this LambdaExpression lambda, string name)
    {
        return Expression.Lambda(lambda.Body, name, lambda.TailCall, lambda.Parameters);
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