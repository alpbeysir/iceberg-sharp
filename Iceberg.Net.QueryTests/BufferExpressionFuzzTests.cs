using System.Linq.Expressions;
using System.Reflection;
using AwesomeAssertions;
using Iceberg.Net.Query.Expressions;
using Xunit;

namespace Iceberg.Net.QueryTests;

public class BufferExpressionFuzzTests
{
    private static readonly List<TestRow> Rows = TestData.GenerateRows();

    public static TheoryData<TestCase> RandomExpressions()
    {
        TheoryData<TestCase> data = [];
        var rng = new Random(42);
        for (int i = 0; i < 100; i++)
        {
            try
            {
                LambdaExpression expr = GenerateRandomExpr(rng);
                // Pre-flight: skip expressions the visitor can't handle
                try
                {
                    new BufferTransformVisitor().Visit(expr);
                }
                catch (NotImplementedException)
                {
                    continue;
                }

                data.Add(new TestCase(expr, $"fuzz_{i}: {expr}"));
            }
            catch
            {
                // skip unconstructable expressions
            }
        }

        return data;
    }

    private static LambdaExpression GenerateRandomExpr(Random rng)
    {
        var rowParam = Expression.Parameter(typeof(TestRow), "str");
        Expression body = GenerateBody(rng, rowParam, depth: 0, wantBool: false);
        return Expression.Lambda(body, rowParam);
    }

    /// <summary>wantBool: the parent context needs a boolean (for Where/All/Any predicates or && / || combinators)</summary>
    private static Expression GenerateBody(Random rng, ParameterExpression row, int depth, bool wantBool)
    {
        // Weighted choice of expression kinds. At higher depth, bias toward leaves.
        var choices = new List<Func<Expression>>();

        // Always available: leaf (field access or constant)
        choices.Add(() => FieldAccess(rng, row));
        choices.Add(() => Constant(rng));

        if (depth < 4)
        {
            // Arithmetic binary
            choices.Add(() => Arithmetic(rng, row, depth));
            // Comparison (produces bool)
            choices.Add(() => Comparison(rng, row, depth));
            // Boolean combinator (needs bool operands, produces bool)
            choices.Add(() => BooleanCombinator(rng, row, depth));
            // Struct construction
            choices.Add(() => NewExpr(rng, row, depth));
        }

        if (depth < 3)
        {
            // List operations
            choices.Add(() => ListOp(rng, row, depth));
        }

        var expr = choices[rng.Next(choices.Count)]();

        // If the parent needs a bool but we produced a non-bool, wrap in a comparison
        if (wantBool && expr.Type != typeof(bool))
        {
            var op = rng.Next(4) switch
            {
                0 => ExpressionType.GreaterThan,
                1 => ExpressionType.LessThan,
                2 => ExpressionType.Equal,
                _ => ExpressionType.NotEqual,
            };
            return Expression.MakeBinary(op, expr, ConstantOfType(rng, expr.Type));
        }

        return expr;
    }

    // --- Leaf generators ---

    private static Expression FieldAccess(Random rng, ParameterExpression row)
    {
        return rng.Next(4) switch
        {
            0 => Expression.Property(row, "A"), // int?
            1 => Expression.Property(row, "B"), // double
            2 => Expression.Property(Expression.Property(row, "N"), "C"), // int (struct field)
            _ => Expression.Property(row, "L"), // List<int>
        };
    }

    private static Expression Constant(Random rng)
    {
        return rng.Next(3) switch
        {
            0 => Expression.Constant(rng.Next(-100, 1000)),
            1 => Expression.Constant(Math.Round(rng.NextDouble() * 1000, 2)),
            _ => Expression.Constant(rng.Next(2) == 0),
        };
    }

    private static Expression ConstantOfType(Random rng, Type type)
    {
        Type t = Nullable.GetUnderlyingType(type) ?? type;
        if (t == typeof(int)) return Expression.Constant(rng.Next(-100, 1000));
        if (t == typeof(double)) return Expression.Constant(Math.Round(rng.NextDouble() * 1000, 2));
        if (t == typeof(bool)) return Expression.Constant(rng.Next(2) == 0);
        return Expression.Constant(rng.Next(-100, 1000));
    }

    // --- Composite generators ---

    private static Expression Arithmetic(Random rng, ParameterExpression row, int depth)
    {
        var left = GenerateBody(rng, row, depth + 1, wantBool: false);
        var right = GenerateBody(rng, row, depth + 1, wantBool: false);
        var op = rng.Next(4) switch
        {
            0 => ExpressionType.Add,
            1 => ExpressionType.Subtract,
            2 => ExpressionType.Multiply,
            _ => ExpressionType.Divide,
        };
        try
        {
            return Expression.MakeBinary(op, left, right);
        }
        catch
        {
            return left;
        }
    }

    private static Expression Comparison(Random rng, ParameterExpression row, int depth)
    {
        var left = GenerateBody(rng, row, depth + 1, wantBool: false);
        var right = GenerateBody(rng, row, depth + 1, wantBool: false);
        var op = rng.Next(6) switch
        {
            0 => ExpressionType.GreaterThan,
            1 => ExpressionType.LessThan,
            2 => ExpressionType.Equal,
            3 => ExpressionType.NotEqual,
            4 => ExpressionType.GreaterThanOrEqual,
            _ => ExpressionType.LessThanOrEqual,
        };
        try
        {
            return Expression.MakeBinary(op, left, right);
        }
        catch
        {
            return Expression.Constant(false);
        }
    }

    private static Expression BooleanCombinator(Random rng, ParameterExpression row, int depth)
    {
        var left = GenerateBody(rng, row, depth + 1, wantBool: true);
        if (rng.Next(3) == 0) return left; // just one comparison
        var right = GenerateBody(rng, row, depth + 1, wantBool: true);
        var op = rng.Next(2) == 0 ? ExpressionType.AndAlso : ExpressionType.OrElse;
        return Expression.AndAlso(left, right);
    }

    private static Expression NewExpr(Random rng, ParameterExpression row, int depth)
    {
        // anonymous type with a few fields
        var members = new List<(string name, Expression value)>();
        int count = 1 + rng.Next(3);
        for (int i = 0; i < count; i++)
        {
            var val = GenerateBody(rng, row, depth + 1, wantBool: false);
            members.Add(($"f{i}", val));
        }

        // build anonymous type via constructor
        var types = members.Select(m => m.value.Type).ToArray();
        var ctor = GetAnonCtor(types);
        return Expression.New(ctor, members.Select(m => m.value));
    }

    private static ConstructorInfo GetAnonCtor(Type[] types)
    {
        // The compiler-generated anonymous type has a constructor matching field types
        // We use ValueTuple for simplicity instead of true anonymous types
        return types.Length switch
        {
            1 => typeof(Tuple<>).MakeGenericType(types).GetConstructors()[0],
            2 => typeof(Tuple<,>).MakeGenericType(types).GetConstructors()[0],
            3 => typeof(Tuple<,,>).MakeGenericType(types).GetConstructors()[0],
            _ => typeof(Tuple<,,,>).MakeGenericType(types).GetConstructors()[0],
        };
    }

    private static Expression ListOp(Random rng, ParameterExpression row, int depth)
    {
        var listAccess = Expression.Property(row, "L");
        var elemType = typeof(int);
        var elemParam = Expression.Parameter(elemType, "x");

        // Generate a predicate body. Sometimes use a closure (capturing 'row').
        var body = rng.Next(3) switch
        {
            0 => GenerateBody(rng, elemParam, depth + 1, wantBool: false), // uses element param
            1 => GenerateBody(rng, row, depth + 1, wantBool: false), // closure: captures outer row
            _ => Expression.MakeBinary(
                rng.Next(2) == 0 ? ExpressionType.GreaterThan : ExpressionType.LessThan,
                elemParam,
                Expression.Property(Expression.Property(row, "N"), "C")), // element vs struct field
        };

        var pred = Expression.Lambda(body, elemParam);

        var method = rng.Next(3) switch
        {
            0 => "Select",
            1 => "Where",
            _ => "Any",
        };

        return method switch
        {
            "Select" => Expression.Call(
                typeof(Enumerable).GetMethod(nameof(Enumerable.Select),
                        [typeof(IEnumerable<>), typeof(Func<,>)])!
                    .MakeGenericMethod(elemType, body.Type),
                listAccess, pred),
            "Where" => Expression.Call(
                typeof(Enumerable).GetMethod(nameof(Enumerable.Where),
                        [typeof(IEnumerable<>), typeof(Func<,>)])!
                    .MakeGenericMethod(elemType),
                listAccess, pred),
            _ => Expression.Call(
                typeof(Enumerable).GetMethod(nameof(Enumerable.Any),
                        [typeof(IEnumerable<>), typeof(Func<,>)])!
                    .MakeGenericMethod(elemType),
                listAccess, pred),
        };
    }

    [Theory]
    [MemberData(nameof(RandomExpressions))]
    public void Execute_arrow_matches_linq_fuzz(TestCase testCase)
    {
        BufferTestHelpers.RunTest(testCase.Expr, Rows);
    }
}