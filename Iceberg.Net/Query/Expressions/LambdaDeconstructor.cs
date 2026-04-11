using System.Collections.Frozen;
using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Iceberg.Net.Misc;

namespace Iceberg.Net.Query.Expressions;

public record ColumnExpressions(
    FrozenDictionary<string, FrozenDictionary<string, ParameterExpression>> Inputs,
    FrozenDictionary<string, Expression> Outputs)
{
    public override string ToString()
    {
        StringBuilder s = new();

        foreach (var (name, param) in Inputs)
        {
            s.Append($"{name}: ");
            foreach (var input in param)
                s.Append($"[{input.Key}, {input.Value.Type.PrettyPrint()}] ");
        }

        s.Append("-> ");

        foreach (var name in Outputs)
            s.Append($"[{name.Key}, {name.Value.Type.PrettyPrint()}] ");

        return s.ToString();
    }
}

public class LambdaDeconstructor : ExpressionVisitor
{
    public const string Root = "__root__";
    private readonly Dictionary<string, Expression> _assignments = [];
    private readonly Dictionary<string, Dictionary<string, ParameterExpression>> _inputParameters;
    private readonly ReadOnlyCollection<ParameterExpression> _sourceParameters;
    private readonly HashSet<Type> _splittableTypes;

    private LambdaDeconstructor(ReadOnlyCollection<ParameterExpression> sourceParameters, HashSet<Type> splittableTypes)
    {
        _sourceParameters = sourceParameters;
        _splittableTypes = splittableTypes;
        _inputParameters = new Dictionary<string, Dictionary<string, ParameterExpression>>(sourceParameters.Count);
        foreach (var t in sourceParameters)
            _inputParameters[t.Name!] = new Dictionary<string, ParameterExpression>();
    }

    public static ColumnExpressions Deconstruct(LambdaExpression lambda, HashSet<Type> splittableTypes)
    {
        var instance = new LambdaDeconstructor(lambda.Parameters, splittableTypes);
        instance.DeconstructOutput(lambda.Body, "");

        var dict = instance._inputParameters.ToFrozenDictionary(
            pair => pair.Key,
            pair => pair.Value.ToFrozenDictionary());

        return new ColumnExpressions(
            dict,
            instance._assignments.ToFrozenDictionary());
    }

    private void DeconstructOutput(Expression expression, string path)
    {
        switch (expression)
        {
            case MemberInitExpression init:
                foreach (var binding in init.Bindings.OfType<MemberAssignment>())
                    DeconstructOutput(binding.Expression, CombinePath(path, binding.Member.Name));
                break;

            case NewExpression { Members: not null } newExpr:
                for (var i = 0; i < newExpr.Arguments.Count; i++)
                    DeconstructOutput(newExpr.Arguments[i], CombinePath(path, newExpr.Members[i].Name));
                break;

            default:
                if (IsDecomposable(expression.Type))
                {
                    ExpandStruct(expression, path);
                }
                else
                {
                    // Now that we know it's a leaf for the OUTPUT, 
                    // we Visit it to create/retrieve the INPUT parameter.
                    var rewrittenLeaf = Visit(expression);
                    var finalKey = string.IsNullOrEmpty(path) ? Root : path;
                    _assignments.Add(finalKey, rewrittenLeaf);
                }

                break;
        }
    }

    private void ExpandStruct(Expression node, string path)
    {
        var members = node.Type.GetMembers(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m is FieldInfo or PropertyInfo);

        foreach (var member in members)
        {
            var memberAccess = Expression.MakeMemberAccess(node, member);
            DeconstructOutput(memberAccess, CombinePath(path, member.Name));
        }
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        var maybeParam = _sourceParameters.FirstOrDefault(param => IsFromParameter(node, param));

        if (maybeParam is not null)
        {
            var path = GetMemberPath(node);
            var paramDict = _inputParameters[maybeParam.Name!];

            if (!paramDict.TryGetValue(path, out var flatParam))
            {
                flatParam = Expression.Parameter(node.Type, $"{path}");
                paramDict.Add(path, flatParam);
            }

            return flatParam;
        }

        return base.VisitMember(node);
    }

    protected override Expression VisitParameter(ParameterExpression node)
    {
        var index = _sourceParameters.IndexOf(node);
        if (index != -1)
        {
            var paramDict = _inputParameters[node.Name!];
            if (!paramDict.TryGetValue(Root, out var flatParam))
            {
                flatParam = Expression.Parameter(node.Type, $"{node.Name!}");
                paramDict.Add(Root, flatParam);
            }

            return flatParam;
        }

        return base.VisitParameter(node);
    }

    private static string GetMemberPath(MemberExpression node)
    {
        var parts = new List<string>();
        var current = node;
        while (current != null)
        {
            parts.Add(current.Member.Name);
            if (current.Expression is ParameterExpression) break;
            current = current.Expression as MemberExpression;
        }

        parts.Reverse();
        return $"{string.Join(".", parts)}";
    }

    private bool IsDecomposable(Type t)
    {
        return _splittableTypes.Contains(t) || t.IsAnonymousType() ||
               // TODO this is hacky
               t.Name.Contains("Tuple");
    }

    private static string CombinePath(string prefix, string name)
    {
        return $"{prefix}{(prefix != "" ? "." : "")}{name}";
    }

    private static bool IsFromParameter(Expression node, ParameterExpression target)
    {
        var current = node;
        while (current is MemberExpression member) current = member.Expression;
        return current == target;
    }
}