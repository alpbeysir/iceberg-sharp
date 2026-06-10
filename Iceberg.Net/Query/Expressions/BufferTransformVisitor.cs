using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq.CompilerServices;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Avro;
using DotNext.Linq.Expressions;
using DotNext.Metaprogramming;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.FastArrow;
using Varena;

namespace Iceberg.Net.Query.Expressions;

public sealed class ExecutionContext
{
    public required VirtualBuffer Arena { get; init; }
    public required UnsafeArenaMemoryAllocator ArrowAllocator { get; init; }
}

public enum InputType
{
    Identity,
    Ranged,
    Masked,
    Indexed
}

public interface IInput<out TArray> where TArray : IArrowArray
{
    public TArray Array { get; }
    public int Length { get; }
}

public readonly record struct IdentityInput<TArray>(TArray Array)
    : IInput<TArray> where TArray : IArrowArray
{
    public IdentityInput<TReturn> Apply<TReturn>(TReturn array) where TReturn : IArrowArray
    {
        return new IdentityInput<TReturn>(array);
    }

    public static IdentityInput<TArray> New(TArray array)
    {
        return new IdentityInput<TArray>(array);
    }

    public int Length => Array.Length;
}

public readonly record struct RangedInput<TArray>(TArray Array, Range Range)
    : IInput<TArray> where TArray : IArrowArray
{
    public RangedInput<TReturn> Apply<TReturn>(TReturn array) where TReturn : IArrowArray
    {
        return new RangedInput<TReturn>(array, Range);
    }
    
    public ReadOnlySpan<T> Slice<T>(ReadOnlySpan<T> span)
    {
        (int Offset, int Length) offsetAndLength = Range.GetOffsetAndLength(span.Length);
        return span.Slice(offsetAndLength.Offset, offsetAndLength.Length);
    }

    public int Length => Array.Length - Range.GetOffsetAndLength(Array.Length).Length;
}

public readonly record struct MaskedInput<TArray>(TArray Array, BooleanArray Mask)
    : IInput<TArray> where TArray : IArrowArray
{
    public MaskedInput<TReturn> Apply<TReturn>(TReturn array) where TReturn : IArrowArray
    {
        return new MaskedInput<TReturn>(array, Mask);
    }

    public int Length => throw new NotImplementedException();
}

public readonly record struct IndexedInput<TArray>(TArray Array, int Index)
    : IInput<TArray> where TArray : IArrowArray
{
    public IndexedInput<TReturn> Apply<TReturn>(TReturn array) where TReturn : IArrowArray
    {
        return new IndexedInput<TReturn>(array, Index);
    }

    public int Length => 1;
}

public class BufferTransformVisitor : ExpressionVisitorNarrow<Expression, LambdaExpression, Expression,
    NewExpression, ElementInit, MemberBinding, MemberAssignment, MemberListBinding, MemberMemberBinding>
{
    private readonly Stack<Dictionary<string, ParameterExpression>> _bindings = [];
    private readonly ParameterExpression _ctxParam = Expression.Parameter(typeof(ExecutionContext), "ctx");
    private readonly Dictionary<MemberInfo, int> _memberIndex = [];
    private readonly Stack<Expression?> _builderStack = new();
    private readonly Stack<InputType[]?> _inputTypes = new([[InputType.Identity]]);
    private const int MaxBatchSize = 65536;

    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        Type returnType = Nullable.GetUnderlyingType(node.ReturnType) ?? node.ReturnType;
        ParameterExpression resultBuilderParam = Expression.Parameter(
            GetConcreteBuilderType(GetBufferType(returnType)),
            "builder");

        _builderStack.Push(resultBuilderParam);
        
        _bindings.Push([]);
        Dictionary<string, ParameterExpression> curBindings = _bindings.Peek();
        foreach ((var index, ParameterExpression expression) in node.Parameters.Index())
            curBindings.Add(
                expression.Name!,
                Expression.Parameter(MakeInputTypeForArray(GetBufferType(expression.Type), index), expression.Name));

        Expression? expr = base.VisitLambda(node);

        _builderStack.Pop();
        
        _bindings.Pop();

        return expr;
    }

    protected override Expression VisitNew(NewExpression node)
    {
        Type structType = node.Type;

        List<MemberInfo> members = structType.GetMembers(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m is PropertyInfo or FieldInfo).ToList();

        List<Expression> visitedArgs = new(node.Arguments.Count);
        for (var i = 0; i < node.Arguments.Count; i++)
        {
            Type memberType = Utils.PropertyOrFieldType(members[i]);
            Type underlying = Nullable.GetUnderlyingType(memberType) ?? memberType;

            if (underlying.IsClass || underlying is { IsValueType: true, IsPrimitive: false })
            {
                Expression? parentBuilder = _builderStack.Peek();
                MethodCallExpression subBuilder = Expression.Call(
                    parentBuilder,
                    typeof(StructArrayBuilder).GetMethod(
                            nameof(StructArrayBuilder.GetFieldBuilder))!
                        .MakeGenericMethod(typeof(StructArrayBuilder)),
                    Expression.Constant(i));
                _builderStack.Push(subBuilder);
                visitedArgs.Add(Visit(node.Arguments[i]));
                _builderStack.Pop();
            }
            else
            {
                visitedArgs.Add(Visit(node.Arguments[i]));
            }
        }

        return MakeNew(node, visitedArgs.AsReadOnly());
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        _builderStack.Push(null);
        Expression? expr = base.VisitBinary(node);
        _builderStack.Pop();
        return expr;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        Expression source = Visit(node.Object);
        var isRangeNeeded = IsRangeNeeded(node);
        
        if (isRangeNeeded) _inputTypes.Push([InputType.Ranged]);
        ReadOnlyCollection<Expression>? args = Visit(node.Arguments);
        if (isRangeNeeded) _inputTypes.Pop();

        return MakeMethodCall(node, source, args);
    }

    private Expression GenerateListSelect(
        Expression source,
        Type originalReturnType,
        LambdaExpression predicate)
    {
        return ExecuteListSelect(
            source,
            AccessValueBuilderAndCallPredicate(predicate),
            ArrowUtilities.GetTypeInfo(originalReturnType));
    }

    private static LambdaExpression AccessValueBuilderAndCallPredicate(
        LambdaExpression arrowPredicate)
    {
        Type inputType = arrowPredicate.Parameters[1].Type;
        Type resultType = GetResultBuilder(arrowPredicate).Type;

        ParameterExpression ctxP = Expression.Parameter(typeof(ExecutionContext), "ctx");
        ParameterExpression elemP = Expression.Parameter(inputType, "elem");
        ParameterExpression builderP = Expression.Parameter(typeof(ListArrayBuilder), "builder");

        List<Expression> invokeArgs =
        [
            ctxP,
            elemP,
            AccessValueBuilder(resultType, builderP)
        ];
        List<ParameterExpression> outerParams = [ctxP, elemP, builderP];
        
        return Expression.Lambda(
            Expression.Invoke(arrowPredicate, invokeArgs),
            outerParams);
    }

    private static ParameterExpression GetResultBuilder(LambdaExpression arrowPredicate)
    {
        return arrowPredicate.Parameters[^1];
    }

    private static Expression AccessValueBuilder(Type valueBuilderType, ParameterExpression listBuilder)
    {
        return typeof(Unsafe).GetMethod(nameof(Unsafe.As), [typeof(object)])!.CallStatic(
            [valueBuilderType],
            [listBuilder.Property(nameof(ListArrayBuilder.ValueBuilder))]);
    }

    private Expression GenerateListAll(
        Expression source,
        Type originalReturnType,
        LambdaExpression predicate)
    {
        _builderStack.Push(null);
        Expression map = GenerateListSelect(source, originalReturnType, predicate);
        _builderStack.Pop();

        LambdaExpression all = (ExecutionContext ctx, RangedInput<BooleanArray> input, BooleanArrayBuilder builder) =>
            HideBuilderReturnGeneric(builder.Append(ArrowCompute.All(input.Array, input.Range)));

        return ExecuteElementWiseListOp(
            map,
            all,
            ArrowUtilities.GetTypeInfo(typeof(bool)));
    }

    private Expression GenerateListAny(
        Expression source,
        Type originalReturnType,
        LambdaExpression predicate)
    {
        _builderStack.Push(null);
        Expression map = GenerateListSelect(source, originalReturnType, predicate);
        _builderStack.Pop();

        LambdaExpression any = (ExecutionContext ctx, RangedInput<BooleanArray> input, BooleanArrayBuilder builder) =>
            HideBuilderReturnGeneric(builder.Append(ArrowCompute.Any(input.Array, input.Range)));

        return ExecuteElementWiseListOp(
            map,
            any,
            ArrowUtilities.GetTypeInfo(typeof(bool)));
    }

    // TODO null should be handled
    public static LambdaExpression ListContainsPredicate<T>(Expression value)
    {
        var tParam = Expression.Parameter(typeof(T), "input");
        var pred = Expression.Lambda<Func<T, bool>>(Expression.MakeBinary(ExpressionType.Equal, tParam, value), tParam);
        return (IQueryable<T> i) => i.Any(pred);
    }

    private Expression GenerateListContains(Expression source, Expression valueExpr)
    {
        LambdaExpression predicate =
            (LambdaExpression)typeof(BufferTransformVisitor).GetMethod(nameof(ListContainsPredicate))!
                .MakeGenericMethod(valueExpr.Type).Invoke(null, [valueExpr])!;

        _inputTypes.Push([InputType.Ranged]);
        LambdaExpression arrowPredicate = (LambdaExpression)Visit(predicate);
        _inputTypes.Pop();

        throw new NotImplementedException();
    }

    private Expression ExecuteListSelect(
        Expression source,
        LambdaExpression op,
        ArrowUtilities.ArrowTypeInfo resultElementType)
    {
        ArrowUtilities.ArrowTypeInfo resultInfo = ArrowUtilities.ListOf(resultElementType.ArrowType);
        Type arrowInputArrayType = GetInputArrayType(GetInput(op, 0));
        MethodInfo method = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteListSelect))!
            .MakeGenericMethod(source.Type, arrowInputArrayType);

        return ExecuteInline(method, [source], op, resultInfo);
    }

    private Expression ExecuteElementWiseListOp(
        Expression source,
        LambdaExpression op,
        ArrowUtilities.ArrowTypeInfo resultInfo)
    {
        var hasRange = IsRangedInput(GetInput(op, 0));
        if (!hasRange) throw new InvalidOperationException("must have range to execute element-wise");

        ParameterExpression inputType = GetInput(op, 0);
        Type arrowResultBuilderType = GetResultBuilder(op).Type;
        MethodInfo method =
            typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteElementWiseListOpWithRange))!
                .MakeGenericMethod(
                    source.Type,
                    GetInputArrayType(inputType),
                    resultInfo.ArrayType,
                    arrowResultBuilderType);

        return ExecuteInline(method, [source], op, resultInfo);
    }

    protected override Expression MakeBinary(
        BinaryExpression node,
        Expression left,
        LambdaExpression conversion,
        Expression right)
    {
        Type leftElementType = ExpressionPrimitiveElementType(left);
        Type rightElementType = ExpressionPrimitiveElementType(right);

        // if not equal, convert both to bitmap
        if ((leftElementType == typeof(bool) && rightElementType == typeof(bool)) ||
            leftElementType != rightElementType)
        {
            if (leftElementType != typeof(bool))
            {
                left = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapFromMask))!.CallStatic(
                    [leftElementType],
                    [_ctxParam, AccessSpan(left)]);
            }

            if (rightElementType != typeof(bool))
            {
                right = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapFromMask))!.CallStatic(
                    [rightElementType],
                    [_ctxParam, AccessSpan(right)]);
            }

            return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapOps))!.CallStatic(
                [],
                [
                    _ctxParam,
                    AccessSpan(left),
                    AccessSpan(right),
                    node.NodeType.Quoted
                ]);
        }

        return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.Zip))!.CallStatic(
            [leftElementType],
            [
                _ctxParam,
                AccessSpan(left),
                AccessSpan(right),
                node.NodeType.Quoted
            ]);
    }

    protected override Expression MakeConditional(
        ConditionalExpression node,
        Expression test,
        Expression ifTrue,
        Expression ifFalse)
    {
        return node;
    }

    protected override Expression MakeConstant(ConstantExpression node)
    {
        return ArrowUtilities.MakeConstantArray(node.Type, node.Value, MaxBatchSize);
    }

    protected override Expression MakeDefault(DefaultExpression node)
    {
        return node;
    }

    protected override ElementInit MakeElementInit(ElementInit node, ReadOnlyCollection<Expression> arguments)
    {
        return node;
    }

    protected override Expression MakeInvocation(
        InvocationExpression node,
        Expression expression,
        ReadOnlyCollection<Expression> arguments)
    {
        return node;
    }

    protected override LambdaExpression MakeLambda<T>(
        Expression<T> node,
        Expression body,
        ReadOnlyCollection<Expression> parameters)
    {
        List<ParameterExpression> lambdaParams = [_ctxParam, ..parameters.Cast<ParameterExpression>()];
        
        var hasBuilderParam = TryGetParameter(_builderStack, out ParameterExpression? builderParam);
        if (!hasBuilderParam)
            throw new InvalidOperationException("Lambda cannot be constructed without a builder parameter");

        lambdaParams.Add(builderParam!);

        Expression append = IsSpan(body) ? AppendSpanToBuilder(body, builderParam!) : body;
        LambdaExpression lambda = Expression.Lambda(
            HideBuilderReturn(append),
            $"QueryMethod_{node}",
            false,
            lambdaParams);
        return lambda;
    }

    [DynamicDependency(DynamicallyAccessedMemberTypes.AllMethods, typeof(IArrowArrayBuilder<,,>))]
    private static Expression AppendSpanToBuilder(Expression span, Expression builder)
    {
        if (IsBooleanBuilder(builder))
        {
            MethodInfo method = typeof(BooleanArrayBuilder).GetMethod(nameof(BooleanArrayBuilder.AppendMask))!
                .MakeGenericMethod(ExpressionPrimitiveElementType(span));
            return builder.Call(method, span);
        }
        else
        {
            return builder.Call(nameof(IArrowArrayBuilder<,,>.Append), span);
        }
    }

    [DynamicDependency(DynamicallyAccessedMemberTypes.AllMethods, typeof(IdentityInput<>))]
    private static Expression TransformInput(Expression input, Func<Expression, Expression> transformer)
    {
        Expression transformed = transformer(AccessArray(input));
        return Expression.Call(
            input,
            // Hacky but works for other subtypes as well
            nameof(IdentityInput<>.Apply),
            [transformed.Type],
            transformed);
    }

    protected override Expression MakeListInit(
        ListInitExpression node,
        NewExpression newExpression,
        ReadOnlyCollection<ElementInit> initializers)
    {
        return node;
    }

    protected override Expression MakeMember(MemberExpression node, Expression input)
    {
        return TransformInput(input, expression => AccessStructField(node, expression));
    }

    protected override MemberAssignment MakeMemberAssignment(MemberAssignment node, Expression expression)
    {
        return node;
    }

    protected override Expression MakeMemberInit(
        MemberInitExpression node,
        NewExpression newExpression,
        ReadOnlyCollection<MemberBinding> bindings)
    {
        return node;
    }

    protected override MemberListBinding MakeMemberListBinding(
        MemberListBinding node,
        ReadOnlyCollection<ElementInit> initializers)
    {
        return node;
    }

    protected override MemberMemberBinding MakeMemberMemberBinding(
        MemberMemberBinding node,
        ReadOnlyCollection<MemberBinding> bindings)
    {
        return node;
    }

    protected override Expression MakeMethodCall(
        MethodCallExpression node,
        Expression @object,
        ReadOnlyCollection<Expression> arguments)
    {
        if (_builderStack.Count == 0)
        {
            throw new NotImplementedException("TODO top-level query operator");
        }
        else
        {
            // TODO replace this with 'function registry'
            List<string> supportedTypes = ["Enumerable", "Queryable", "List"];
            Type? declaringType = node.Method.DeclaringType;
            if (declaringType == null && !supportedTypes.Any(name => declaringType!.Name.Contains(name)))
                throw new NotImplementedException("this method can't be mapped yet");

            var methodName = node.Method.Name;

            Expression enumerable = arguments[0];
            
            if (methodName == "Contains")
                return GenerateListContains(enumerable, node.Arguments[0]);
            
            LambdaExpression predicate = (LambdaExpression)arguments[1];
            LambdaExpression originalPredicate = (LambdaExpression)node.Arguments[1];
            
            return methodName switch
            {
                "Select" => GenerateListSelect(enumerable, originalPredicate.ReturnType, predicate),
                "All" => GenerateListAll(enumerable, originalPredicate.ReturnType, predicate),
                "Any" => GenerateListAny(enumerable, originalPredicate.ReturnType, predicate),
                _ => throw new NotImplementedException("this method can't be mapped yet")
            };
        }
    }

    protected override Expression MakeNew(NewExpression node, ReadOnlyCollection<Expression> arguments)
    {
        Type type = node.Type;

        if (type.ImplementsInterface(typeof(IReadOnlyDictionary<,>)))
        {
            throw new NotImplementedException("not yet");
        }

        if (type.ImplementsInterface(typeof(IEnumerable<>)))
        {
            Type? elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();
            if (elementType != null)
            {
                throw new NotImplementedException("not yet");
            }
        }

        if (type.IsClass || type is { IsValueType: true, IsPrimitive: false })
        {
            return BuildStructWithBuilder(arguments);
        }

        throw new NotImplementedException("not yet");
    }

    private Expression BuildStructWithBuilder(
        ReadOnlyCollection<Expression> arguments)
    {
        Expression? sbExpr = _builderStack.Peek();
        List<Expression> ops = new(arguments.Count);

        for (var i = 0; i < arguments.Count; i++)
        {
            Expression arg = arguments[i];

            if (arg.Type == typeof(void) || arg.Type.Name.Contains("Builder"))
            {
                // Case 0: nested struct already builder-populated via VisitNew
                // the arg IS the block that populates the sub-builder — include it
                ops.Add(arg);
            }
            else if (IsIdentityInput(arg))
            {
                // Case 1: alias existing array directly
                ops.Add(
                    Expression.Call(
                        sbExpr,
                        typeof(StructArrayBuilder).GetMethod(nameof(StructArrayBuilder.SetFieldArray))!,
                        Expression.Constant(i),
                        AccessArray(arg)));
            }
            else if (IsSpan(arg))
            {
                // Case 2: append span to field builder
                Type elemType = arg.Type.GetGenericArguments()[0];
                Type builderType = GetConcreteBuilderType(GetBufferType(elemType));
                MethodCallExpression builder = Expression.Call(
                    sbExpr,
                    typeof(StructArrayBuilder).GetMethod(nameof(StructArrayBuilder.GetFieldBuilder))!.MakeGenericMethod(
                        builderType),
                    Expression.Constant(i));
                ops.Add(AppendSpanToBuilder(arg, builder));
            }
            else
            {
                throw new NotImplementedException($"Unsupported field type in builder: {arg.Type.Name}");
            }
        }

        return Expression.Block(ops);
    }

    protected override Expression MakeNewArray(NewArrayExpression node, ReadOnlyCollection<Expression> expressions)
    {
        return node;
    }

    protected override Expression MakeParameter(ParameterExpression node)
    {
        if (!_bindings.Peek().TryGetValue(node.Name!, out ParameterExpression? param))
        {
            throw new UnreachableException("we should have seen this parameter");
        }

        return param;
    }

    protected override Expression MakeTypeBinary(TypeBinaryExpression node, Expression expression)
    {
        return node;
    }

    protected override Expression MakeUnary(UnaryExpression node, Expression operand)
    {
        if (node.NodeType == ExpressionType.Convert)
        {
            // If operand is already an Arrow array, compare element types
            if (IsInput(operand))
            {
                Type elemType = ExpressionPrimitiveElementType(operand);
                Type resultUnderlyingType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
                if (elemType == resultUnderlyingType) return operand;
                MethodInfo convertMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ConvertLogical))!
                    .MakeGenericMethod(elemType, resultUnderlyingType);
                return Expression.Call(
                    null,
                    convertMethod,
                    [_ctxParam, AccessSpan(operand)]);
            }
            else if (IsSpan(operand))
            {
                Type opUnderlying = Nullable.GetUnderlyingType(node.Operand.Type) ?? node.Operand.Type;
                Type resUnderlying = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
                if (opUnderlying == resUnderlying) return operand;
                MethodInfo convertMethod2 = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ConvertLogical))!
                    .MakeGenericMethod(opUnderlying, resUnderlying);
                return Expression.Call(
                    null,
                    convertMethod2,
                    [_ctxParam, AccessSpan(operand)]);
            }
        }

        throw new NotImplementedException("not yet");
    }

    private Expression AccessStructField(
        MemberExpression expr,
        Expression buffer)
    {
        if (buffer.Type == typeof(StructArray))
        {
            return typeof(ArrowUtilities).GetMethod(nameof(ArrowUtilities.AccessStructField))!.CallStatic(
                [GetBufferType(expr.Type)],
                [buffer, _memberIndex[expr.Member].Quoted]);
        }

        throw new InvalidOperationException("only struct can be accessed");
    }

    private static Expression MakeArray(Expression spanOrArray)
    {
        if (spanOrArray.Type.ImplementsInterface(typeof(IArrowArray))) return spanOrArray;

        // TODO bitmap is problematic
        if (spanOrArray.Type.IsGenericType && spanOrArray.Type.GetGenericArguments()[0] == typeof(byte))
        {
            return typeof(ArrowUtilities).GetMethod(nameof(ArrowUtilities.BooleanArrayFromBitmap))!.CallStatic(
                [],
                [spanOrArray, spanOrArray.Property(nameof(ReadOnlySpan<>.Length))]);
        }

        return typeof(ArrowUtilities).GetMethod(nameof(ArrowUtilities.ArrayFromSpan))!.CallStatic(
            [ExpressionPrimitiveElementType(spanOrArray)],
            [spanOrArray]);
    }

    private Type MakeInputTypeForArray(Type type, int index)
    {
        Debug.Assert(type.ImplementsInterface(typeof(IArrowArray)));

        InputType inputType = _inputTypes.Peek()![index];
        Type baseType = inputType switch
        {
            InputType.Identity => typeof(IdentityInput<>),
            InputType.Ranged => typeof(RangedInput<>),
            InputType.Masked => typeof(MaskedInput<>),
            InputType.Indexed => typeof(IndexedInput<>),
            _ => throw new ArgumentOutOfRangeException()
        };

        return baseType.MakeGenericType(type);
    }

    private Type GetBufferType(Type type)
    {
        Type underlying = Nullable.GetUnderlyingType(type) ?? type;
        if (underlying == typeof(int)) return typeof(PrimitiveArray<int>);
        if (underlying == typeof(double)) return typeof(PrimitiveArray<double>);
        if (underlying == typeof(bool)) return typeof(BooleanArray);

        if (type.ImplementsInterface(typeof(IReadOnlyDictionary<,>)))
        {
            throw new NotImplementedException("not yet");
        }

        if (type.ImplementsInterface(typeof(IEnumerable<>))
            || (type is { IsInterface: true, IsGenericType: true }
                && type.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
        {
            Type? elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();
            if (elementType != null)
            {
                return typeof(ListArray);
            }
        }

        if (type.IsClass || type is { IsValueType: true, IsPrimitive: false })
        {
            BindMemberIndexes(type);
            return typeof(StructArray);
        }

        // TODO this is hacky
        if (type == typeof(byte)) return typeof(BooleanArray);

        throw new NotImplementedException("not yet");
    }

    private void BindMemberIndexes(Type type)
    {
        List<MemberInfo> members = type.GetMembers(BindingFlags.Instance | BindingFlags.Public)
            .Where(info => info is PropertyInfo or FieldInfo).ToList();
        foreach ((var idx, MemberInfo member) in members.Index()) _memberIndex[member] = idx;
    }

    private static Type GetConcreteBuilderType(Type bufferType)
    {
        if (bufferType.IsGenericType && bufferType.GetGenericTypeDefinition() == typeof(PrimitiveArray<>))
        {
            Type innerType = bufferType.GetGenericArguments()[0];
            return ArrowUtilities.GetTypeInfo(innerType).BuilderType;
        }

        if (bufferType == typeof(StructArray)) return typeof(StructArrayBuilder);
        if (bufferType == typeof(ListArray)) return typeof(ListArrayBuilder);
        if (bufferType == typeof(BooleanArray)) return typeof(BooleanArrayBuilder);
        throw new NotSupportedException($"No concrete builder for {bufferType.Name}");
    }

    private static Type ExpressionPrimitiveElementType(Expression expression)
    {
        if (IsInput(expression))
        {
            Type arrayType = expression.Type.GetGenericArguments()[0];

            if (arrayType == typeof(BooleanArray))
                return typeof(bool);

            return arrayType.GetGenericArguments()[0];
        }
        
        if (IsSpan(expression))
            // TODO bitmap is problematic
            return expression.Type.GetGenericArguments()[0];

        throw new InvalidOperationException("Expression doesn't have a primitive element type");
    }

    private Expression BuildArray(ParameterExpression builder)
    {
        return MakeIdentityInput(builder.Call(nameof(IArrowArrayBuilder<>.Build), ArrowArenaAllocator()));
    }

    private static ParameterExpression GetInput(LambdaExpression op, int index)
    {
        // +1 to account for context
        ParameterExpression param = op.Parameters[index + 1];
        Debug.Assert(IsInput(param));
        return param;
    }

    private static Type GetInputArrayType(Expression input)
    {
        Debug.Assert(IsInput(input));
        Type arrayType = input.Type.GetGenericArguments()[0];
        return arrayType;
    }

    private static bool IsSpan(Expression expression)
    {
        return expression.Type.Name.Contains("Span");
    }

    private static bool IsInput(Expression expression)
    {
        return expression.Type.ImplementsInterface(typeof(IInput<>));
    }

    private static bool IsIdentityInput(Expression expression)
    {
        return expression.Type.Name.Contains("IdentityInput");
    }

    private static Expression MakeIdentityInput(Expression expression)
    {
        return typeof(IdentityInput<>).MakeGenericType(expression.Type)
            .CallStatic(nameof(IdentityInput<>.New), expression);
    }

    private static bool IsRangedInput(Expression expression)
    {
        return expression.Type.Name.Contains("RangedInput");
    }

    private static bool IsMaskedInput(Expression expression)
    {
        return expression.Type.Name.Contains("MaskedInput");
    }

    private static bool IsBooleanBuilder(Expression expression)
    {
        return expression.Type == typeof(BooleanArrayBuilder);
    }

    private static BlockExpression HideBuilderReturn(Expression expression)
    {
        Debug.Assert(expression.Type.Name.Contains("Builder"));
        return Expression.Block(expression, Expression.Empty());
    }

    private static void HideBuilderReturnGeneric<T>(T value)
    {
    }

    private static Expression AccessSpan(Expression input)
    {
        if (IsSpan(input)) return input;

        if (IsIdentityInput(input))
        {
            return AccessValues(AccessArray(input));
        }
        else if (IsRangedInput(input))
        {
            Expression span = AccessValues(AccessArray(input));
            return Expression.Call(
                input,
                nameof(RangedInput<>.Slice),
                [ExpressionPrimitiveElementType(span)],
                span);
        }
        else if (IsMaskedInput(input))
        {
        }

        throw new NotImplementedException();
    }

    private static Expression AccessValues(Expression array)
    {
        return array.Property(nameof(PrimitiveArray<>.Values));
    }

    private static Expression AccessArray(Expression input)
    {
        Debug.Assert(input.Type.ImplementsInterface(typeof(IInput<>)));
        return input.Property(nameof(IInput<>.Array));
    }

    private static bool IsRangeNeeded(MethodCallExpression node)
    {
        Type? declaringType = node.Method.DeclaringType;
        if (declaringType == null) return false;
        List<string> names = [];
        return names.Contains(node.Method.Name);
    }

    [DynamicDependency(DynamicallyAccessedMemberTypes.AllProperties, typeof(ExecutionContext))]
    private Expression ArrowArenaAllocator()
    {
        return _ctxParam.Property(nameof(ExecutionContext.ArrowAllocator));
    }

    private static bool TryGetParameter<T>(Stack<T?> stack, [NotNullWhen(true)] out ParameterExpression? param) where T : Expression
    {
        if (stack.Count == 0)
        {
            param = null;
            return false;
        }

        T? item = stack.Peek();
        if (item is ParameterExpression param2)
        {
            param = param2;
            return true;
        }

        param = null;
        return false;
    }

    private Expression ExecuteWithTempBuilder(
        MethodInfo method,
        List<Expression> inputs,
        LambdaExpression op,
        ArrowUtilities.ArrowTypeInfo resultInfo)
    {
        ParameterExpression builder = Expression.Variable(resultInfo.BuilderType, "tmpBuilder");
        MethodCallExpression init = MakeBuilderFor(resultInfo);
        return Expression.Block(
            [builder],
            Expression.Assign(builder, init),
            Expression.Call(null, method, [_ctxParam, ..inputs, op, builder]),
            BuildArray(builder));
    }
    
    private MethodCallExpression MakeBuilderFor(ArrowUtilities.ArrowTypeInfo resultInfo)
    {
        return Expression.Call(
            null,
            typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.MakeBuilderForGeneric))!
                .MakeGenericMethod(resultInfo.BuilderType),
            resultInfo.ArrowType.Quoted,
            ArrowArenaAllocator());
    }

    private Expression ExecuteInline(
        MethodInfo method,
        List<Expression> inputs,
        LambdaExpression op,
        ArrowUtilities.ArrowTypeInfo resultInfo)
    {
        Expression? outer = _builderStack.Peek();
        if (outer is not null)
            return Expression.Call(null, method, [_ctxParam, ..inputs, op, outer]);
        else
            return ExecuteWithTempBuilder(method, inputs, op, resultInfo);
    }
}