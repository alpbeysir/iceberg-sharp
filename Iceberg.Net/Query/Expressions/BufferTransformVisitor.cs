using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq.CompilerServices;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using DotNext.Linq.Expressions;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.FastArrow;
using Varena;

namespace Iceberg.Net.Query.Expressions;

public sealed class ExecutionContext
{
    public required VirtualBuffer Arena { get; init; }
    public required UnsafeArenaMemoryAllocator ArrowAllocator { get; init; }
}

public record InputInfo(Expression Array, Expression? Range)
{
    public bool HasRange()
    {
        return Range is not null;
    }
}

public class BufferTransformVisitor : ExpressionVisitorNarrow<Expression, LambdaExpression, Expression,
    NewExpression, ElementInit, MemberBinding, MemberAssignment, MemberListBinding, MemberMemberBinding>
{
    private readonly Stack<Dictionary<string, ParameterExpression>> _bindings = [];
    private readonly ParameterExpression _ctxParam = Expression.Parameter(typeof(ExecutionContext), "ctx");
    private readonly Dictionary<MemberInfo, int> _memberIndex = [];
    private readonly Stack<Expression?> _builderStack = new();
    private readonly Stack<ParameterExpression?> _rangeStack = new();
    private const int MaxBatchSize = 65536;

    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        Type returnType = Nullable.GetUnderlyingType(node.ReturnType) ?? node.ReturnType;
        ParameterExpression builderParam = Expression.Parameter(
            GetConcreteBuilderType(GetBufferType(returnType)),
            "builder");

        _builderStack.Push(builderParam);
        _bindings.Push([]);

        Dictionary<string, ParameterExpression> curBindings = _bindings.Peek();
        foreach (ParameterExpression expression in node.Parameters)
            curBindings.Add(
                expression.Name!,
                Expression.Parameter(GetBufferType(expression.Type), expression.Name));

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
        _builderStack.Push(Expression.Empty());
        Expression? expr = base.VisitBinary(node);
        _builderStack.Pop();
        return expr;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        Expression? o = Visit(node.Object);
        var isRangeNeeded = IsRangeNeeded(node);
        if (isRangeNeeded) _rangeStack.Push(Expression.Parameter(typeof(Range), "range"));
        ReadOnlyCollection<Expression>? a = Visit(node.Arguments);
        if (isRangeNeeded) _rangeStack.Pop();
        return MakeMethodCall(node, o, a);
    }

    private Expression GenerateListSelect(
        Expression source,
        Type originalReturnType,
        LambdaExpression predicate)
    {
        return ExecuteOneToOneListOp(
            source,
            AccessValueBuilderAndCallPredicate(predicate),
            ArrowUtilities.GetTypeInfo(originalReturnType));
    }

    private static LambdaExpression AccessValueBuilderAndCallPredicate(
        LambdaExpression arrowPredicate)
    {
        var hasRange = HasRange(arrowPredicate);
        Type arrowInputArrayType = arrowPredicate.Parameters[1].Type;
        Type arrowElementBuilderType = GetResultBuilder(arrowPredicate).Type;

        ParameterExpression ctxP = Expression.Parameter(typeof(ExecutionContext), "ctx");
        ParameterExpression elemP = Expression.Parameter(arrowInputArrayType, "elem");
        ParameterExpression builderP = Expression.Parameter(typeof(ListArrayBuilder), "builder");

        List<Expression> invokeArgs = new()
        {
            ctxP,
            elemP,
            AccessValueBuilder(arrowElementBuilderType, builderP)
        };
        List<ParameterExpression> outerParams = [ctxP, elemP, builderP];

        if (hasRange)
        {
            ParameterExpression rangeP = Expression.Parameter(typeof(Range), "range");
            invokeArgs.Insert(2, rangeP);
            outerParams.Insert(2, rangeP);
        }

        return Expression.Lambda(
            Expression.Invoke(arrowPredicate, invokeArgs),
            outerParams);
    }

    private static ParameterExpression GetResultBuilder(LambdaExpression arrowPredicate)
    {
        return arrowPredicate.Parameters[^1];
    }

    private static bool HasRange(LambdaExpression op)
    {
        return op.Parameters.Count == 4 && op.Parameters[2].Type == typeof(Range);
    }

    private static Expression AccessValueBuilder(Type valueBuilderType, ParameterExpression listBuilder)
    {
#if DEBUG
        return Expression.Convert(
            Expression.Property(listBuilder, nameof(ListArrayBuilder.ValueBuilder)),
            valueBuilderType);
#else
        return typeof(Unsafe).GetMethod(nameof(Unsafe.As), [typeof(object)])!.CallStatic(
            [valueBuilderType],
            [listBuilder.Property(nameof(ListArrayBuilder.ValueBuilder))]);
#endif
    }

    private Expression GenerateListAll(
        Expression source,
        Type originalReturnType,
        LambdaExpression predicate)
    {
        _builderStack.Push(Expression.Empty());
        _rangeStack.Push(null);
        Expression map = GenerateListSelect(source, originalReturnType, predicate);
        _builderStack.Pop();
        _rangeStack.Pop();

        LambdaExpression all = (ExecutionContext ctx, BooleanArray b, Range r, BooleanArrayBuilder builder) =>
            HideBuilderReturnGeneric(builder.Append(ArrowCompute.All(b, r)));

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
        _rangeStack.Push(null);
        Expression map = GenerateListSelect(source, originalReturnType, predicate);
        _rangeStack.Pop();
        _builderStack.Pop();

        LambdaExpression all = (ExecutionContext ctx, BooleanArray b, Range r, BooleanArrayBuilder builder) =>
            HideBuilderReturnGeneric(builder.Append(true));

        return ExecuteElementWiseListOp(
            map,
            all,
            ArrowUtilities.GetTypeInfo(typeof(bool)));
    }

    private Expression GenerateListContains(Expression source, Expression valueExpr)
    {
        throw new NotImplementedException();
    }

    private Expression ExecuteOneToOneListOp(
        Expression source,
        LambdaExpression op,
        ArrowUtilities.ArrowTypeInfo resultElementType)
    {
        ArrowUtilities.ArrowTypeInfo resultListType = ArrowUtilities.ListOf(resultElementType.ArrowType);
        Type arrowInputArrayType = op.Parameters[1].Type;
        MethodInfo method = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteOneToOneListOp))!
            .MakeGenericMethod(arrowInputArrayType);

        Expression? outer = _builderStack.Peek();
        if (outer is not null)
        {
            return Expression.Call(null, method, _ctxParam, source, outer, op);
        }
        else
        {
            ParameterExpression builder = Expression.Variable(resultListType.BuilderType, "tmpBuilder");
            MethodCallExpression init = Expression.Call(
                null,
                typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.MakeBuilderForGeneric))!
                    .MakeGenericMethod(resultListType.BuilderType),
                resultListType.ArrowType.Quoted,
                ArrowArenaAllocator());
            return Expression.Block(
                [builder],
                Expression.Assign(builder, init),
                Expression.Call(null, method, _ctxParam, source, builder, op),
                builder.Call(nameof(IArrowArrayBuilder<>.Build), ArrowArenaAllocator()));
        }
    }
    
    private Expression ExecuteElementWiseListOp(
        Expression source,
        LambdaExpression op,
        ArrowUtilities.ArrowTypeInfo resultInfo)
    {
        var hasRange = HasRange(op);
        if (!hasRange) throw new InvalidOperationException("must have range to execute element-wise");

        Type arrowInputArrayType = GetInput(op).Type;
        Type arrowResultBuilderType = GetResultBuilder(op).Type;
        MethodInfo method =
            typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteElementWiseListOpWithRange))!
                .MakeGenericMethod(arrowInputArrayType, arrowResultBuilderType);

        Expression? outer = _builderStack.Peek();
        if (outer is not null)
        {
            return Expression.Call(null, method, _ctxParam, source, outer, op);
        }
        else
        {
            ParameterExpression builder = Expression.Variable(resultInfo.BuilderType, "tmpBuilder");
            MethodCallExpression init = Expression.Call(
                null,
                typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.MakeBuilderForGeneric))!
                    .MakeGenericMethod(resultInfo.BuilderType),
                resultInfo.ArrowType.Quoted,
                ArrowArenaAllocator());
            MethodCallExpression reserveInBuilder = builder.Call(
                nameof(IArrowArrayBuilder<,>.Reserve),
                source.Property(nameof(IArrowArray.Length)));
            return Expression.Block(
                [builder],
                Expression.Assign(builder, init),
                reserveInBuilder,
                Expression.Call(null, method, _ctxParam, source, builder, op),
                builder.Call(nameof(IArrowArrayBuilder<>.Build), ArrowArenaAllocator()));
        }
    }

    private static ParameterExpression GetInput(LambdaExpression op)
    {
        return op.Parameters[1];
    }

    protected override Expression MakeBinary(
        BinaryExpression node,
        Expression left,
        LambdaExpression conversion,
        Expression right)
    {
        Type leftElementType = PrimitiveBufferElementType(left);
        Type rightElementType = PrimitiveBufferElementType(right);

        // if not equal, convert both to bitmap
        if ((leftElementType == typeof(bool) && rightElementType == typeof(bool)) ||
            leftElementType != rightElementType)
        {
            if (leftElementType != typeof(bool))
            {
                left = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapFromMask))!.CallStatic(
                    [leftElementType],
                    [_ctxParam, MaybeApplyRange(AccessPrimitiveValues(left))]);
            }

            if (rightElementType != typeof(bool))
            {
                right = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapFromMask))!.CallStatic(
                    [rightElementType],
                    [_ctxParam, MaybeApplyRange(AccessPrimitiveValues(right))]);
            }

            return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapOps))!.CallStatic(
                [],
                [
                    _ctxParam,
                    MaybeApplyRange(AccessPrimitiveValues(left)),
                    MaybeApplyRange(AccessPrimitiveValues(right)),
                    node.NodeType.Quoted
                ]);
        }

        return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.Zip))!.CallStatic(
            [leftElementType],
            [
                _ctxParam,
                MaybeApplyRange(AccessPrimitiveValues(left)),
                MaybeApplyRange(AccessPrimitiveValues(right)),
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

        if (TryGetParameter(_rangeStack, out ParameterExpression? rangeParam))
            lambdaParams.Add(rangeParam);

        var hasBuilderParam = TryGetParameter(_builderStack, out ParameterExpression? builderParam);
        if (!hasBuilderParam)
            throw new InvalidOperationException("Lambda cannot be constructed without a builder parameter");

        lambdaParams.Add(builderParam!);

        Expression append = IsSpan(body) ? AppendSpanToBuilder(body, builderParam!) : body;
        LambdaExpression lambda = Expression.Lambda(HideBuilderReturn(append), lambdaParams);
        return lambda;
    }

    private static Expression AppendSpanToBuilder(Expression span, Expression builder)
    {
        if (IsBooleanBuilder(builder))
        {
            MethodInfo method = typeof(BooleanArrayBuilder).GetMethod(nameof(BooleanArrayBuilder.AppendMask))!
                .MakeGenericMethod(PrimitiveBufferElementType(span));
            return builder.Call(method, span);
        }
        else
        {
            return builder.Call(nameof(IArrowArrayBuilder<,,>.Append), span);
        }
    }

    protected override Expression MakeListInit(
        ListInitExpression node,
        NewExpression newExpression,
        ReadOnlyCollection<ElementInit> initializers)
    {
        return node;
    }

    protected override Expression MakeMember(MemberExpression node, Expression buffer)
    {
        return AccessCompositeBuffer(node, buffer);
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
            Type? declaringType = node.Method.DeclaringType;
            if (declaringType == null ||
                (!declaringType.Name.Contains("Enumerable") && !declaringType.Name.Contains("Queryable")))
                throw new NotImplementedException("this method can't be mapped yet");

            var methodName = node.Method.Name;

            Expression enumerable = arguments[0];
            if (enumerable.Type != typeof(ListArray))
                throw new NotImplementedException("this method can't be mapped yet");

            if (methodName == "Contains")
                return GenerateListContains(enumerable, node.Arguments[1]);

            LambdaExpression predicate = (LambdaExpression)arguments[1];
            LambdaExpression originalPredicate = (LambdaExpression)node.Arguments[1];

            return methodName switch
            {
                "Select" => GenerateListSelect(enumerable, originalPredicate.ReturnType, predicate),
                "All" => GenerateListAll(enumerable, originalPredicate.ReturnType, predicate),
                "Any" => GenerateListAny(enumerable, originalPredicate.ReturnType, predicate),
                _ => node
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
            else if (arg.Type.ImplementsInterface(typeof(IArrowArray)))
            {
                // Case 1: alias existing array directly (zero-copy)
                ops.Add(
                    Expression.Call(
                        sbExpr,
                        typeof(StructArrayBuilder).GetMethod(nameof(StructArrayBuilder.SetFieldArray))!,
                        Expression.Constant(i),
                        arg));
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
            if (operand.Type.ImplementsInterface(typeof(IArrowArray)))
            {
                Type elemType = PrimitiveBufferElementType(operand);
                Type resultUnderlyingType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
                if (elemType == resultUnderlyingType) return operand;
                MethodInfo convertMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ConvertLogical))!
                    .MakeGenericMethod(elemType, resultUnderlyingType);
                return Expression.Call(
                    null,
                    convertMethod,
                    [_ctxParam, MaybeApplyRange(AccessPrimitiveValues(operand))]);
            }

            Type opUnderlying = Nullable.GetUnderlyingType(node.Operand.Type) ?? node.Operand.Type;
            Type resUnderlying = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
            if (opUnderlying == resUnderlying) return operand;
            MethodInfo convertMethod2 = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ConvertLogical))!
                .MakeGenericMethod(opUnderlying, resUnderlying);
            return Expression.Call(
                null,
                convertMethod2,
                [_ctxParam, MaybeApplyRange(AccessPrimitiveValues(operand))]);
        }

        return node;
    }

    private Expression AccessCompositeBuffer(
        MemberExpression expr,
        Expression buffer)
    {
        if (buffer.Type == typeof(StructArray))
        {
            MethodInfo method =
                typeof(ArrowUtilities).GetMethod(nameof(ArrowUtilities.AccessField))!.MakeGenericMethod(
                    GetBufferType(expr.Type));
            return Expression.Call(null, method, buffer, _memberIndex[expr.Member].Quoted);
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
            [PrimitiveBufferElementType(spanOrArray)],
            [spanOrArray]);
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
            List<MemberInfo> members = type.GetMembers(BindingFlags.Instance | BindingFlags.Public)
                .Where(info => info is PropertyInfo or FieldInfo).ToList();
            foreach ((var idx, MemberInfo member) in members.Index()) _memberIndex.Add(member, idx);
            return typeof(StructArray);
        }

        // TODO this is hacky
        if (type == typeof(byte)) return typeof(BooleanArray);

        throw new NotImplementedException("not yet");
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

    private static Type PrimitiveBufferElementType(Expression expression)
    {
        if (expression.Type.ImplementsInterface(typeof(IArrowArray)))
        {
            if (expression.Type == typeof(BooleanArray))
                return typeof(bool);

            return expression.Type.GetGenericArguments()[0];
        }

        if (IsSpan(expression))
            // TODO bitmap is problematic
            return expression.Type.GetGenericArguments()[0];

        throw new InvalidOperationException("invalid");
    }

    private static bool IsSpan(Expression expression)
    {
        return expression.Type.Name.Contains("Span");
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

    private static Expression AccessPrimitiveValues(Expression buffer)
    {
        if (IsSpan(buffer)) return buffer;
        return buffer.Property(nameof(PrimitiveArray<>.Values));
    }

    private static bool IsRangeNeeded(MethodCallExpression node)
    {
        Type? declaringType = node.Method.DeclaringType;
        if (declaringType == null) return false;
        List<string> names = [];
        return names.Contains(node.Method.Name);
    }

    private Expression MaybeApplyRange(Expression spanExpr)
    {
        if (TryGetParameter(_rangeStack, out ParameterExpression? rangeParam))
        {
            Type elemType = spanExpr.Type.GetGenericArguments()[0];
            return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.SliceSpan))!
                .CallStatic([elemType], [spanExpr, rangeParam]);
        }

        return spanExpr;
    }

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
}