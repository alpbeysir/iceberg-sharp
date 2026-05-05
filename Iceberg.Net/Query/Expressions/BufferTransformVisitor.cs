using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq.CompilerServices;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using DotNext.Linq.Expressions;
using Iceberg.Net.Misc;
using Varena;

namespace Iceberg.Net.Query.Expressions;

public sealed class ExecutionContext
{
    public required VirtualBuffer Arena { get; init; }
}

public class BufferTransformVisitor : ExpressionVisitorNarrow<Expression, LambdaExpression, Expression,
    NewExpression, ElementInit, MemberBinding, MemberAssignment, MemberListBinding, MemberMemberBinding>
{
    private readonly Stack<Dictionary<string, ParameterExpression>> _bindings = [];
    private readonly ParameterExpression _ctxParam = Expression.Parameter(typeof(ExecutionContext), "ctx");
    private readonly Dictionary<MemberInfo, int> _memberIndex = [];
    private readonly Stack<Expression> _builderStack = new();
    private const int MaxBatchSize = 65536;

    private Expression GenerateListSelect(Expression source, LambdaExpression transformedInner)
    {
        var elementArrayType = transformedInner.Parameters[1].Type;

        var ctxP = Expression.Parameter(typeof(ExecutionContext), "ctx");
        var elemP = Expression.Parameter(elementArrayType, "elem");
        var builderP = Expression.Parameter(
            typeof(IArrowArrayBuilder<ListArray, ListArray.Builder>),
            "builder");

        var lbVar = Expression.Variable(typeof(ListArray.Builder));
        var assignLb = Expression.Assign(
            lbVar,
            Expression.Convert(builderP, typeof(ListArray.Builder)));
        var valueBuilder = Expression.Property(lbVar, "ValueBuilder");
        var typedVb = Expression.Convert(
            valueBuilder,
            transformedInner.Parameters[2].Type);
        var invokeInner = Expression.Invoke(transformedInner, ctxP, elemP, typedVb);
        var body = Expression.Block([lbVar], assignLb, invokeInner);

        var valueElementType = elementArrayType.IsGenericType
            ? elementArrayType.GetGenericArguments()[0]
            : typeof(int);
        var valueType = ArrowUtilities.GetTypeInfo(valueElementType).ArrowType;

        var selectMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteListOp))!
            .MakeGenericMethod(elementArrayType, typeof(ListArray), typeof(ListArray.Builder));

        var opLambda = Expression.Lambda(body, ctxP, elemP, builderP);

        return CallListOpWithBuilder(
            selectMethod,
            source,
            opLambda,
            typeof(ListArray.Builder),
            Expression.Constant(valueType));
    }
    
    private Expression GenerateListAll(Expression source, LambdaExpression transformedInner)
    {
        // transformedInner: (ctx, elementArray, builder) => ReadOnlySpan<T> (mask)
        var elementArrayType = transformedInner.Parameters[1].Type;
        var resultElementType = transformedInner.ReturnType.GetGenericArguments()[0];

        var selectMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteListOp))!
            .MakeGenericMethod(elementArrayType, typeof(BooleanArray), typeof(BooleanArray.Builder));

        var ctxP = Expression.Parameter(typeof(ExecutionContext), "ctx");
        var elemP = Expression.Parameter(elementArrayType, "elem");
        var builderP = Expression.Parameter(
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>),
            "builder");

        // Inner predicate is (ctx, elem[, builder]) => ReadOnlySpan<T> — call with builder if available
        var castBuilder = Expression.Convert(builderP, typeof(BooleanArray.Builder));
        var mask = transformedInner.Parameters.Count > 2
            ? Expression.Invoke(transformedInner, ctxP, elemP, castBuilder)
            : Expression.Invoke(transformedInner, ctxP, elemP);

        var allTrueCall = Expression.Call(
            null,
            typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.AllTrue))!
                .MakeGenericMethod(resultElementType),
            mask);

        var appendCall = Expression.Call(
            castBuilder,
            typeof(BooleanArray.Builder).GetMethod("Append", [typeof(bool)])!,
            allTrueCall);

        var actionType = typeof(Action<,,>).MakeGenericType(
            typeof(ExecutionContext),
            elementArrayType,
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>));
        var opLambda = Expression.Lambda(
            actionType,
            Expression.Block(appendCall, Expression.Empty()),
            ctxP,
            elemP,
            builderP);

        return CallListOpWithBuilder(selectMethod, source, opLambda, typeof(BooleanArray.Builder));
    }

    private Expression GenerateListAny(Expression source, LambdaExpression transformedInner)
    {
        var elementArrayType = transformedInner.Parameters[1].Type;
        var resultElementType = transformedInner.ReturnType.GetGenericArguments()[0];

        var selectMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteListOp))!
            .MakeGenericMethod(elementArrayType, typeof(BooleanArray), typeof(BooleanArray.Builder));

        var ctxP = Expression.Parameter(typeof(ExecutionContext), "ctx");
        var elemP = Expression.Parameter(elementArrayType, "elem");
        var builderP = Expression.Parameter(
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>),
            "builder");

        var castBuilder = Expression.Convert(builderP, typeof(BooleanArray.Builder));
        var mask = transformedInner.Parameters.Count > 2
            ? Expression.Invoke(transformedInner, ctxP, elemP, castBuilder)
            : Expression.Invoke(transformedInner, ctxP, elemP);

        var anyTrueMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.AnyTrue))!
            .MakeGenericMethod(resultElementType);
        var anyTrueCall = Expression.Call(null, anyTrueMethod, mask);

        var appendCall = Expression.Call(
            castBuilder,
            typeof(BooleanArray.Builder).GetMethod("Append", [typeof(bool)])!,
            anyTrueCall);

        var actionType = typeof(Action<,,>).MakeGenericType(
            typeof(ExecutionContext),
            elementArrayType,
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>));
        var opLambda = Expression.Lambda(
            actionType,
            Expression.Block(appendCall, Expression.Empty()),
            ctxP,
            elemP,
            builderP);

        return CallListOpWithBuilder(selectMethod, source, opLambda, typeof(BooleanArray.Builder));
    }

    private Expression GenerateListContains(Expression source, Expression valueExpr)
    {
        if (valueExpr is not ConstantExpression ce)
            return Expression.Empty(); // TODO

        var elementType = Nullable.GetUnderlyingType(valueExpr.Type) ?? valueExpr.Type;
        var elementArrayType = typeof(PrimitiveArray<>).MakeGenericType(elementType);

        var selectMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteListOp))!
            .MakeGenericMethod(elementArrayType, typeof(BooleanArray), typeof(BooleanArray.Builder));

        var ctxP = Expression.Parameter(typeof(ExecutionContext), "ctx");
        var elemP = Expression.Parameter(elementArrayType, "elem");
        var builderP = Expression.Parameter(
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>),
            "builder");

        var containsMethod = typeof(MemoryExtensions).GetMethod(
            "Contains",
            [typeof(ReadOnlySpan<>).MakeGenericType(elementType), elementType])!;
        var valuesProp = Expression.Property(elemP, "Values");
        var containsCall = Expression.Call(
            null,
            containsMethod,
            valuesProp,
            Expression.Constant(ce.Value, elementType));

        var appendCall = Expression.Call(
            Expression.Convert(builderP, typeof(BooleanArray.Builder)),
            typeof(BooleanArray.Builder).GetMethod("Append", [typeof(bool)])!,
            containsCall);

        var actionType = typeof(Action<,,>).MakeGenericType(
            typeof(ExecutionContext),
            elementArrayType,
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>));
        var opLambda = Expression.Lambda(
            actionType,
            Expression.Block(appendCall, Expression.Empty()),
            ctxP,
            elemP,
            builderP);

        return CallListOpWithBuilder(selectMethod, source, opLambda, typeof(BooleanArray.Builder));
    }

    private Expression CallListOpWithBuilder(
        MethodInfo selectMethod,
        Expression source,
        LambdaExpression opLambda,
        Type concreteBuilderType,
        params Expression[] ctorArgs)
    {
        var outer = _builderStack.Peek();
        if (outer != null)
            return Expression.Call(null, selectMethod, _ctxParam, source, outer, opLambda);

        // Intermediate: create temp builder, execute, build, return
        var bVar = Expression.Variable(concreteBuilderType);
        Expression init = ctorArgs.Length > 0
            ? Expression.New(
                concreteBuilderType.GetConstructor(
                    BindingFlags.Public | BindingFlags.Instance,
                    null,
                    ctorArgs.Select(a => a.Type).ToArray(),
                    [])!,
                ctorArgs)
            : Expression.New(concreteBuilderType);
        return Expression.Block(
            [bVar],
            Expression.Assign(bVar, init),
            Expression.Call(null, selectMethod, _ctxParam, source, bVar, opLambda),
            Expression.Call(
                bVar,
                concreteBuilderType.GetMethod(
                    "Build",
                    BindingFlags.Public | BindingFlags.Instance,
                    null,
                    Type.EmptyTypes,
                    [])!));
    }

    protected override Expression MakeBinary(
        BinaryExpression node,
        Expression left,
        LambdaExpression conversion,
        Expression right)
    {
        var leftElementType = PrimitiveBufferElementType(left);
        var rightElementType = PrimitiveBufferElementType(right);

        // if not equal, convert both to bitmap
        if ((leftElementType == typeof(bool) && rightElementType == typeof(bool)) ||
            leftElementType != rightElementType)
        {
            if (leftElementType != typeof(bool))
            {
                left = Expression.Call(
                    null,
                    typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BooleanArrayFromMask))!
                        .MakeGenericMethod(leftElementType),
                    _ctxParam,
                    left);
            }

            if (rightElementType != typeof(bool))
            {
                right = Expression.Call(
                    null,
                    typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BooleanArrayFromMask))!
                        .MakeGenericMethod(rightElementType),
                    _ctxParam,
                    right);
            }

            var bitmapOpsMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapOps))!;
            var bitmapOpsCall = Expression.Call(
                null,
                bitmapOpsMethod,
                _ctxParam,
                AccessValues(left),
                AccessValues(right),
                node.NodeType.Quoted);
            return bitmapOpsCall;
        }

        var zipMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.Zip))!.MakeGenericMethod(leftElementType);
        var zipCall = Expression.Call(
            null,
            zipMethod,
            _ctxParam,
            AccessValues(left),
            AccessValues(right),
            node.NodeType.Quoted);
        return zipCall;
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

    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        var returnType = Nullable.GetUnderlyingType(node.ReturnType) ?? node.ReturnType;
        var builderParam = Expression.Parameter(
            GetConcreteBuilderType(GetBufferType(returnType)),
            "builder");
        
        _builderStack.Push(builderParam);
        _bindings.Push([]);
        foreach (var expression in node.Parameters)
            _bindings.Peek().Add(
                expression.Name!,
                Expression.Parameter(GetBufferType(expression.Type), expression.Name));

        var expr = base.VisitLambda(node);
        
        _builderStack.Pop();
        _bindings.Clear();

        return expr;
    }
    
    private static Type GetConcreteBuilderType(Type bufferType)
    {
        if (bufferType.IsGenericType && bufferType.GetGenericTypeDefinition() == typeof(PrimitiveArray<>))
        {
            var innerType = bufferType.GetGenericArguments()[0];
            return ArrowUtilities.GetTypeInfo(innerType).BuilderType;
        }

        if (bufferType == typeof(StructArray)) return typeof(StructArrayBuilder);
        if (bufferType == typeof(ListArray)) return typeof(ListArray.Builder);
        if (bufferType == typeof(BooleanArray)) return typeof(BooleanArray.Builder);
        throw new NotSupportedException($"No concrete builder for {bufferType.Name}");
    }

    protected override LambdaExpression MakeLambda<T>(
        Expression<T> node,
        Expression body,
        ReadOnlyCollection<Expression> parameters)
    {
        List<ParameterExpression> lambdaParams = [_ctxParam, .._bindings.Peek().Values];

        var builder = _builderStack.Peek();
        if (builder is ParameterExpression builderParam)
            lambdaParams.Add(builderParam);

        if (body.Type.Name.Contains("ReadOnlySpan"))
        {
            body = builder.Call(nameof(IArrowArrayBuilder<,,>.Append), body);
        }

        var lambda = Expression.Lambda(body, lambdaParams);
        return lambda;
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
        var declaringType = node.Method.DeclaringType;
        if (declaringType == null ||
            (!declaringType.Name.Contains("Enumerable") && !declaringType.Name.Contains("Queryable")))
            throw new NotImplementedException("this method can't be mapped yet");

        var methodName = node.Method.Name;

        var source = arguments[0];
        if (source.Type != typeof(ListArray))
            throw new NotImplementedException("this method can't be mapped yet");

        if (methodName == "Contains")
            return GenerateListContains(source, node.Arguments[1]);

        // already visited by base — MakeLambda scoped bindings correctly
        var lambdaArg = arguments[1];
        if (lambdaArg is UnaryExpression { NodeType: ExpressionType.Quote } u)
            lambdaArg = u.Operand;

        if (lambdaArg is not LambdaExpression transformedInner)
            return node;

        return methodName switch
        {
            "Select" => GenerateListSelect(source, transformedInner),
            "All" => GenerateListAll(source, transformedInner),
            "Any" => GenerateListAny(source, transformedInner),
            _ => node
        };
    }

    protected override Expression VisitNew(NewExpression node)
    {
        var type = node.Type;
        if (_builderStack.Count == 0
            || !(type.IsClass || type is { IsValueType: true, IsPrimitive: false }))
            return base.VisitNew(node);
        
        var members = type.GetMembers(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m is PropertyInfo or FieldInfo).ToList();

        var visitedArgs = new List<Expression>(node.Arguments.Count);
        for (var i = 0; i < node.Arguments.Count; i++)
        {
            var memberType = Utils.PropertyOrFieldType(members[i]);
            var underlying = Nullable.GetUnderlyingType(memberType) ?? memberType;

            if (underlying.IsClass || underlying is { IsValueType: true, IsPrimitive: false })
            {
                var parentBuilder = _builderStack.Peek();
                var subBuilder = Expression.Call(
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

    protected override Expression MakeNew(NewExpression node, ReadOnlyCollection<Expression> arguments)
    {
        var type = node.Type;

        if (type.ImplementsInterface(typeof(IReadOnlyDictionary<,>)))
        {
            throw new NotImplementedException("not yet");
        }

        if (type.ImplementsInterface(typeof(IEnumerable<>)))
        {
            var elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();
            if (elementType != null)
            {
                throw new NotImplementedException("not yet");
            }
        }

        if (type.IsClass || type is { IsValueType: true, IsPrimitive: false })
        {
            // var structType = new StructType(
            //     ArrowSchema.FromSchema(CSharpSchema.ToIcebergSchema(node.Type, null, _ => -1)).FieldsList);
            return BuildStructWithBuilder(arguments);
        }

        throw new NotImplementedException("not yet");
    }

    private Expression BuildStructWithBuilder(
        ReadOnlyCollection<Expression> arguments)
    {
        var sbExpr = _builderStack.Peek();
        var ops = new List<Expression>(arguments.Count);

        for (var i = 0; i < arguments.Count; i++)
        {
            var arg = arguments[i];

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
            else if (arg.Type.Name.Contains("ReadOnlySpan"))
            {
                // Case 2: append span to field builder
                var elemType = arg.Type.GetGenericArguments()[0];
                var arrayType = GetBufferType(elemType);
                var fbExpr = Expression.Call(
                    sbExpr,
                    typeof(StructArrayBuilder).GetMethod(nameof(StructArrayBuilder.GetFieldBuilder))!.MakeGenericMethod(
                        GetConcreteBuilderType(arrayType)),
                    Expression.Constant(i));
                ops.Add(fbExpr.Call(nameof(IArrowArrayBuilder<,,>.Append), arg));
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
        if (!_bindings.Peek().TryGetValue(node.Name!, out var param))
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
                var elemType = PrimitiveBufferElementType(operand);
                var resultUnderlyingType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
                if (elemType == resultUnderlyingType) return operand;
                var convertMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ConvertLogical))!
                    .MakeGenericMethod(elemType, resultUnderlyingType);
                return Expression.Call(
                    null,
                    convertMethod,
                    [_ctxParam, AccessValues(operand)]);
            }

            var opUnderlying = Nullable.GetUnderlyingType(node.Operand.Type) ?? node.Operand.Type;
            var resUnderlying = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
            if (opUnderlying == resUnderlying) return operand;
            var convertMethod2 = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ConvertLogical))!
                .MakeGenericMethod(opUnderlying, resUnderlying);
            return Expression.Call(
                null,
                convertMethod2,
                [_ctxParam, AccessValues(operand)]);
        }

        return node;
    }

    private Expression AccessCompositeBuffer(
        MemberExpression expr,
        Expression buffer)
    {
        if (buffer.Type == typeof(StructArray))
        {
            var method =
                typeof(BufferTransformVisitor).GetMethod(nameof(AccessField))!.MakeGenericMethod(
                    GetBufferType(expr.Type));
            return Expression.Call(null, method, buffer, _memberIndex[expr.Member].Quoted);
        }

        throw new InvalidOperationException("only struct can be accessed");
    }

    private static Expression AccessValues(Expression buffer)
    {
        if (buffer.Type.Name.Contains("Span")) return buffer;
        return buffer.Property("Values");
    }

    private static Expression MakeBuffer(Expression spanOrArray)
    {
        if (spanOrArray.Type.ImplementsInterface(typeof(IArrowArray))) return spanOrArray;

        // ReadOnlySpan<byte> is a bitmap — convert to BooleanArray
        if (spanOrArray.Type.IsGenericType
            && spanOrArray.Type.GetGenericArguments()[0] == typeof(byte))
        {
            var method = typeof(ArrowUtilities).GetMethod(nameof(ArrowUtilities.BooleanArrayFromBitmap))!;
            return Expression.Call(
                null,
                method,
                spanOrArray,
                Expression.Property(spanOrArray, "Length"));
        }

        var elementType = PrimitiveBufferElementType(spanOrArray);
        var spanMethod = typeof(ArrowUtilities).GetMethod(nameof(ArrowUtilities.ArrayFromSpan))!
            .MakeGenericMethod(elementType);

        return Expression.Call(null, spanMethod, spanOrArray);
    }

    private Type GetBufferType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
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
            var elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();
            if (elementType != null)
            {
                return typeof(ListArray);
            }
        }

        if (type.IsClass || type is { IsValueType: true, IsPrimitive: false })
        {
            var members = type.GetMembers(BindingFlags.Instance | BindingFlags.Public)
                .Where(info => info is PropertyInfo or FieldInfo).ToList();
            foreach (var (idx, member) in members.Index()) _memberIndex.Add(member, idx);
            return typeof(StructArray);
        }

        throw new NotImplementedException("not yet");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T AccessField<T>(StructArray arr, int index) where T : class, IArrowArray
    {
        // TOOD use Unsafe.As in release mode
        return (T)arr.Fields[index];
    }

    private static Type PrimitiveBufferElementType(Expression expression)
    {
        if (expression.Type.ImplementsInterface(typeof(IArrowArray)))
        {
            if (expression.Type == typeof(BooleanArray))
                return typeof(bool);

            return expression.Type.GetGenericArguments()[0];
        }

        if (expression.Type.Name.Contains("Span"))
            // TODO bitmap is problematic
            return expression.Type.GetGenericArguments()[0];

        throw new InvalidOperationException("invalid");
    }
}