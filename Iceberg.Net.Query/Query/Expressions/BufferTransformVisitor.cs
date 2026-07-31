#if !DEBUG
#endif
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq.CompilerServices;
using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using DotNext.Linq.Expressions;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.FastArrow;
using Varena;
using Array = Apache.Arrow.Array;

namespace Iceberg.Net.Query.Expressions;

public sealed class ExecutionContext
{
    public required VirtualBuffer Arena { get; init; }
    public required UnsafeArenaMemoryAllocator ArrowAllocator { get; init; }
}

public class BufferTransformVisitor : ExpressionVisitorNarrow<Expression, Expression, Expression,
    NewExpression, ElementInit, MemberBinding, MemberAssignment, MemberListBinding, MemberMemberBinding>
{
    private readonly Stack<(ParameterExpression original, ParameterExpression mapped)> _bindings = [];
    private readonly ParameterExpression _ctxParam = Expression.Parameter(typeof(ExecutionContext), "ctx");
    private readonly Dictionary<MemberInfo, int> _memberIndex = [];
    private readonly Stack<Expression?> _builderStack = new();
    private readonly Stack<InputType[]?> _inputTypes = new([[InputType.Identity]]);
    
    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        Type returnType = Nullable.GetUnderlyingType(node.ReturnType) ?? node.ReturnType;
        EnsureBindMemberIndexes(returnType);
        
        ParameterExpression resultBuilderParam = Expression.Parameter(
            ArrowTypeUtils.ForCSharpType(returnType).BuilderType,
            "builder");

        _builderStack.Push(resultBuilderParam);

        // Create bindings for local params (based on current _inputTypes)
        foreach ((var index, ParameterExpression expression) in node.Parameters.Index())
        {
            EnsureBindMemberIndexes(expression.Type);
            _bindings.Push(
                (
                expression,
                Expression.Parameter(
                    MakeInputTypeForArray(index),
                    expression.Name)));
        }

        Expression? expr = base.VisitLambda(node);

        foreach (ParameterExpression _ in node.Parameters) _bindings.Pop();

        _builderStack.Pop();
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

            Expression? subBuilder = null;
            if (underlying.IsClass || underlying is { IsValueType: true, IsPrimitive: false })
            {
                Expression? parentBuilder = _builderStack.Peek();
                subBuilder = Expression.Call(
                    parentBuilder,
                    typeof(StructArrayBuilder).GetMethod(
                            nameof(StructArrayBuilder.GetFieldBuilder))!
                        .MakeGenericMethod(typeof(StructArrayBuilder)),
                    Expression.Constant(i));
            }
            
            _builderStack.Push(subBuilder);
            visitedArgs.Add(Visit(node.Arguments[i]));
            _builderStack.Pop();
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
        // TODO instance calls
        Debug.Assert(node.Object == null);

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

            Expression originalEnumerable = node.Arguments[0];
            LambdaExpression originalPredicate = (LambdaExpression)node.Arguments[1];

            List<(ParameterExpression, ParameterExpression)> closureParams = GetClosureParameters(originalPredicate);
            var hasClosures = closureParams.Count > 0;

            Expression enumerable = Visit(originalEnumerable);

            if (hasClosures)
            {
                // Create block-level variables for closures (captured by nested lambdas via expression compiler)
                List<(ParameterExpression original, ParameterExpression closureVar, ParameterExpression originalBinding
                    )> closureVars = closureParams
                    .Select(pair => (
                        original: pair.Item1,
                        closureVar: Expression.Variable(typeof(IndexedInput), pair.Item1.Name + "_closure"),
                        originalBinding: _bindings.First(b => b.original == pair.Item1).mapped
                    ))
                    .ToList();

                // Push closure vars as bindings so MakeParameter returns the block variable
                foreach ((ParameterExpression original, ParameterExpression closureVar, ParameterExpression
                         originalBinding) cv in closureVars)
                    _bindings.Push((cv.original, cv.closureVar));

                Expression enumerableWithClosures = Visit(originalEnumerable);

                Expression innerResult = methodName switch
                {
                    "Select" => GenerateListSelect(enumerableWithClosures, originalPredicate),
                    "All" => GenerateListAll(enumerableWithClosures, originalPredicate),
                    "Any" => GenerateListAny(enumerableWithClosures, originalPredicate),
                    "Where" => GenerateListWhere(enumerableWithClosures, originalPredicate),
                    _ => throw new NotImplementedException("this method can't be mapped yet")
                };

                foreach ((ParameterExpression original, ParameterExpression closureVar, ParameterExpression
                         originalBinding) _ in closureVars) _bindings.Pop();
                
                ParameterExpression rowVar = Expression.Variable(typeof(int), "row");
                
                IEnumerable<BinaryExpression> closureAssigns = closureVars.Select(cv =>
                    Expression.Assign(
                        cv.closureVar,
                        Expression.New(
                            typeof(IndexedInput).GetConstructor([typeof(IArrowArray), typeof(int)])!,
                            Expression.Property(cv.originalBinding, nameof(IInput<>.Array)),
                            rowVar)));
                
                List<ParameterExpression> blockVars = closureVars.Select(cv => cv.closureVar).ToList();

                Expression forLoop = ExpressionUtilities.ForExpression(
                    rowVar,
                    Expression.Constant(0),
                    Expression.LessThan(rowVar, Expression.Property(enumerable, nameof(IInput<>.Length))),
                    Expression.PostIncrementAssign(rowVar),
                    Expression.Block(blockVars, [..closureAssigns, innerResult]));
                
                return forLoop;
            }
            else
            {
                return methodName switch
                {
                    "Select" => GenerateListSelect(enumerable, originalPredicate),
                    "All" => GenerateListAll(enumerable, originalPredicate),
                    "Any" => GenerateListAny(enumerable, originalPredicate),
                    "Where" => GenerateListWhere(enumerable, originalPredicate),
                    _ => throw new NotImplementedException("this method can't be mapped yet")
                };
            }
        }
    }

    private Expression GenerateListSelect(
        Expression source,
        LambdaExpression originalPredicate)
    {
        InputType inputType = GetInputType(source);
        InputType predicateInputType = inputType switch
        {
            InputType.Identity => InputType.Identity,
            InputType.Ranged => InputType.Ranged,
            InputType.Masked => InputType.Ranged,
            InputType.Indexed => InputType.Ranged,
            _ => throw new ArgumentOutOfRangeException()
        };

        _inputTypes.Push([predicateInputType]);
        LambdaExpression predicate = (LambdaExpression)Visit(originalPredicate);
        _inputTypes.Pop();

        ArrowTypeInfo resultElementType = ArrowTypeUtils.ForCSharpType(originalPredicate.ReturnType);
        ArrowTypeInfo resultInfo = ArrowTypeUtils.ListOf(resultElementType);
        
        Type valueBuilderType = predicate.Parameters[^1].Type;


        return ExecuteWithBuilder(
            builder => ListOperations.ExecuteOneToOneListOpGen(
                source,
                (subInput, valueBuilder) =>
                {
                    return Expression.Invoke(predicate, _ctxParam, subInput, valueBuilder);
                },
                builder,
                valueBuilderType),
            resultInfo,
            source.Type);
    }


    private Expression GenerateListWhere(
        Expression source,
        LambdaExpression originalPredicate)
    {
        InputType inputType = GetInputType(source);
        _inputTypes.Push([inputType]);
        LambdaExpression predicate = (LambdaExpression)Visit(originalPredicate);
        _inputTypes.Pop();

        Type elementType = originalPredicate.Parameters[0].Type;
        ArrowTypeInfo elementTypeInfo = ArrowTypeUtils.ForCSharpType(elementType);
        LambdaExpression valueCopier = Copier.GenerateCopy(typeof(IndexedInput), elementTypeInfo);

        ArrowTypeInfo resultInfo = ArrowTypeUtils.ListOf(elementTypeInfo);

        return ExecuteWithBuilder(
            builder => ListOperations.ExecuteListWhereGen(
                _ctxParam,
                source,
                (subInput, maskBuilder) =>
                    Expression.Invoke(predicate, _ctxParam, subInput, maskBuilder),
                valueCopier,
                builder,
                elementTypeInfo.BuilderType),
            resultInfo,
            source.Type);
    }

    private Expression GenerateListAll(
        Expression source,
        LambdaExpression originalPredicate)
    {
        _builderStack.Push(null);
        Expression map = GenerateListSelect(source, originalPredicate);
        _builderStack.Pop();

        LambdaExpression all = (ExecutionContext ctx, RangedInput input, BooleanArrayBuilder builder) =>
            HideBuilderReturnGeneric(builder.Append(ArrowCompute.All((BooleanArray)input.Array, input.Range)));
        
        return ExecuteElementWiseListOp(
            map,
            all.WithName("All"),
            ArrowTypeUtils.ForCSharpType(typeof(bool)));
    }

    private Expression GenerateListAny(
        Expression source,
        LambdaExpression originalPredicate)
    {
        _builderStack.Push(null);
        Expression map = GenerateListSelect(source, originalPredicate);
        _builderStack.Pop();

        LambdaExpression any = (ExecutionContext ctx, RangedInput input, BooleanArrayBuilder builder) =>
            HideBuilderReturnGeneric(builder.Append(ArrowCompute.Any((BooleanArray)input.Array, input.Range)));

        return ExecuteElementWiseListOp(
            map,
            any.WithName("Any"),
            ArrowTypeUtils.ForCSharpType(typeof(bool)));
    }

    private Expression ExecuteElementWiseListOp(
        Expression source,
        LambdaExpression op,
        ArrowTypeInfo resultInfo)
    {
        MethodInfo method =
            typeof(ListOperations).GetMethod(nameof(ListOperations.ExecuteElementWiseListOp))!
                .MakeGenericMethod(
                    source.Type,
                    resultInfo.ArrayType,
                    resultInfo.BuilderType);

        return ExecuteWithBuilder(
            builder => Expression.Call(null, method, _ctxParam, source, op, builder),
            resultInfo,
            source.Type);
    }

    protected override Expression MakeBinary(
        BinaryExpression node,
        Expression left,
        Expression conversion,
        Expression right)
    {
        var leftIsSpanLike = IsSpanLike(left);
        var rightIsSpanLike = IsSpanLike(right);
        var leftIsScalarLike = IsScalarLike(left);
        var rightIsScalarLike = IsScalarLike(right);

        if (leftIsSpanLike && rightIsSpanLike)
        {
            // Both vectorized — use Zip<T> or BitmapOps
            Expression leftSpan = ExtractRawSpan(left, node.Left);
            Expression rightSpan = ExtractRawSpan(right, node.Right);
            return ExecuteBinarySpan(node, leftSpan, rightSpan);
        }

        if (leftIsSpanLike && rightIsScalarLike)
        {
            // Left span, right scalar
            Expression leftSpan = ExtractRawSpan(left, node.Left);
            Expression rightScalar = ExtractRawScalar(right, node.Right);
            return ExecuteBinarySpanScalarRight(node, leftSpan, rightScalar);
        }

        if (leftIsScalarLike && rightIsSpanLike)
        {
            // Left scalar, right span
            Expression leftScalar = ExtractRawScalar(left, node.Left);
            Expression rightSpan = ExtractRawSpan(right, node.Right);
            return ExecuteBinarySpanScalarLeft(node, leftScalar, rightSpan);
        }

        // Both scalar — use standard C# binary expression
        {
            Expression l = ExtractRawScalar(left, node.Left);
            Expression r = ExtractRawScalar(right, node.Right);
            return Expression.MakeBinary(node.NodeType, l, r);
        }
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
        // Return the constant value directly — it will be used as a scalar operand
        // in binary expressions, or bulk-repeated when it's the entire lambda body.
        return node;
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

    protected override Expression MakeLambda<T>(
        Expression<T> node,
        Expression body,
        ReadOnlyCollection<Expression> parameters)
    {
        List<ParameterExpression> lambdaParams = [_ctxParam, ..parameters.Cast<ParameterExpression>()];

        var hasBuilderParam = TryGetParameter(_builderStack, out ParameterExpression? builderParam);
        if (!hasBuilderParam)
            throw new InvalidOperationException("Lambda cannot be constructed without a builder parameter");

        lambdaParams.Add(builderParam!);

        // TODO this is a mess
        
        // If the body is an IndexedInput, extract the scalar value before appending
        if (IsIndexedInput(body))
            body = ExtractRawScalar(body, node.Body);

        // If the body is a scalar and the lambda has an input, broadcast to input length
        if (!IsSpan(body) && !IsBitmap(body) && body.Type != typeof(void)
            && !body.Type.Name.Contains("Builder") && lambdaParams.Count > 1)
        {
            ParameterExpression inputParam = lambdaParams[1];
            Expression count = Expression.Property(inputParam, nameof(IInput<>.Length));
            body = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.FillSpan))!
                .CallStatic([body.Type], [_ctxParam, body, count]);
        }

        body = IsSpan(body) ? AppendSpanToBuilder(body, builderParam!)
            : IsBitmap(body) ? AppendBitmapToBuilder(body, builderParam!)
            : body.Type == typeof(void) ? body // nested list op already populated the builder
            : !body.Type.Name.Contains("Builder") ? AppendScalarToBuilder(body, builderParam!)
            : body;
        LambdaExpression lambda = Expression.Lambda(
            EnsureNoReturnValue(body),
            false,
            lambdaParams)
            .WithName($"QueryMethod_{node}");
        return lambda;
    }

    [DynamicDependency(DynamicallyAccessedMemberTypes.AllMethods, typeof(IArrowArrayBuilder<,,>))]
    private static Expression AppendSpanToBuilder(Expression span, Expression builder)
    {
        // If span is Span<T>, convert to ReadOnlySpan<T> for the builder's Append/AppendMask
        span = AsReadOnlySpanExpr(span);

        if (IsBooleanBuilder(builder))
        {
            Type elementType = SpanElementType(span);
            if (elementType == typeof(bool))
                return builder.Call(nameof(BooleanArrayBuilder.Append), span);
            MethodInfo method = typeof(BooleanArrayBuilder).GetMethod(nameof(BooleanArrayBuilder.AppendMask))!
                .MakeGenericMethod(elementType);
            return builder.Call(method, span);
        }
        else
        {
            return builder.Call(nameof(IArrowArrayBuilder<,,>.Append), span);
        }
    }

    private static Expression AppendBitmapToBuilder(Expression bitmap, Expression builder)
    {
        if (!IsBooleanBuilder(builder))
            throw new InvalidOperationException("Bitmap results can only be appended to BooleanArrayBuilder");

        // Extract Bitmap.Bytes (Span<byte>) and Bitmap.Length (int)
        Expression bytes = Expression.Field(bitmap, nameof(Bitmap.Bytes));
        Expression length = Expression.Field(bitmap, nameof(Bitmap.Length));

        // Convert Span<byte> to ReadOnlySpan<byte> for the builder method
        bytes = AsReadOnlySpanExpr(bytes);

        MethodInfo method = typeof(BooleanArrayBuilder).GetMethod(nameof(BooleanArrayBuilder.AppendBitmap))!;
        return builder.Call(method, bytes, length);
    }

    private static Expression AppendScalarToBuilder(Expression scalar, Expression builder)
    {
        if (IsBooleanBuilder(builder))
            return builder.Call(nameof(BooleanArrayBuilder.Append), scalar);
        else
            return builder.Call(nameof(IArrowArrayBuilder<,,>.Append), scalar);
    }

    [DynamicDependency(DynamicallyAccessedMemberTypes.AllMethods, typeof(IInput<>))]
    private static Expression TransformInput(
        Expression input,
        Expression original,
        Func<Expression, Expression> transformer)
    {
        Expression transformed = transformer(AccessArray(input, original));
        return input.Call(nameof(IInput<>.Apply), transformed);
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
        return TransformInput(input, node.Expression!, expression => AccessStructField(node, expression));
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

    private List<(ParameterExpression, ParameterExpression)> GetClosureParameters(LambdaExpression predicate)
    {
        List<ParameterExpression> capturedParams = FreeVariableScanner.Scan(predicate).ToList();
        if (capturedParams.Count == 0) return [];

        // TODO handle different existing input types
        return capturedParams
            .Where(param => !_bindings.First(pair => pair.original == param).mapped.Name!.Contains("_closure"))
            .Select(param => (param,
                Expression.Parameter(
                    typeof(IndexedInput),
                    param.Name + "_closure"))).ToList();
    }

    protected override Expression MakeMethodCall(
        MethodCallExpression node,
        Expression @object,
        ReadOnlyCollection<Expression> arguments)
    {
        throw new InvalidOperationException("Must have done everything with VisitMethodCall");
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
            return BuildStructWithBuilder(arguments, node.Arguments);
        }

        throw new NotImplementedException("not yet");
    }

    private Expression BuildStructWithBuilder(
        ReadOnlyCollection<Expression> arguments,
        ReadOnlyCollection<Expression> originalArgs)
    {
        Expression sbExpr = _builderStack.Peek()!;
        Debug.Assert(sbExpr.Type == typeof(StructArrayBuilder));

        return Expression.Block(
            arguments.Zip(originalArgs).Select((tuple, index) =>
        {
            Expression mapped = tuple.First;
            Expression original = tuple.Second;
            if (mapped.Type == typeof(void) || mapped.Type.Name.Contains("Builder"))
            {
                // nested struct already builder-populated via VisitNew
                // the arg IS the block that populates the sub-builder — include it
                return mapped;
            }
            else if (IsIdentityInput(mapped))
            {
                // alias existing array directly
                return Expression.Call(
                        sbExpr,
                        typeof(StructArrayBuilder).GetMethod(nameof(StructArrayBuilder.SetFieldArray))!,
                        Expression.Constant(index),
                        AccessArray(mapped, original));
            }
            else if (IsRangedInput(mapped))
            {
            }
            else if (IsSpan(mapped))
            {
                // append span to field builder
                Type elemType = SpanElementType(mapped);
                Type builderType = ArrowTypeUtils.ForCSharpType(elemType).BuilderType;
                MethodCallExpression builder = Expression.Call(
                    sbExpr,
                    typeof(StructArrayBuilder).GetMethod(nameof(StructArrayBuilder.GetFieldBuilder))!.MakeGenericMethod(
                        builderType),
                    Expression.Constant(index));
                return AppendSpanToBuilder(mapped, builder);
            }

            throw new NotImplementedException($"Unsupported field type in builder: {mapped.Type.Name}");
        }));
    }

    protected override Expression MakeNewArray(NewArrayExpression node, ReadOnlyCollection<Expression> expressions)
    {
        return node;
    }

    protected override Expression MakeParameter(ParameterExpression node)
    {
        return _bindings.First(tuple => tuple.original == node).mapped;
    }

    protected override Expression MakeTypeBinary(TypeBinaryExpression node, Expression expression)
    {
        return node;
    }

    protected override Expression MakeUnary(UnaryExpression node, Expression operand)
    {
        if (node.NodeType == ExpressionType.Convert)
        {
            if (IsInput(operand))
            {
                if (IsIndexedInput(operand))
                {
                    // For indexed input, convert scalar using normal operators
                    Expression scalar = ExtractRawScalar(operand, node.Operand);
                    Type targetType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
                    if (scalar.Type == targetType) return scalar;
                    return Expression.Convert(scalar, node.Type);
                }

                // IdentityInput or RangedInput — convert via ArrowCompute.ConvertLogical
                Expression rawSpan = ExtractRawSpan(operand, node.Operand);
                Type elemType = SpanElementType(rawSpan);
                Type resultUnderlyingType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
                if (elemType == resultUnderlyingType) return rawSpan;
                MethodInfo convertMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ConvertLogical))!
                    .MakeGenericMethod(elemType, resultUnderlyingType);
                return Expression.Call(
                    null,
                    convertMethod,
                    [_ctxParam, rawSpan, Expression.Constant(MemoryConfig.None)]);
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
                    [_ctxParam, operand, Expression.Constant(MemoryConfig.None)]);
            }
            else
            {
                // Scalar operand (constant or already-converted value)
                Type targetType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
                if (operand.Type == targetType) return operand;
                return Expression.Convert(operand, node.Type);
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
                [ArrowTypeUtils.ForCSharpType(expr.Type).ArrayType],
                [buffer, _memberIndex[expr.Member].Quoted]);
        }

        throw new InvalidOperationException("only struct can be accessed");
    }

    private Type MakeInputTypeForArray(int index)
    {
        InputType inputType = _inputTypes.Peek()![index];
        return inputType switch
        {
            InputType.Identity => typeof(IdentityInput),
            InputType.Ranged => typeof(RangedInput),
            InputType.Masked => typeof(MaskedInput),
            InputType.Indexed => typeof(IndexedInput),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    private static InputType GetInputType(Expression input)
    {
        Debug.Assert(input.Type.ImplementsInterface(typeof(IInput<>)));
        if (IsIdentityInput(input))
            return InputType.Identity;
        else if (IsRangedInput(input))
            return InputType.Ranged;
        else if (IsIndexedInput(input))
            return InputType.Indexed;

        throw new NotImplementedException();
    }

    private void EnsureBindMemberIndexes(Type type)
    {
        if (type.IsClass || type is { IsValueType: true, IsPrimitive: false })
        {
            List<MemberInfo> members = type.GetMembers(BindingFlags.Instance | BindingFlags.Public)
                .Where(info => info is PropertyInfo or FieldInfo).ToList();
            foreach ((var idx, MemberInfo member) in members.Index())
            {
                if (member is FieldInfo field)
                    EnsureBindMemberIndexes(field.FieldType);
                else if (member is PropertyInfo propertyInfo) EnsureBindMemberIndexes(propertyInfo.PropertyType);

                _memberIndex[member] = idx;
            }
        }
    }

    private static Type GetConcreteBuilderType(Type bufferType)
    {
        if (bufferType.IsGenericType && bufferType.GetGenericTypeDefinition() == typeof(PrimitiveArray<>))
        {
            Type innerType = bufferType.GetGenericArguments()[0];
            return ArrowTypeUtils.ForCSharpType(innerType).BuilderType;
        }

        if (bufferType == typeof(StructArray)) return typeof(StructArrayBuilder);
        if (bufferType == typeof(ListArray)) return typeof(ListArrayBuilder);
        if (bufferType == typeof(BooleanArray)) return typeof(BooleanArrayBuilder);
        throw new NotSupportedException($"No concrete builder for {bufferType.Name}");
    }

    private static Type SpanElementType(Expression expression)
    {
        if (IsSpan(expression))
            // TODO bitmap is problematic
            return expression.Type.GetGenericArguments()[0];

        throw new InvalidOperationException("Expression wasn't a span");
    }

    private Expression BuildArray(ParameterExpression builder, Type sourceType)
    {
        Expression array = builder.Call(nameof(IArrowArrayBuilder<>.Build), ArrowArenaAllocator());
        return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BuildArray))!
            .MakeGenericMethod(sourceType)
            .CallStatic([], [array]);
    }

    private static ParameterExpression GetInput(LambdaExpression op, int index)
    {
        // +1 to account for context
        ParameterExpression param = op.Parameters[index + 1];
        Debug.Assert(IsInput(param));
        return param;
    }

    private static bool IsSpan(Expression expression)
    {
        return expression.Type.Name.Contains("Span");
    }

    private static bool IsBitmap(Expression expression)
    {
        return expression.Type.Name == "Bitmap";
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
        return typeof(IdentityInput).CallStatic(nameof(IdentityInput.New), expression);
    }

    private static bool IsRangedInput(Expression expression)
    {
        return expression.Type.Name.Contains("RangedInput");
    }

    private static bool IsMaskedInput(Expression expression)
    {
        return expression.Type.Name.Contains("MaskedInput");
    }

    private static bool IsIndexedInput(Expression expression)
    {
        return expression.Type.Name.Contains("IndexedInput");
    }

    private static bool IsBooleanBuilder(Expression expression)
    {
        return expression.Type == typeof(BooleanArrayBuilder);
    }

    private static Expression EnsureNoReturnValue(Expression expression)
    {
        // Already void — nothing to do
        if (expression.Type == typeof(void))
            return expression;

        // Builder return — discard the builder value
        if (expression.Type.Name.Contains("Builder"))
            return Expression.Block(expression, Expression.Empty());

        throw new InvalidOperationException($"Invalid return type {expression.Type}");
    }

    private static void HideBuilderReturnGeneric<T>(T _)
    {
    }

    /// <summary>
    ///     Extracts a writable <see cref="Span{T}" /> from a span-like input expression
    ///     (IdentityInput, RangedInput, or an already-span expression).
    ///     Always returns a <see cref="Span{T}" /> typed expression.
    /// </summary>
    private static Expression ExtractRawSpan(Expression input, Expression original)
    {
        if (IsSpan(input)) return EnsureWritableSpan(input);

        if (IsBitmap(input)) return input; // Already a Bitmap

        if (IsIdentityInput(input))
        {
            // For BooleanArray (bool type), extract as Bitmap to preserve logical bit count
            Type underlying = Nullable.GetUnderlyingType(original.Type) ?? original.Type;
            if (underlying == typeof(bool))
            {
                Expression array = AccessArray(input, original);
                Expression roSpan = AccessValuesReadOnly(array);
                Expression writable = EnsureWritableSpan(roSpan);
                Expression length = Expression.Property(
                    Expression.Convert(array, typeof(Array)),
                    nameof(Array.Length));
                return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.AsBitmap))!
                    .CallStatic([], [writable, length]);
            }

            return AccessWritableSpan(input, original);
        }

        if (IsRangedInput(input))
        {
            Expression array = AccessArray(input, original);
            Expression roSpan = AccessValuesReadOnly(array);
            // RangedInput.Slice takes ReadOnlySpan<T> and returns ReadOnlySpan<T>
            Expression sliced = Expression.Call(
                input,
                nameof(RangedInput.Slice),
                [roSpan.Type.GetGenericArguments()[0]],
                roSpan);
            return EnsureWritableSpan(sliced);
        }

        throw new InvalidOperationException($"Cannot extract span from: {input.Type.Name}");
    }

    /// <summary>
    ///     Extracts a scalar value from a scalar-like input expression
    ///     (IndexedInput, ConstantExpression, or already-scalar).
    /// </summary>
    private static Expression ExtractRawScalar(Expression input, Expression original)
    {
        if (IsIndexedInput(input))
        {
            // For IndexedInput, we need the span element type. The ValueAt method
            // on IndexedInput returns a scalar T. We compute that T from the original
            // expression's type (unwrapping nullable if needed).
            Type scalarType = Nullable.GetUnderlyingType(original.Type) ?? original.Type;
            Expression array = AccessArray(input, original);
            Expression roSpan = AccessValuesReadOnly(array);
            return Expression.Call(
                input,
                nameof(IndexedInput.ValueAt),
                [scalarType],
                roSpan);
        }

        // Constant or already a scalar (e.g., from MakeConstant or a prior Convert)
        if (!IsSpan(input) && !IsInput(input))
        {
            if (original is ConstantExpression constExpr)
                return Expression.Constant(constExpr.Value, constExpr.Type);
            return input;
        }

        throw new InvalidOperationException($"Cannot extract scalar from: {input.Type.Name}");
    }

    /// <summary>
    ///     Returns true if the expression represents a scalar value
    ///     (IndexedInput, constant, or plain scalar — not a span or array-wrapper input).
    /// </summary>
    private static bool IsScalarLike(Expression input)
    {
        return IsIndexedInput(input) || (!IsSpan(input) && !IsInput(input));
    }

    /// <summary>
    ///     Returns true if the expression represents a span-like value
    ///     (already a Span, IdentityInput, or RangedInput).
    /// </summary>
    private static bool IsSpanLike(Expression input)
    {
        return IsSpan(input) || IsBitmap(input) || IsIdentityInput(input) || IsRangedInput(input);
    }

    /// <summary>
    ///     If the expression is a <see cref="Span{T}" />, converts it to <see cref="ReadOnlySpan{T}" />
    ///     via <see cref="ArrowCompute.AsReadOnlySpan{T}" />. Otherwise returns as-is.
    /// </summary>
    private static Expression AsReadOnlySpanExpr(Expression spanExpr)
    {
        if (spanExpr.Type.IsGenericType && spanExpr.Type.GetGenericTypeDefinition() == typeof(Span<>))
        {
            Type elementType = spanExpr.Type.GetGenericArguments()[0];
            return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.AsReadOnlySpan))!
                .CallStatic([elementType], [spanExpr]);
        }

        return spanExpr; // Already ReadOnlySpan<T> or other
    }

    /// <summary>
    ///     If the expression is a <see cref="ReadOnlySpan{T}" />, converts it to <see cref="Span{T}" />
    ///     via <see cref="ArrowCompute.AsWritable{T}" />. Otherwisem returns as-is.
    /// </summary>
    private static Expression EnsureWritableSpan(Expression spanExpr)
    {
        if (spanExpr.Type.IsGenericType && spanExpr.Type.GetGenericTypeDefinition() == typeof(ReadOnlySpan<>))
        {
            Type elementType = spanExpr.Type.GetGenericArguments()[0];
            return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.AsWritable))!
                .CallStatic([elementType], [spanExpr]);
        }

        return spanExpr; // Already Span<T>
    }

    /// <summary>
    ///     Produces a writable <see cref="Span{T}" /> from an IInput expression
    ///     by accessing the underlying array's Values and converting to a mutable span.
    /// </summary>
    private static Expression AccessWritableSpan(Expression input, Expression original)
    {
        Expression array = AccessArray(input, original);
        Expression roSpan = AccessValuesReadOnly(array);
        return EnsureWritableSpan(roSpan);
    }

    private static Expression AccessValuesReadOnly(Expression array)
    {
        return array.Property(nameof(PrimitiveArray<>.Values));
    }

    private static Expression AccessArray(Expression input, Expression original)
    {
        Debug.Assert(input.Type.ImplementsInterface(typeof(IInput<>)));
        Type arrayType = ArrowTypeUtils.ForCSharpType(original.Type).ArrayType;

        return Expression.Convert(input.Property(nameof(IInput<>.Array)), arrayType);
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

    private MethodCallExpression MakeBuilderFor(ArrowTypeInfo resultInfo)
    {
        return Expression.Call(
            null,
            typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.MakeBuilderForGeneric))!
                .MakeGenericMethod(resultInfo.BuilderType),
            resultInfo.ArrowType.Quoted,
            ArrowArenaAllocator());
    }

    private Expression ExecuteWithBuilder(
        Func<ParameterExpression, Expression> func,
        ArrowTypeInfo resultInfo,
        Type sourceType)
    {
        Expression? outer = _builderStack.Peek();
        if (outer is not null)
            return func((ParameterExpression)outer);
        else
        {
            ParameterExpression builder = Expression.Variable(
                resultInfo.BuilderType,
                $"{resultInfo.BuilderType.Name}_tmpBuilder");
            return Expression.Block(
                [builder],
                Expression.Assign(builder, MakeBuilderFor(resultInfo)),
                func(builder),
                BuildArray(builder, sourceType));
        }
    }

    private Expression ExecuteBinarySpan(BinaryExpression node, Expression left, Expression right)
    {
        var leftIsBitmap = IsBitmap(left);
        var rightIsBitmap = IsBitmap(right);
        var leftIsSpan = IsSpan(left);
        var rightIsSpan = IsSpan(right);

        // If either side is a Bitmap, convert the other side and use BitmapOps
        if (leftIsBitmap || rightIsBitmap)
        {
            if (leftIsSpan)
                left = ConvertSpanToBitmap(left);
            if (rightIsSpan)
                right = ConvertSpanToBitmap(right);

            return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapOps))!.CallStatic(
                [],
                [_ctxParam, left, right, node.NodeType.Quoted, Expression.Constant(MemoryConfig.None)]);
        }

        // Both are spans — compare element types
        Type leftElementType = SpanElementType(left);
        Type rightElementType = SpanElementType(right);

        if (leftElementType != rightElementType)
        {
            // Type mismatch — convert both to bitmaps
            left = ConvertSpanToBitmap(left);
            right = ConvertSpanToBitmap(right);

            return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapOps))!.CallStatic(
                [],
                [_ctxParam, left, right, node.NodeType.Quoted, Expression.Constant(MemoryConfig.None)]);
        }

        // Same type — use Zip<T>
        return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.Zip))!
            .CallStatic(
                [leftElementType],
                [_ctxParam, left, right, node.NodeType.Quoted, Expression.Constant(MemoryConfig.None)]);
    }

    /// <summary>Converts a Span{T} expression to a Bitmap expression via BitmapFromMask{T}.</summary>
    private Expression ConvertSpanToBitmap(Expression spanExpr)
    {
        Type elementType = SpanElementType(spanExpr);
        return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapFromMask))!
            .CallStatic(
                [elementType],
                [_ctxParam, spanExpr]);
    }

    private Expression ExecuteBinarySpanScalarRight(
        BinaryExpression node,
        Expression leftSpan,
        Expression rightScalar)
    {
        Type elementType = SpanElementType(leftSpan);

        // For boolean operations on masks, the scalar needs to be converted to the mask type
        // Otherwise the element types should match (C# compiler inserts Convert nodes)

        return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ZipScalarRight))!
            .CallStatic(
                [elementType],
                [_ctxParam, leftSpan, rightScalar, node.NodeType.Quoted, Expression.Constant(MemoryConfig.None)]);
    }

    private Expression ExecuteBinarySpanScalarLeft(
        BinaryExpression node,
        Expression leftScalar,
        Expression rightSpan)
    {
        Type elementType = SpanElementType(rightSpan);

        return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ZipScalarLeft))!
            .CallStatic(
                [elementType],
                [_ctxParam, leftScalar, rightSpan, node.NodeType.Quoted, Expression.Constant(MemoryConfig.None)]);
    }
}