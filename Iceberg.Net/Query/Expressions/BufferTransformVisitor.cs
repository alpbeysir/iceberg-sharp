using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq.CompilerServices;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
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

public enum InputType
{
    Identity,
    Ranged,
    Masked,
    Indexed
}

public interface IInput<out TSelf> where TSelf : IInput<TSelf>
{
    public IArrowArray Array { get; }
    public int Length { get; }

    public TSelf Apply(IArrowArray array);
}

public readonly record struct IdentityInput(IArrowArray Array)
    : IInput<IdentityInput>
{
    public IdentityInput Apply(IArrowArray array)
    {
        return new IdentityInput(array);
    }

    public static IdentityInput New(IArrowArray array)
    {
        return new IdentityInput(array);
    }

    public int Length => Array.Length;
}

public readonly record struct RangedInput(IArrowArray Array, Range Range)
    : IInput<RangedInput>
{
    public RangedInput Apply(IArrowArray array)
    {
        return new RangedInput(array, Range);
    }
    
    public ReadOnlySpan<T> Slice<T>(ReadOnlySpan<T> span)
    {
        (int Offset, int Length) offsetAndLength = Range.GetOffsetAndLength(span.Length);
        return span.Slice(offsetAndLength.Offset, offsetAndLength.Length);
    }

    public int Length => Array.Length - Range.GetOffsetAndLength(Array.Length).Length;
}

public readonly record struct MaskedInput(IArrowArray Array, BooleanArray Mask)
    : IInput<MaskedInput>
{
    public MaskedInput Apply(IArrowArray array)
    {
        return new MaskedInput(array, Mask);
    }

    public int Length => throw new NotImplementedException();
}

public readonly record struct IndexedInput(IArrowArray Array, int Index)
    : IInput<IndexedInput>
{
    public IndexedInput Apply(IArrowArray array)
    {
        return new IndexedInput(array, Index);
    }

    public int Length => 1;

    public T ValueAt<T>(ReadOnlySpan<T> span)
    {
        return span[Index];
    }
}

public class BufferTransformVisitor : ExpressionVisitorNarrow<Expression, LambdaExpression, Expression,
    NewExpression, ElementInit, MemberBinding, MemberAssignment, MemberListBinding, MemberMemberBinding>
{
    private readonly Stack<(ParameterExpression original, ParameterExpression mapped)> _bindings = [];
    private readonly ParameterExpression _ctxParam = Expression.Parameter(typeof(ExecutionContext), "ctx");
    private readonly Dictionary<MemberInfo, int> _memberIndex = [];
    private readonly Stack<Expression?> _builderStack = new();
    private readonly Stack<InputType[]?> _inputTypes = new([[InputType.Identity]]);
    private const int MaxBatchSize = 65536;

    public void SetInitialInputTypes(InputType[] types)
    {
        if (_inputTypes.Count != 1) throw new InvalidOperationException();

        _inputTypes.Pop();
        _inputTypes.Push(types);
    }

    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        Type returnType = Nullable.GetUnderlyingType(node.ReturnType) ?? node.ReturnType;
        ParameterExpression resultBuilderParam = Expression.Parameter(
            GetConcreteBuilderType(GetBufferType(returnType)),
            "builder");

        _builderStack.Push(resultBuilderParam);

        // Create bindings for local params (based on current _inputTypes)
        foreach ((var index, ParameterExpression expression) in node.Parameters.Index())
        {
            _bindings.Push(
                (
                expression,
                Expression.Parameter(
                    MakeInputTypeForArray(GetBufferType(expression.Type), index),
                    expression.Name)));
        }

        Expression? expr = base.VisitLambda(node);

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

            List<(ParameterExpression, ParameterExpression)> closureParams = SetupClosureParameters(originalPredicate);
            var hasClosures = closureParams.Count > 0;

            Expression enumerable = Visit(originalEnumerable);

            if (hasClosures)
            {
            }

            closureParams.ForEach(pair => _bindings.Push(pair));

            Expression result = methodName switch
            {
                "Select" => GenerateListSelect(enumerable, originalPredicate),
                "All" => GenerateListAll(enumerable, originalPredicate),
                "Any" => GenerateListAny(enumerable, originalPredicate),
                "Where" => GenerateListWhere(enumerable, originalPredicate),
                _ => throw new NotImplementedException("this method can't be mapped yet")
            };

            closureParams.ForEach(_ => _bindings.Pop());

            return result;
        }
    }

    private static LambdaExpression PassValueBuilderAndCall(
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
#if DEBUG
        return Expression.Convert(listBuilder.Property(nameof(ListArrayBuilder.ValueBuilder)), valueBuilderType);
#else

        return typeof(Unsafe).GetMethod(nameof(Unsafe.As), [typeof(object)])!.CallStatic(
            [valueBuilderType],
            [listBuilder.Property(nameof(ListArrayBuilder.ValueBuilder))]);
#endif
    }

    private Expression GenerateListSelect(
        Expression source,
        LambdaExpression originalPredicate)
    {
        InputType inputType = GetInputType(source);
        _inputTypes.Push([inputType]);
        LambdaExpression predicate = (LambdaExpression)Visit(originalPredicate);
        _inputTypes.Pop();

        LambdaExpression op = PassValueBuilderAndCall(predicate);
        ArrowTypeInfo resultElementType = ArrowTypeUtils.ForCSharpType(originalPredicate.ReturnType);
        ArrowTypeInfo resultInfo = ArrowTypeUtils.ListOf(resultElementType);

        MethodInfo method = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteListSelect))!
            .MakeGenericMethod(source.Type);

        ParameterExpression builderParam = Expression.Parameter(resultInfo.BuilderType, "builder");
        LambdaExpression callLambda = Expression.Lambda(
            Expression.Call(null, method, _ctxParam, source, op, builderParam),
            builderParam);

        return TryExecuteWithBuilder(callLambda, resultInfo);
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
        LambdaExpression valueCopier = GenerateCopy(typeof(IndexedInput), elementTypeInfo);
        LambdaExpression copier = PassValueBuilderAndCall(valueCopier);

        ArrowTypeInfo resultInfo = ArrowTypeUtils.ListOf(elementTypeInfo);
        MethodInfo method = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteListWhere))!
            .MakeGenericMethod(source.Type);

        ParameterExpression builderParam = Expression.Parameter(resultInfo.BuilderType, "builder");
        LambdaExpression callLambda = Expression.Lambda(
            Expression.Call(null, method, _ctxParam, source, predicate, copier, builderParam),
            builderParam);

        return TryExecuteWithBuilder(callLambda, resultInfo);
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
            all,
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
            any,
            ArrowTypeUtils.ForCSharpType(typeof(bool)));
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

    private static LambdaExpression GeneratePrimitiveCopy<TInput, TArray, TValue, TBuilder>()
        where TInput : IInput<TInput>
        where TArray : PrimitiveArray<TValue>
        where TBuilder : PrimitiveArrayBuilder<TValue, TArray, TBuilder>
        where TValue : struct, IEquatable<TValue>
    {
        LambdaExpression expr = (ExecutionContext ctx, TInput input, TBuilder builder) =>
            HideBuilderReturnGeneric(ArrowCompute.CopyPrimitive<TInput, TArray, TValue, TBuilder>(ctx, input, builder));
        return expr;
    }

    private static LambdaExpression GenerateListCopy<TInput>(LambdaExpression valueCopier)
        where TInput : IInput<TInput>
    {
        var compiled = (Action<ExecutionContext, TInput, ListArrayBuilder>)PassValueBuilderAndCall(valueCopier).Compile();
        LambdaExpression expr = (ExecutionContext ctx, TInput input, ListArrayBuilder builder) =>
            HideBuilderReturnGeneric(
                ArrowCompute.CopyList(
                    ctx,
                    input,
                    builder,
                    compiled));
        return expr;
    }

    private static LambdaExpression GenerateCopy(Type inputType, ArrowTypeInfo typeInfo)
    {
        IArrowType arrowType = typeInfo.ArrowType;
        if (arrowType is ListType listType)
        {
            ArrowTypeInfo elementTypeInfo = ArrowTypeUtils.ForArrowType(listType.ValueDataType);
            LambdaExpression valueCopier = GenerateCopy(inputType, elementTypeInfo);
            MethodInfo method = typeof(BufferTransformVisitor).GetMethod(nameof(GenerateListCopy), BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(
                inputType
            );
            return (LambdaExpression)method.Invoke(null, [valueCopier])!;
        }
        else if (arrowType is FixedWidthType)
        {
            MethodInfo method = typeof(BufferTransformVisitor).GetMethod(nameof(GeneratePrimitiveCopy), BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(
                inputType,
                typeInfo.ArrayType,
                typeInfo.CSharpType,
                typeInfo.BuilderType
            );
            return (LambdaExpression)method.Invoke(null, [])!;
        }
        else
        {
            throw new NotImplementedException();
        }

        throw new NotImplementedException();
    }

    private Expression ExecuteElementWiseListOp(
        Expression source,
        LambdaExpression op,
        ArrowTypeInfo resultInfo)
    {
        var hasRange = IsRangedInput(GetInput(op, 0));
        if (!hasRange) throw new InvalidOperationException("must have range to execute element-wise");
        
        Type arrowResultBuilderType = GetResultBuilder(op).Type;
        MethodInfo method =
            typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteElementWiseListOp))!
                .MakeGenericMethod(
                    source.Type,
                    resultInfo.ArrayType,
                    arrowResultBuilderType);

        ParameterExpression builderParam = Expression.Parameter(resultInfo.BuilderType, "builder");
        LambdaExpression callLambda = Expression.Lambda(
            Expression.Call(null, method, _ctxParam, source, op, builderParam),
            builderParam);

        return TryExecuteWithBuilder(callLambda, resultInfo);
    }

    protected override Expression MakeBinary(
        BinaryExpression node,
        Expression left,
        LambdaExpression conversion,
        Expression right)
    {
        var leftIsScalar = !IsSpan(left) && !IsInput(left);
        var rightIsScalar = !IsSpan(right) && !IsInput(right);

        if (leftIsScalar || rightIsScalar || IsIndexedInput(left) || IsIndexedInput(right))
        {
            // At least one operand is (or derived from) an indexed input — use scalar path
            Expression l = AsScalar(left, node.Left);
            Expression r = AsScalar(right, node.Right);
            return Expression.MakeBinary(node.NodeType, l, r);
        }

        Expression leftSpan = AsSpan(left, node.Left);
        Expression rightSpan = AsSpan(right, node.Right);
        return ExecuteBinarySpan(node, leftSpan, rightSpan);
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

        body = IsSpan(body) ? AppendSpanToBuilder(body, builderParam!)
            : !body.Type.Name.Contains("Builder") ? AppendScalarToBuilder(body, builderParam!)
            : body;
        LambdaExpression lambda = Expression.Lambda(
            HideBuilderReturn(body),
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
                .MakeGenericMethod(SpanElementType(span));
            return builder.Call(method, span);
        }
        else
        {
            return builder.Call(nameof(IArrowArrayBuilder<,,>.Append), span);
        }
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

    private Expression WrapWithClosureLoop(
        Expression source,
        LambdaExpression originalPredicate)
    {
        throw new NotImplementedException();
        // Re-build the predicate with IndexedInput.
        InputType[] saved = _inputTypes.Peek()!;
        _inputTypes.Pop();
        _inputTypes.Push([InputType.Indexed]);
        LambdaExpression predicate = (LambdaExpression)Visit(originalPredicate);
        _inputTypes.Pop();
        _inputTypes.Push(saved);

        ArrowTypeInfo resultInfo = ArrowTypeUtils.ListOf(
            ArrowTypeUtils.ForCSharpType(originalPredicate.Parameters[0].Type));

        Type elementType = originalPredicate.Parameters[0].Type;
        ArrowTypeInfo elementTypeInfo = ArrowTypeUtils.ForCSharpType(elementType);
        LambdaExpression valueCopier = GenerateCopy(typeof(IndexedInput), elementTypeInfo);
        LambdaExpression copier = PassValueBuilderAndCall(valueCopier);

        ParameterExpression builderParam = Expression.Parameter(resultInfo.BuilderType, "builder");

        MethodInfo execMethod = typeof(ArrowCompute)
            .GetMethod(nameof(ArrowCompute.ExecuteListWhere))!
            .MakeGenericMethod(source.Type);

        // For-loop call lambda: per-row iteration calling ExecuteListWhere
        ParameterExpression listVar = Expression.Variable(typeof(ListArray), "list");
        ParameterExpression rowVar = Expression.Variable(typeof(int), "row");
        ParameterExpression startVar = Expression.Variable(typeof(int), "start");
        ParameterExpression endVar = Expression.Variable(typeof(int), "end");
        LabelTarget rowBreak = Expression.Label("rowEnd");

        Expression listArray = Expression.Convert(
            Expression.Property(source, nameof(IInput<>.Array)),
            typeof(ListArray));
        MemberExpression offsets = Expression.Property(listVar, nameof(ListArray.ValueOffsets));
        MethodInfo readOff = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ReadOffset))!;
        MethodCallExpression getLen = Expression.Call(listVar, nameof(ListArray.GetValueLength), null, rowVar);

        // Per-row source: RangedInput on list values
        NewExpression rowSource = Expression.New(
            typeof(RangedInput).GetConstructor([typeof(IArrowArray), typeof(Range)])!,
            Expression.Property(listVar, nameof(ListArray.Values)),
            Expression.New(
                typeof(Range).GetConstructor([typeof(int), typeof(int)])!,
                startVar,
                endVar));

        // Call: ExecuteListWhere(ctx, rowSource, predicate, copier, builder, outerIdx0, ...)
        // List<Expression> execArgs = [_ctxParam, rowSource, predicate, copier, builderParam];
        // execArgs.AddRange(outerIdxExprs);
        // Expression rowCall = Expression.Call(null, execMethod, execArgs);

        // Loop body
        Expression rowBody = Expression.Block(
            [startVar, endVar],
            Expression.Assign(startVar, Expression.Call(null, readOff, offsets, rowVar)),
            Expression.Assign(endVar, Expression.Add(startVar, getLen)),
            null);

        // for (int r = 0; r < list.Length; r++)
        Expression loop = Expression.Loop(
            Expression.IfThenElse(
                Expression.LessThan(rowVar, Expression.Property(listVar, nameof(IArrowArray.Length))),
                Expression.Block(rowBody, Expression.PostIncrementAssign(rowVar)),
                Expression.Break(rowBreak)),
            rowBreak);

        Expression callBody = Expression.Block(
            [listVar, rowVar],
            Expression.Assign(listVar, listArray),
            Expression.Assign(rowVar, Expression.Constant(0)),
            loop);

        LambdaExpression callLambda = Expression.Lambda(callBody, builderParam);
        return TryExecuteWithBuilder(callLambda, resultInfo);
    }

    private List<(ParameterExpression, ParameterExpression)> SetupClosureParameters(LambdaExpression predicate)
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
                Type builderType = GetConcreteBuilderType(GetBufferType(elemType));
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
                    Expression scalar = AsScalar(operand, node.Operand);
                    Type targetType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
                    if (scalar.Type == targetType) return scalar;
                    return Expression.Convert(scalar, node.Type);
                }

                Expression asSpan = AsSpan(operand, node.Operand);
                Type elemType = SpanElementType(asSpan);
                Type resultUnderlyingType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
                if (elemType == resultUnderlyingType) return operand;
                MethodInfo convertMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ConvertLogical))!
                    .MakeGenericMethod(elemType, resultUnderlyingType);
                return Expression.Call(
                    null,
                    convertMethod,
                    [_ctxParam, asSpan]);
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
                    [_ctxParam, AsSpan(operand, node)]);
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

    private Type MakeInputTypeForArray(Type type, int index)
    {
        Debug.Assert(type.ImplementsInterface(typeof(IArrowArray)));

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
        else if (IsIndexedInput(input)) return InputType.Masked;

        throw new NotImplementedException();
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

    private static BlockExpression HideBuilderReturn(Expression expression)
    {
        Debug.Assert(expression.Type.Name.Contains("Builder"));
        return Expression.Block(expression, Expression.Empty());
    }

    private static void HideBuilderReturnGeneric<T>(T _)
    {
    }

    private static Expression AsSpan(Expression input, Expression original)
    {
        if (IsSpan(input)) return input;

        if (IsIdentityInput(input))
        {
            return AccessValues(AccessArray(input, original));
        }
        else if (IsRangedInput(input))
        {
            Expression span = AccessValues(AccessArray(input, original));
            return Expression.Call(
                input,
                nameof(RangedInput.Slice),
                [SpanElementType(span)],
                span);
        }
        else if (IsIndexedInput(input))
        {
            Expression span = AccessValues(AccessArray(input, original));
            return Expression.Call(
                input,
                nameof(IndexedInput.ValueAt),
                [SpanElementType(span)],
                span);
        }

        throw new InvalidOperationException();
    }

    private static Expression AsScalar(Expression input, Expression original)
    {
        if (IsIndexedInput(input))
        {
            Expression span = AccessValues(AccessArray(input, original));
            return Expression.Call(
                input,
                nameof(IndexedInput.ValueAt),
                [SpanElementType(span)],
                span);
        }

        if (original is ConstantExpression constExpr) return Expression.Constant(constExpr.Value, constExpr.Type);

        if (!IsSpan(input) && !IsInput(input))
            // Already a scalar (e.g., from a prior MakeUnary convert)
            return input;

        throw new InvalidOperationException($"Cannot convert to scalar: {input.Type.Name}");
    }

    private static Expression AccessValues(Expression array)
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

    private Expression ExecuteWithTempBuilder(
        LambdaExpression callLambda,
        ArrowTypeInfo resultInfo)
    {
        ParameterExpression builder = Expression.Variable(resultInfo.BuilderType, "tmpBuilder");
        MethodCallExpression init = MakeBuilderFor(resultInfo);
        return Expression.Block(
            [builder],
            Expression.Assign(builder, init),
            Expression.Invoke(callLambda, builder),
            BuildArray(builder));
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

    private Expression TryExecuteWithBuilder(
        LambdaExpression callLambda,
        ArrowTypeInfo resultInfo)
    {
        Expression? outer = _builderStack.Peek();
        if (outer is not null)
            return Expression.Invoke(callLambda, outer);
        else
            return ExecuteWithTempBuilder(callLambda, resultInfo);
    }

    private Expression ExecuteBinarySpan(BinaryExpression node, Expression left, Expression right)
    {
        Type leftElementType = SpanElementType(left);
        Type rightElementType = SpanElementType(right);

        // if not equal, convert both to bitmap
        if ((leftElementType == typeof(bool) && rightElementType == typeof(bool)) ||
            leftElementType != rightElementType)
        {
            if (leftElementType != typeof(bool))
                left = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapFromMask))!.CallStatic(
                    [leftElementType],
                    [_ctxParam, left]);

            if (rightElementType != typeof(bool))
                right = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapFromMask))!.CallStatic(
                    [rightElementType],
                    [_ctxParam, right]);

            return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapOps))!.CallStatic(
                [],
                [
                    _ctxParam,
                    left,
                    right,
                    node.NodeType.Quoted
                ]);
        }

        return typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.Zip))!.CallStatic(
            [leftElementType],
            [
                _ctxParam,
                left,
                right,
                node.NodeType.Quoted
            ]);
    }
}