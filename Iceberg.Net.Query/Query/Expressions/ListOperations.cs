using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Memory;
using DotNext.Linq.Expressions;
using DotNext.Metaprogramming;
using Iceberg.Net.Query.FastArrow;

namespace Iceberg.Net.Query.Expressions;

public static class ListOperations
{
    public static void ExecuteElementWiseListOp<TInput, TResultArray, TResultBuilder>(
        ExecutionContext ctx,
        TInput input,
        Action<ExecutionContext, RangedInput, TResultBuilder> op,
        TResultBuilder builder)
        where TResultBuilder : IArrowArrayBuilder<TResultArray, TResultBuilder>
        where TInput : IInput<TInput>
        where TResultArray : class, IArrowArray
    {
        builder.Reserve(input.Length);
        ListArray l = (ListArray)input.Array;
        ListArrayBuilder? asListBuilder = builder as ListArrayBuilder;

        if (typeof(TInput) == typeof(IdentityInput))
        {
            for (var i = 0; i < l.Length; i++)
            {
                asListBuilder?.Append();
                RangedInput subInput = ValueRangeForIndex(l, i);
                op(ctx, subInput, builder);
            }
        }
        else if (typeof(TInput) == typeof(RangedInput))
        {
            RangedInput ranged = Unsafe.As<TInput, RangedInput>(ref input);
            var (offset, length) = ranged.Range.GetOffsetAndLength(l.Length);
            for (var i = offset; i < offset + length; i++)
            {
                asListBuilder?.Append();
                RangedInput subInput = ValueRangeForIndex(l, i);
                op(ctx, subInput, builder);
            }
        }
        else if (typeof(TInput) == typeof(IndexedInput))
        {
            ref IndexedInput indexed = ref Unsafe.As<TInput, IndexedInput>(ref input);
            asListBuilder?.Append();
            RangedInput subInput = ValueRangeForIndex(l, indexed.Index);
            op(ctx, subInput, builder);
        }
        else
        {
            throw new InvalidOperationException($"Unsupported TInput: {typeof(TInput)}");
        }
    }

    public static Expression ExecuteOneToOneListOpGen(
        Expression input,
        Func<ParameterExpression, ParameterExpression, Expression> op,
        ParameterExpression builder,
        Type valueBuilderType)
    {
        return CodeGenerator.Lambda<Action>(_ =>
        {
            ParameterExpression valueBuilder = CodeGenerator.DeclareVariable(
                "valueBuilder",
                builder.Property("ValueBuilder").Convert(valueBuilderType));
            ParameterExpression l = CodeGenerator.DeclareVariable(
                "l",
                input.Property("Array").Convert<ListArray>());

            if (input.Type == typeof(IdentityInput))
            {
                CodeGenerator.Call(builder, nameof(IArrowArrayBuilder<,>.Reserve), l.Property("Length"));
                CodeGenerator.Call(
                    valueBuilder,
                    nameof(IArrowArrayBuilder<,>.Reserve),
                    l.Property("Values").Property("Length"));
                CodeGenerator.Call(
                    builder,
                    nameof(ListArrayBuilder.InitializeOffsetsFromList),
                    l,
                    0.Quoted,
                    l.Property("Length"));

                ParameterExpression subInput = CodeGenerator.DeclareVariable(
                    "subInput",
                    input.Call(nameof(IInput<>.Apply), l.Property("Values")));
                CodeGenerator.Statement(op(subInput, valueBuilder));
            }
            else if (input.Type == typeof(RangedInput))
            {
                ParameterExpression ranged = CodeGenerator.DeclareVariable(
                    "ranged",
                    input.Convert<RangedInput>());

                CodeGenerator.Call(
                    builder,
                    nameof(ListArrayBuilder.InitializeOffsetsFromRangedList),
                    l,
                    ranged.Property("Range"));

                ParameterExpression subInput = CodeGenerator.DeclareVariable(
                    "subInput",
                    typeof(ListOperations).CallStatic(
                        nameof(ValueRangeForRangedInput),
                        l,
                        ranged));

                CodeGenerator.Statement(op(subInput, valueBuilder));
            }
            else if (input.Type == typeof(IndexedInput))
            {
                ParameterExpression indexed = CodeGenerator.DeclareVariable(
                    "indexed",
                    input.Convert<IndexedInput>());

                CodeGenerator.Call(
                    builder,
                    nameof(ListArrayBuilder.PrepareIndexedListElement),
                    l,
                    indexed.Property("Index"));

                ParameterExpression subInput = CodeGenerator.DeclareVariable(
                    "subInput",
                    typeof(ListOperations).CallStatic(
                        nameof(ValueRangeForIndex),
                        l,
                        indexed.Property("Index")));

                CodeGenerator.Statement(op(subInput, valueBuilder));
            }
            else
            {
                throw new InvalidOperationException($"Unsupported TInput: {valueBuilderType}");
            }
        }).Body;
    }

    private static readonly ConstructorInfo s_maskBuilderCtor = typeof(BooleanArrayBuilder)
        .GetConstructor([typeof(MemoryAllocator)])!;

    private static readonly ConstructorInfo s_indexedInputCtor = typeof(IndexedInput)
        .GetConstructor([typeof(IArrowArray), typeof(int)])!;

    private static readonly MethodInfo s_booleanGetValue = typeof(BooleanArray)
        .GetMethod(nameof(BooleanArray.GetValue), [typeof(int)])!;

    private static readonly MethodInfo s_getValueRangeMethod = typeof(ListOperations)
        .GetMethod(nameof(GetValueRange), BindingFlags.Public | BindingFlags.Static)!;

    private static readonly MethodInfo s_getOffsetMethod = typeof(ListOperations)
        .GetMethod(nameof(GetOffset), BindingFlags.Public | BindingFlags.Static)!;

    private static readonly MethodInfo s_getLengthMethod = typeof(ListOperations)
        .GetMethod(nameof(GetLength), BindingFlags.Public | BindingFlags.Static)!;

    private static readonly MethodInfo s_readSpanMethod = typeof(ListOperations)
        .GetMethod(nameof(ReadSpan), BindingFlags.Public | BindingFlags.Static)!;

    private static readonly MethodInfo s_valueRangeForRangedMethod = typeof(ListOperations)
        .GetMethod(nameof(ValueRangeForRangedInput), BindingFlags.Public | BindingFlags.Static)!;

    private static Expression MaskValue(ParameterExpression mask, Expression index)
    {
        return Expression.Property(
            Expression.Call(mask, s_booleanGetValue, index), "Value");
    }

    private static Expression CopierInvoke(
        LambdaExpression copier, ParameterExpression ctx,
        Expression l, Expression valueIndex, ParameterExpression valueBuilder)
    {
        return Expression.Invoke(
            copier, ctx,
            Expression.New(s_indexedInputCtor, l.Property("Values"), valueIndex),
            valueBuilder);
    }

    /// <summary>Builds the nested copy loop: for each list element, for each value, check mask and invoke copier.</summary>
    private static Expression BuildCopyLoop(
        Expression l, ParameterExpression mask,
        Expression listStart, Expression listCount, Expression maskBase,
        ParameterExpression valueBuilder, LambdaExpression copier, ParameterExpression ctx,
        ParameterExpression builder)
    {
        ParameterExpression iVar = Expression.Variable(typeof(int), "i");
        ParameterExpression jVar = Expression.Variable(typeof(int), "j");
        ParameterExpression vrVar = Expression.Variable(typeof(ValueTuple<int, int>), "vr");

        Expression innerBody = Expression.IfThen(
            MaskValue(mask, Expression.Subtract(jVar, maskBase)),
            CopierInvoke(copier, ctx, l, jVar, valueBuilder));

        Expression innerLoop = ExpressionUtilities.ForExpression(
            jVar,
            Expression.Field(vrVar, "Item1"),
            Expression.LessThan(jVar, Expression.Field(vrVar, "Item2")),
            Expression.PostIncrementAssign(jVar),
            innerBody);

        Expression outerBody = Expression.Block(
            [vrVar],
            builder.Call(nameof(ListArrayBuilder.Append)),
            Expression.Assign(vrVar, Expression.Call(null, s_getValueRangeMethod, l, iVar)),
            innerLoop);

        return ExpressionUtilities.ForExpression(
            iVar,
            listStart,
            Expression.LessThan(iVar, Expression.Add(listStart, listCount)),
            Expression.PostIncrementAssign(iVar),
            outerBody);
    }

    /// <summary>Builds the predicate loop for IndexedInput: for each value in the element, call predicateFactory.</summary>
    private static Expression BuildIndexedPredicateLoop(
        Expression l, Expression valueRange,
        Func<ParameterExpression, ParameterExpression, Expression> predicateFactory,
        ParameterExpression maskBuilder)
    {
        ParameterExpression jVar = Expression.Variable(typeof(int), "j");
        ParameterExpression elVar = Expression.Variable(typeof(IndexedInput), "el");

        Expression predBody = Expression.Block(
            Expression.Assign(
                elVar,
                Expression.New(s_indexedInputCtor, l.Property("Values"), jVar)),
            predicateFactory(elVar, maskBuilder));

        return Expression.Block(
            [elVar],
            ExpressionUtilities.ForExpression(
                jVar,
                Expression.Field(valueRange, "Item1"),
                Expression.LessThan(jVar, Expression.Field(valueRange, "Item2")),
                Expression.PostIncrementAssign(jVar),
                predBody));
    }

    /// <summary>Builds the copy loop for IndexedInput: for each value 0..(end-start), check mask and invoke copier.</summary>
    private static Expression BuildIndexedCopyLoop(
        Expression l, ParameterExpression mask, Expression valueRange,
        ParameterExpression valueBuilder, LambdaExpression copier, ParameterExpression ctx)
    {
        ParameterExpression jVar = Expression.Variable(typeof(int), "j");

        Expression copyBody = Expression.IfThen(
            MaskValue(mask, jVar),
            CopierInvoke(
                copier, ctx, l,
                Expression.Add(Expression.Field(valueRange, "Item1"), jVar),
                valueBuilder));

        return ExpressionUtilities.ForExpression(
            jVar,
            Expression.Constant(0),
            Expression.LessThan(
                jVar,
                Expression.Subtract(
                    Expression.Field(valueRange, "Item2"),
                    Expression.Field(valueRange, "Item1"))),
            Expression.PostIncrementAssign(jVar),
            copyBody);
    }

    public static Expression ExecuteListWhereGen(
        ParameterExpression ctx,
        Expression input,
        Func<ParameterExpression, ParameterExpression, Expression> predicateFactory,
        LambdaExpression copier,
        ParameterExpression builder,
        Type valueBuilderType)
    {
        return CodeGenerator.Lambda<Action>(_ =>
        {
            ParameterExpression valueBuilder = CodeGenerator.DeclareVariable(
                "valueBuilder",
                builder.Property("ValueBuilder").Convert(valueBuilderType));
            var lVar = CodeGenerator.DeclareVariable(
                "l",
                input.Property("Array").Convert<ListArray>());
            var l = lVar.AsUsable<ListArray>();

            if (input.Type == typeof(IdentityInput))
            {
                ParameterExpression maskBuilder = CodeGenerator.DeclareVariable(
                    "maskBuilder",
                    Expression.New(s_maskBuilderCtor, ctx.Property("ArrowAllocator")));
                ParameterExpression whereInput = CodeGenerator.DeclareVariable(
                    "whereInput",
                    input.Call(nameof(IInput<>.Apply), lVar.Property("Values")));
                CodeGenerator.Statement(predicateFactory(whereInput, maskBuilder));
                ParameterExpression mask = CodeGenerator.DeclareVariable(
                    "mask",
                    maskBuilder.Call(
                        nameof(BooleanArrayBuilder.Build),
                        ctx.Property("ArrowAllocator")));

                CodeGenerator.Statement(
                    BuildCopyLoop(lVar, mask, 0.Quoted, lVar.Property("Length"), 0.Quoted,
                        valueBuilder, copier, ctx, builder));
                CodeGenerator.Call(mask, nameof(IDisposable.Dispose));
            }
            else if (input.Type == typeof(RangedInput))
            {
                ParameterExpression maskBuilder = CodeGenerator.DeclareVariable(
                    "maskBuilder",
                    Expression.New(s_maskBuilderCtor, ctx.Property("ArrowAllocator")));
                ParameterExpression whereInput = CodeGenerator.DeclareVariable(
                    "whereInput",
                    ExpressionUtilities.Use(() =>
                        ValueRangeForRangedInput(l, input.As<RangedInput>())));
                CodeGenerator.Statement(predicateFactory(whereInput, maskBuilder));
                ParameterExpression mask = CodeGenerator.DeclareVariable(
                    "mask",
                    maskBuilder.Call(
                        nameof(BooleanArrayBuilder.Build),
                        ctx.Property("ArrowAllocator")));

                ParameterExpression offsetAndLength = CodeGenerator.DeclareVariable(
                    "offsetAndLength",
                    input.Property("Range").Call(
                        nameof(Range.GetOffsetAndLength),
                        lVar.Property("Length")));
                ParameterExpression valStart = CodeGenerator.DeclareVariable(
                    "valStart",
                    lVar.Property("ValueOffsets").Call(
                        "get_Item",
                        offsetAndLength.Property("Item1")));

                CodeGenerator.Statement(
                    BuildCopyLoop(lVar, mask, offsetAndLength.Property("Item1"),
                        offsetAndLength.Property("Item2"), valStart,
                        valueBuilder, copier, ctx, builder));
                CodeGenerator.Call(mask, nameof(IDisposable.Dispose));
            }
            else if (input.Type == typeof(IndexedInput))
            {
                ParameterExpression indexed = CodeGenerator.DeclareVariable(
                    "indexed",
                    input.Convert<IndexedInput>());

                ParameterExpression maskBuilder = CodeGenerator.DeclareVariable(
                    "maskBuilder",
                    Expression.New(s_maskBuilderCtor, ctx.Property("ArrowAllocator")));
                ParameterExpression valueRange = CodeGenerator.DeclareVariable(
                    "valueRange",
                    Expression.Call(null, s_getValueRangeMethod, lVar, indexed.Property("Index")));

                CodeGenerator.Statement(
                    BuildIndexedPredicateLoop(lVar, valueRange, predicateFactory, maskBuilder));

                ParameterExpression mask = CodeGenerator.DeclareVariable(
                    "mask",
                    maskBuilder.Call(
                        nameof(BooleanArrayBuilder.Build),
                        ctx.Property("ArrowAllocator")));
                CodeGenerator.Call(builder, nameof(ListArrayBuilder.Append));

                CodeGenerator.Statement(
                    BuildIndexedCopyLoop(lVar, mask, valueRange, valueBuilder, copier, ctx));
                CodeGenerator.Call(mask, nameof(IDisposable.Dispose));
            }
            else
            {
                throw new InvalidOperationException($"Unsupported input type: {input.Type}");
            }
        }).Body;
    }

    public static void ExecuteOneToOneListOp<TInput, TValueInput, TValueBuilder>(
        ExecutionContext ctx,
        TInput input,
        Action<ExecutionContext, TValueInput, TValueBuilder> op,
        ListArrayBuilder builder)
        where TInput : IInput<TInput>
        where TValueInput : IInput<TValueInput>
        where TValueBuilder : class, IArrowArrayBuilder
    {
        TValueBuilder valueBuilder = (TValueBuilder)builder.ValueBuilder;
        ListArray l = (ListArray)input.Array;

        if (typeof(TInput) == typeof(IdentityInput))
        {
            builder.Reserve(l.Length);
            builder.ValueBuilder.Reserve(l.Values.Length);
            builder.InitializeOffsetsFromList(l, 0, l.Length);

            TInput subInput = input.Apply(l.Values);
            op(ctx, Unsafe.As<TInput, TValueInput>(ref subInput), valueBuilder);
        }
        else if (typeof(TInput) == typeof(RangedInput))
        {
            RangedInput ranged = Unsafe.As<TInput, RangedInput>(ref input);
            var (offset, length) = ranged.Range.GetOffsetAndLength(l.Length);

            var start = l.ValueOffsets[offset];
            var end = l.ValueOffsets[offset + length];

            builder.ValueBuilder.Reserve(end - start);
            builder.InitializeOffsetsFromList(l, offset, length);

            RangedInput subInput = new(l.Values, new Range(start, end));
            op(ctx, Unsafe.As<RangedInput, TValueInput>(ref subInput), valueBuilder);
        }
        else if (typeof(TInput) == typeof(IndexedInput))
        {
            IndexedInput indexed = Unsafe.As<TInput, IndexedInput>(ref input);
            var start = l.ValueOffsets[indexed.Index];
            var end = start + l.GetValueLength(indexed.Index);

            builder.Append();
            builder.ValueBuilder.Reserve(end - start);

            RangedInput subInput = new(l.Values, new Range(start, end));
            op(ctx, Unsafe.As<RangedInput, TValueInput>(ref subInput), valueBuilder);
        }
        else
        {
            throw new InvalidOperationException($"Unsupported TInput: {typeof(TInput)}");
        }
    }

    public static void ExecuteListWhere<TInput, TValueBuilder>(
        ExecutionContext ctx,
        TInput input,
        Action<ExecutionContext, TInput, BooleanArrayBuilder> op,
        Action<ExecutionContext, IndexedInput, TValueBuilder> copier,
        ListArrayBuilder builder)
        where TInput : IInput<TInput>
        where TValueBuilder : class, IArrowArrayBuilder
    {
        TValueBuilder valueBuilder = (TValueBuilder)builder.ValueBuilder;
        ListArray l = (ListArray)input.Array;
        BooleanArrayBuilder maskBuilder = new(ctx.ArrowAllocator);

        if (typeof(TInput) == typeof(IdentityInput))
        {
            TInput subInput = input.Apply(l.Values);
            op(ctx, subInput, maskBuilder);
        }
        else if (typeof(TInput) == typeof(RangedInput))
        {
            RangedInput ranged = Unsafe.As<TInput, RangedInput>(ref input);
            RangedInput subInput = ValueRangeForRangedInput(l, ranged);
            op(ctx, Unsafe.As<RangedInput, TInput>(ref subInput), maskBuilder);
        }
        else if (typeof(TInput) == typeof(IndexedInput))
        {
            IndexedInput indexed = Unsafe.As<TInput, IndexedInput>(ref input);
            var (start, end) = GetValueRange(l, indexed.Index);
            for (var j = start; j < end; j++)
            {
                IndexedInput el = new(l.Values, j);
                op(ctx, Unsafe.As<IndexedInput, TInput>(ref el), maskBuilder);
            }
        }
        else
        {
            throw new InvalidOperationException($"Unsupported TInput: {typeof(TInput)}");
        }

        using BooleanArray mask = maskBuilder.Build(ctx.ArrowAllocator);

        if (typeof(TInput) == typeof(IdentityInput))
        {
            CopyWhereElements(ctx, l, 0, l.Length, mask, 0, copier, builder, valueBuilder);
        }
        else if (typeof(TInput) == typeof(RangedInput))
        {
            RangedInput ranged = Unsafe.As<TInput, RangedInput>(ref input);
            var (offset, length) = ranged.Range.GetOffsetAndLength(l.Length);
            var valStart = l.ValueOffsets[offset];
            CopyWhereElements(ctx, l, offset, length, mask, valStart, copier, builder, valueBuilder);
        }
        else if (typeof(TInput) == typeof(IndexedInput))
        {
            IndexedInput indexed = Unsafe.As<TInput, IndexedInput>(ref input);
            var (start, end) = GetValueRange(l, indexed.Index);
            CopyWhereElements(ctx, l, indexed.Index, 1, mask, start, copier, builder, valueBuilder);
        }
        else
        {
            throw new InvalidOperationException($"Unsupported TInput: {typeof(TInput)}");
        }
    }

    private static void CopyWhereElements<TValueBuilder>(
        ExecutionContext ctx,
        ListArray l,
        int listOffset,
        int listLength,
        BooleanArray mask,
        int maskBaseOffset,
        Action<ExecutionContext, IndexedInput, TValueBuilder> copier,
        ListArrayBuilder builder,
        TValueBuilder valueBuilder)
        where TValueBuilder : class, IArrowArrayBuilder
    {
        for (var i = listOffset; i < listOffset + listLength; i++)
        {
            builder.Append();
            var (start, end) = GetValueRange(l, i);
            for (var j = start; j < end; j++)
            {
                if (!mask.GetValue(j - maskBaseOffset)!.Value)
                    continue;
                copier(ctx, new IndexedInput(l.Values, j), valueBuilder);
            }
        }
    }

    public static (int start, int end) GetValueRange(ListArray l, int index)
    {
        var start = l.ValueOffsets[index];
        return (start, start + l.GetValueLength(index));
    }

    public static RangedInput ValueRangeForIndex(ListArray l, int index)
    {
        var (start, end) = GetValueRange(l, index);
        return new RangedInput(l.Values, new Range(start, end));
    }

    public static RangedInput ValueRangeForRangedInput(ListArray l, RangedInput ranged)
    {
        var (offset, length) = ranged.Range.GetOffsetAndLength(l.Length);
        var start = l.ValueOffsets[offset];
        var end = l.ValueOffsets[offset + length];
        return new RangedInput(l.Values, new Range(start, end));
    }

    public static int GetOffset((int offset, int length) ol)
    {
        return ol.offset;
    }

    public static int GetLength((int offset, int length) ol)
    {
        return ol.length;
    }

    public static T ReadSpan<T>(ReadOnlySpan<T> span, int index) where T : unmanaged
    {
        return span[index];
    }
}