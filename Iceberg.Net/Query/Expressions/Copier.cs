using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using Iceberg.Net.Query.FastArrow;

namespace Iceberg.Net.Query.Expressions;

public static class Copier
{
    private static LambdaExpression GeneratePrimitiveCopy<TInput, TArray, TValue, TBuilder>()
        where TInput : IInput<TInput>
        where TArray : PrimitiveArray<TValue>
        where TBuilder : PrimitiveArrayBuilder<TValue, TArray, TBuilder>
        where TValue : struct, IEquatable<TValue>
    {
        LambdaExpression expr = (ExecutionContext ctx, TInput input, TBuilder builder) =>
            CopyPrimitive<TInput, TArray, TValue, TBuilder>(ctx, input, builder);
        return expr.WithName("PrimitiveCopy");
    }

    private static LambdaExpression GenerateListCopy<TInput, TValueBuilder>(LambdaExpression valueCopier)
        where TInput : IInput<TInput>
        where TValueBuilder : class, IArrowArrayBuilder
    {
        Action<ExecutionContext, TInput, TValueBuilder> compiled =
            (Action<ExecutionContext, TInput, TValueBuilder>)valueCopier.Compile();
        LambdaExpression expr = (ExecutionContext ctx, TInput input, ListArrayBuilder builder) =>
            CopyList(
                ctx,
                input,
                builder,
                compiled);
        return expr.WithName("ListCopy");
    }

    public static LambdaExpression GenerateCopy(Type inputType, ArrowTypeInfo typeInfo)
    {
        IArrowType arrowType = typeInfo.ArrowType;
        switch (arrowType)
        {
            case ListType listType:
            {
                ArrowTypeInfo elementTypeInfo = ArrowTypeUtils.ForArrowType(listType.ValueDataType);
                LambdaExpression valueCopier = GenerateCopy(inputType, elementTypeInfo);
                MethodInfo method = typeof(Copier).GetMethod(
                    nameof(GenerateListCopy),
                    BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(
                    inputType,
                    valueCopier.Parameters[^1].Type
                );
                return (LambdaExpression)method.Invoke(null, [valueCopier])!;
            }
            case FixedWidthType:
            {
                MethodInfo method = typeof(Copier).GetMethod(
                    nameof(GeneratePrimitiveCopy),
                    BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(
                    inputType,
                    typeInfo.ArrayType,
                    typeInfo.CSharpType,
                    typeInfo.BuilderType
                );
                return (LambdaExpression)method.Invoke(null, [])!;
            }
            default:
                throw new NotImplementedException();
        }
    }

    private static void CopyPrimitive<TInput, TArray, TValue, TBuilder>(
        ExecutionContext ctx,
        TInput input,
        TBuilder builder)
        where TInput : IInput<TInput>
        where TArray : PrimitiveArray<TValue>
        where TBuilder : PrimitiveArrayBuilder<TValue, TArray, TBuilder>
        where TValue : struct, IEquatable<TValue>
    {
        TArray array = (TArray)input.Array;

        switch (input)
        {
            case IndexedInput indexed:
                builder.Append(array.Values[indexed.Index]);
                break;
            case IdentityInput:
                builder.Append(array.Values);
                break;
            default:
                throw new InvalidOperationException($"Unsupported input type: {typeof(TInput)}");
        }
    }

    private static void CopyList<TInput, TValueBuilder>(
        ExecutionContext ctx,
        TInput input,
        ListArrayBuilder builder,
        Action<ExecutionContext, TInput, TValueBuilder> valueCopier)
        where TInput : IInput<TInput>
        where TValueBuilder : class, IArrowArrayBuilder
    {
        // TODO switch by input type
        Debug.Assert(typeof(TInput) == typeof(IdentityInput));

        TValueBuilder valueBuilder = (TValueBuilder)builder.ValueBuilder;
        ListArray l = (ListArray)input.Array;
        builder.InitializeOffsetsFromList(l, 0, input.Length);
        TInput subInput = input.Apply(l.Values);
        valueCopier(ctx, subInput, valueBuilder);
    }
}