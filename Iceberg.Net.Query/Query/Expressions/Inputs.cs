using Apache.Arrow;

namespace Iceberg.Net.Query.Expressions;

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

    public int Length => Range.GetOffsetAndLength(Array.Length).Length;
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