using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Iceberg.Net.Misc;
using ZLinq;

namespace Iceberg.Net.Benchmarks;

public class DataLayouts
{
    private const int Size = 100000000;
    private double[] doubles;
    private long[] numbers;
    private string[] strings;

    [GlobalSetup]
    public void Setup()
    {
        var rand = new Random();
        numbers = ValueEnumerable.Range(0, Size).Select(_ => (long)rand.Next()).ToArray();
        strings = ValueEnumerable.Range(0, Size).Select(_ => Utils.RandomCreateString(10)).ToArray();
        doubles = ValueEnumerable.Range(0, Size).Select(_ => rand.NextDouble()).ToArray();
    }

    [Benchmark]
    public void ZLinq()
    {
        numbers.AsValueEnumerable().Sum();
        strings.AsValueEnumerable().Count(n => n[1] == 'z');
        doubles.AsValueEnumerable().Average();
        //numbers.AsValueEnumerable().Zip(strings).Zip(doubles).Select((tuple, i) => new Projection(i)).ToArray();
    }

    [Benchmark]
    public void ValueEnumerableStruct()
    {
        GetValueEnumerableStruct().Sum(row => row.Number);
        GetValueEnumerableStruct().Count(row => row.String[1] == 'z');
        GetValueEnumerableStruct().Average(row => row.Double);
        //GetValueEnumerableStruct().Select(row => new Projection(row.Number)).ToArray();
    }


    private ValueEnumerable<FromColumns<RowCursor>, RowCursor> GetValueEnumerableStruct()
    {
        return new ValueEnumerable<FromColumns<RowCursor>, RowCursor>(
            new FromColumns<RowCursor>(doubles, numbers, strings));
    }


    private record struct Projection(int Number);

    private struct RowCursor(Reader reader) : IBaseCursor<RowCursor>, IMySchema
    {
        public int Index { get; set; }
        public Reader Reader { get; set; } = reader;

        public static RowCursor Create(Reader reader)
        {
            return new RowCursor(reader);
        }

        public double Double => Reader.Access<double>(0, Index);
        public long Number => Reader.Access<long>(1, Index);
        public string String => Reader.AccessRef<string>(2, Index);
    }

    private interface IMySchema
    {
        public double Double { get; }
        public long Number { get; }
        public string String { get; }
    }

    private interface IBaseCursor<out T>
    {
        internal int Index { get; set; }
        internal Reader Reader { get; set; }

        static abstract T Create(Reader reader);
    }

    private class Reader
    {
        internal double[] DoubleBuf;
        internal bool[] Initialized;
        internal long[] NumBuf;
        internal string[] StringBuf;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal T Access<T>(int col, int index) where T : struct
        {
            if (!Initialized[col]) Initialized[col] = true;

            return col switch
            {
                0 => Unsafe.BitCast<double, T>(DoubleBuf[index]),
                1 => Unsafe.BitCast<long, T>(NumBuf[index]),
                _ => throw new ArgumentOutOfRangeException(nameof(col))
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal T AccessRef<T>(int col, int index) where T : class
        {
            if (!Initialized[col]) Initialized[col] = true;

            return col switch
            {
                2 => Unsafe.As<T>(StringBuf[index]),
                _ => throw new ArgumentOutOfRangeException(nameof(col))
            };
        }
    }

    private struct FromColumns<T> : IValueEnumerator<T> where T : IBaseCursor<T>
    {
        public void Dispose()
        {
        }

        private readonly T _cursor;
        private int _pos;
        private readonly int _len;
        private readonly Reader _reader;

        public FromColumns(
            double[] doubles,
            long[] numbers,
            string[] strings)
        {
            _reader = new Reader
            {
                DoubleBuf = doubles,
                NumBuf = numbers,
                StringBuf = strings,
                Initialized = new bool[3]
            };
            _cursor = T.Create(_reader);
            _len = numbers.Length;
        }

        public bool TryGetNext(out T current)
        {
            if (_pos < _len)
            {
                current = _cursor;
                current.Index = _pos;
                _pos++;
                return true;
            }

            Unsafe.SkipInit(out current);
            return false;
        }

        public bool TryGetNonEnumeratedCount(out int count)
        {
            count = _len;
            return true;
        }

        public bool TryGetSpan(out ReadOnlySpan<T> span)
        {
            Unsafe.SkipInit(out span);
            return false;
        }

        public bool TryCopyTo(scoped Span<T> destination, Index offset)
        {
            var pos = offset.GetOffset(_len);
            var spanPos = 0;
            if (pos >= _len) return false;
            while (pos < _len && spanPos < destination.Length)
            {
                destination[spanPos].Reader = _reader;
                destination[spanPos].Index = pos;
                pos++;
                spanPos++;
            }

            return true;
        }
    }
}