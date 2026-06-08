namespace Iceberg.Net.Tests;

public static class TestData
{
    // 1. Configurable static variable for size
    private static int RowCount => 256;

    // 2. Deterministic generator method
    public static List<TestRow> GenerateRows()
    {
        // Using a fixed seed guarantees determinism across runs
        Random rand = new(42);
        List<TestRow> list = new(RowCount);

        for (var i = 0; i < RowCount; i++)
            list.Add(
                new TestRow
                {
                    A = rand.Next() % 1000,
                    B = rand.NextDouble() * 1000,
                    L = Enumerable.Range(0, rand.Next() % 10).Select(_ => rand.Next() % 1000).ToList(),
                    N = new TestNested { C = rand.Next() % 10000 },
                    LNest = Enumerable.Range(0, rand.Next() % 32)
                        .Select(_ => Enumerable.Range(0, rand.Next() % 32).ToList()).ToList()
                });

        return list;
    }
}