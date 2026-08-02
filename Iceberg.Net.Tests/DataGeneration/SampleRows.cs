using Apache.Arrow.Serialization;
using System.Data.SqlTypes;

namespace Iceberg.Net.Tests.DataGeneration;

[ArrowSerializable]
public partial record MySimpleRow
{
    public required List<int> Arr { get; init; }
    public required int Num { get; init; }
    public required string Str { get; init; }
}

[ArrowSerializable]
public partial record MyComplexRow
{
    public required List<int> MyArray { get; init; }
    public required MyNested MyNested { get; init; }
    public required int MyNumber { get; init; }
    public required string MyString { get; init; }
}

[ArrowSerializable]
public partial record MyNested
{
    public required double MyDouble { get; init; }
    public required Dictionary<int, string> MyMap { get; init; }
}

[ArrowSerializable]
public partial record NestedComplexRow
{
    public required int? c_int { get; init; }

    //public required List<Dictionary<int, BasicStruct?>?>? deep_list_map_struct { get; init; }
    // public required Dictionary<string, List<BasicStruct?>?>? deep_map_list_struct { get; init; }
    public required StructComplex? deep_struct_complex { get; init; }
    public required List<List<int>?>? list_of_list { get; init; }

    public required List<Dictionary<string, int>?>? list_of_map { get; init; }

    // public required List<BasicStruct?>? list_of_struct { get; init; }
    public required Dictionary<string, List<int>?>? map_of_list { get; init; }

    public required Dictionary<string, Dictionary<string, string?>?>? map_of_map { get; init; }

    // public required Dictionary<string, BasicStruct?>? map_of_struct { get; init; }
    public required StructOfList? struct_of_list { get; init; }
    public required StructOfMap? struct_of_map { get; init; }
    public required StructOfStruct? struct_of_struct { get; init; }
    public required List<int>? top_list { get; init; }
    public required Dictionary<string, double>? top_map { get; init; }
    public required BasicStruct? top_struct { get; init; }
}

[ArrowSerializable]
public partial record BasicStruct
{
    public int id { get; init; }
    public string? name { get; init; }
}

[ArrowSerializable]
public partial record StructComplex
{
    public required List<string?>? nested_list { get; init; }
    public required Dictionary<string, long>? nested_map { get; init; }
}

[ArrowSerializable]
public partial record StructOfList
{
    public required List<int>? inner_list { get; init; }
}

[ArrowSerializable]
public partial record StructOfMap
{
    public required Dictionary<string, int>? inner_map { get; init; }
}

[ArrowSerializable]
public partial record StructOfStruct
{
    public required BasicStruct? inner_struct { get; init; }
}

[ArrowSerializable]
public partial record ManyTypes
{
    public required byte[]? c_binary { get; init; }
    public required bool? c_bool { get; init; }
    public required DateOnly? c_date { get; init; }
    [DecimalWith(14, 2)]
    public required SqlDecimal? c_decimal_14_2 { get; init; }
    [DecimalWith(21, 2)]
    public required SqlDecimal? c_decimal_21_2 { get; init; }
    [DecimalWith(7, 2)]
    public required SqlDecimal? c_decimal_7_2 { get; init; }
    public required double? c_double { get; init; }
    public required byte[]? c_fixed_6 { get; init; }
    public required int? c_int { get; init; }
    public required long? c_long { get; init; }
    public required Dictionary<int, double>? c_map_double { get; init; }
    public required Dictionary<int, List<string?>?>? c_map_list_str { get; init; }
    public required string? c_string { get; init; }
    public required CStruct? c_struct { get; init; }
    public required DateTime? c_timestamp { get; init; }
    public required DateTime? c_timestamptz { get; init; }
    public required byte[]? c_uuid { get; init; }
}

[ArrowSerializable]
public partial record CStruct
{
    public required List<int>? list_int { get; init; }
    public required List<long>? list_long { get; init; }
}

// [ArrowSerializable]
// public readonly partial record struct MyDeeplyNestedComplexRow
// {
//     public required List<
//         Dictionary<
//             Dictionary<int, int>,
//             List<string>?
//         >?
//     >? complex_col { get; init; }
// }
