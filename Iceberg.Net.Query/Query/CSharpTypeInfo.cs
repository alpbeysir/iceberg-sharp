using System.Collections.Frozen;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Iceberg.Net.Misc;

namespace Iceberg.Net.Query;

internal enum CSharpTypeKind
{
    Primitive,
    Map,
    List,
    Struct
}

internal record CSharpTypeInfo(string Path, Type Type)
{
    internal FrozenDictionary<string, CSharpTypeInfo> GetSubtypes()
    {
        Utils.IsNullable(Type, out Type unwrapped);
        Type[] genericArguments = unwrapped.GetGenericArguments();
        IEnumerable<CSharpTypeInfo> list = TypeKindFromType(unwrapped) switch
        {
            CSharpTypeKind.List => [new CSharpTypeInfo($"{Path}.element", genericArguments[0])],
            CSharpTypeKind.Map =>
            [
                new CSharpTypeInfo($"{Path}.key", genericArguments[0]),
                new CSharpTypeInfo($"{Path}.value", genericArguments[1])
            ],
            CSharpTypeKind.Primitive => [this],
            CSharpTypeKind.Struct => DeconstructStruct(Path, unwrapped),
            _ => throw new UnreachableException()
        };
        return list.ToFrozenDictionary(info => info.Path);
    }

    internal static CSharpTypeKind TypeKindFromType(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)]
        Type type)
    {
        if (type == typeof(string) || type == typeof(bool) || type == typeof(int) || type == typeof(long) ||
            type == typeof(float) || type == typeof(double) || type == typeof(Guid) || type == typeof(DateTime) ||
            type == typeof(DateOnly) || type == typeof(TimeOnly) || type == typeof(TimeSpan) ||
            type == typeof(byte[]) || type == typeof(decimal)) return CSharpTypeKind.Primitive;

        if (type.ImplementsInterface(typeof(IReadOnlyDictionary<,>))) return CSharpTypeKind.Map;

        if (type.ImplementsInterface(typeof(IReadOnlyList<>))) return CSharpTypeKind.List;

        if (type.IsClass || type is { IsValueType: true, IsPrimitive: false }) return CSharpTypeKind.Struct;

        throw new NotSupportedException($"Type {type.Name} is not supported.");
    }

    private static IEnumerable<CSharpTypeInfo> DeconstructStruct(string prefix, Type type)
    {
        Dictionary<string, MemberInfo> members = Utils.GetMembersByName(type);
        return members.Select(kvp => new CSharpTypeInfo($"{prefix}.{kvp.Key}", Utils.PropertyOrFieldType(kvp.Value)));
    }
}