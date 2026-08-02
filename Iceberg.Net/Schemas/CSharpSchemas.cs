using System.Collections.Concurrent;
using System.Data.SqlTypes;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Reflection.Emit;
using Iceberg.Net.Misc;

namespace Iceberg.Net.Schemas;

public static class CSharpSchemas
{
    private static readonly NullabilityInfoContext NullabilityInfoContext = new();
    private static readonly ConcurrentDictionary<IIcebergType, Type> TypeCache = new(new IcebergTypeComparer());

    private static ModuleBuilder ModuleBuilder => AssemblyBuilder
        .DefineDynamicAssembly(new AssemblyName("Iceberg.Generated"), AssemblyBuilderAccess.Run)
        .DefineDynamicModule("MainModule");

    [RequiresDynamicCode("This method requires creating new types at runtime.")]
    private static Type FromIcebergSchema(Schema schema)
    {
        return FromIcebergType(schema, "IcebergRow", true);
    }

    private static Type FromIcebergStruct(StructType structType, string typeName)
    {
        // Check cache first to avoid redundant Reflection.Emit calls
        return TypeCache.GetOrAdd(
            structType,
            _ =>
            {
                TypeBuilder typeBuilder = ModuleBuilder.DefineType(
                    $"{typeName}_{Guid.NewGuid():N}",
                    TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.BeforeFieldInit);

                foreach (StructField field in structType.Fields)
                {
                    Type fieldType = FromIcebergType(field.FieldType, field.Name, field.Required);

                    if (!field.Required && fieldType.IsValueType && Nullable.GetUnderlyingType(fieldType) == null)
                        fieldType = typeof(Nullable<>).MakeGenericType(fieldType);

                    typeBuilder.DefineField(field.Name, fieldType, FieldAttributes.Public);
                }

                return typeBuilder.CreateType()!;
            });
    }

    [RequiresDynamicCode("Calls System.Type.MakeGenericType(params Type[])")]
    private static Type FromIcebergType(IIcebergType icebergType, string path, bool required)
    {
        if (icebergType is PrimitiveType primitiveType) icebergType = PrimitiveType.Parse(primitiveType.Name);
        Type initialType = icebergType switch
        {
            PrimitiveType.Boolean => typeof(bool),
            PrimitiveType.Int => typeof(int),
            PrimitiveType.Long => typeof(long),
            PrimitiveType.Float => typeof(float),
            PrimitiveType.Double => typeof(double),
            PrimitiveType.String => typeof(string),
            PrimitiveType.Uuid => typeof(Guid),
            PrimitiveType.Date => typeof(DateOnly),
            PrimitiveType.Time => typeof(TimeOnly),
            PrimitiveType.Timestamp => typeof(DateTimeOffset),
            PrimitiveType.Binary => typeof(byte[]),
            PrimitiveType.Decimal => typeof(SqlDecimal),

            ListType listType =>
                typeof(List<>).MakeGenericType(
                    FromIcebergType(listType.Element, $"{path}.element", listType.ElementRequired)),

            MapType mapType => typeof(Dictionary<,>).MakeGenericType(
                FromIcebergType(mapType.Key, $"{path}.key", true),
                FromIcebergType(mapType.Value, $"{path}.value", mapType.ValueRequired)),

            StructType structType => FromIcebergStruct(structType, path),

            _ => throw new NotSupportedException($"Iceberg type {icebergType.GetType().Name} is not supported.")
        };
        if (required) return initialType;
        else
            return initialType switch
            {
                { IsValueType: true } => typeof(Nullable<>).MakeGenericType(initialType),
                { IsValueType: false } => initialType
            };
    }

    public static Schema ToIcebergSchema(
        Type type,
        int? schemaId,
        Func<string, int> fieldIdProvider)
    {
        StructType structType = ToIcebergStruct(type, fieldIdProvider);
        return new Schema(structType.Fields, schemaId);
    }

    private static StructType
        ToIcebergStruct(
            Type type,
            Func<string, int> fieldIdProvider,
            string pathPrefix = "")
    {
        List<MemberInfo> members = type.GetMembers(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m is PropertyInfo or FieldInfo)
            .ToList();

        IEnumerable<(Type type, string name, string path, int id, MemberInfo memberInfo)> infos = from member in members
            let memberType = Utils.PropertyOrFieldType(member)
            let memberName = member.Name
            let fullPath = string.IsNullOrEmpty(pathPrefix) ? memberName : $"{pathPrefix}.{memberName}"
            let fieldId = fieldIdProvider(fullPath)
            select (type: memberType, name: memberName, path: fullPath, id: fieldId, memberInfo: member);

        IEnumerable<StructField> fields = from info in infos
            let nullabilityInfo = info.memberInfo switch
            {
                PropertyInfo p => NullabilityInfoContext.Create(p),
                FieldInfo f => NullabilityInfoContext.Create(f),
                _ => throw new NotSupportedException("Only fields and properties are supported.")
            }
            // Check if it's Nullable<T> (ValueType) or marked as nullable reference type
            let isOptional = nullabilityInfo.WriteState == NullabilityState.Nullable

            // For Iceberg, we need the actual data type (e.g., int for int?)
            let underlyingType = Nullable.GetUnderlyingType(info.type) ?? info.type
            let icebergType = ToIcebergType(info.memberInfo, underlyingType, fieldIdProvider, info.path)

            // Iceberg 'Required' is the inverse of 'isOptional'
            select new StructField(info.id, info.name, icebergType, !isOptional);

        return new StructType(fields.ToList());
    }

    public static IIcebergType ToIcebergType(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)]
        Type type,
        Func<string, int> fieldIdProvider, string currentPath)
    {
        if (type == typeof(string)) return new PrimitiveType.String();
        if (type == typeof(bool)) return new PrimitiveType.Boolean();
        if (type == typeof(int)) return new PrimitiveType.Int();
        if (type == typeof(long)) return new PrimitiveType.Long();
        if (type == typeof(float)) return new PrimitiveType.Float();
        if (type == typeof(double)) return new PrimitiveType.Double();
        if (type == typeof(Guid)) return new PrimitiveType.Uuid();
        if (type == typeof(DateTime)) return new PrimitiveType.Timestamp();
        if (type == typeof(DateOnly)) return new PrimitiveType.Date();
        // TODO timestamp stuff is probably wrong
        if (type == typeof(TimeOnly)) return new PrimitiveType.Time();
        if (type == typeof(TimeSpan)) return new PrimitiveType.Time();
        if (type == typeof(byte[])) return new PrimitiveType.Binary();
        if (type == typeof(decimal))
            throw new NotSupportedException(
                "CLR decimal is not supported. Use SqlDecimal with [DecimalWith(precision, scale)].");
        if (type == typeof(SqlDecimal))
            throw new InvalidOperationException(
                "SqlDecimal requires [DecimalWith(precision, scale)] when it is used in an Iceberg schema.");

        if (type.ImplementsInterface(typeof(IReadOnlyDictionary<,>)))
        {
            Type[] genericArguments = type.GetGenericArguments();
            Type keyType = genericArguments[0];
            Type valueType = genericArguments[1];
            Type? maybeUnderlyingType = Nullable.GetUnderlyingType(valueType);
            bool valueRequired = maybeUnderlyingType == null;
            int keyFieldId = fieldIdProvider($"{currentPath}.key");
            int valueFieldId = fieldIdProvider($"{currentPath}.value");
            return new MapType(
                keyFieldId,
                ToIcebergType(keyType, fieldIdProvider, currentPath),
                valueFieldId,
                ToIcebergType(valueRequired ? valueType : maybeUnderlyingType!, fieldIdProvider, currentPath),
                valueRequired);
        }

        if (type.ImplementsInterface(typeof(IReadOnlyList<>)))
        {
            Type? elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();
            if (elementType != null)
            {
                int elementId = fieldIdProvider($"{currentPath}.element");
                Type? maybeUnderlyingType = Nullable.GetUnderlyingType(elementType);
                bool required = maybeUnderlyingType == null;
                return new ListType(
                    elementId,
                    ToIcebergType(
                        required ? elementType : maybeUnderlyingType!,
                        fieldIdProvider,
                        $"{currentPath}.element"),
                    required);
            }
        }

        if (IsEnumerableType(type) && type != typeof(string))
        {
            Type? elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();
            if (elementType != null)
            {
                int elementId = fieldIdProvider($"{currentPath}.element");
                Type? maybeUnderlyingType = Nullable.GetUnderlyingType(elementType);
                bool required = maybeUnderlyingType == null;
                return new ListType(
                    elementId,
                    ToIcebergType(
                        required ? elementType : maybeUnderlyingType!,
                        fieldIdProvider,
                        $"{currentPath}.element"),
                    required);
            }
        }

        if (type.IsClass || type is { IsValueType: true, IsPrimitive: false })
            return ToIcebergStruct(type, fieldIdProvider, currentPath);

        throw new NotSupportedException($"Type {type.Name} at {currentPath} is not supported.");
    }

    private static IIcebergType ToIcebergType(
        MemberInfo member,
        Type type,
        Func<string, int> fieldIdProvider,
        string currentPath)
    {
        Apache.Arrow.Serialization.DecimalWithAttribute? decimalWith =
            member.GetCustomAttribute<Apache.Arrow.Serialization.DecimalWithAttribute>();
        if (decimalWith is null)
            return ToIcebergType(type, fieldIdProvider, currentPath);

        if (type != typeof(SqlDecimal))
            throw new InvalidOperationException(
                $"[DecimalWith] can only be used on SqlDecimal members, but {member.Name} is {type.Name}.");

        return new PrimitiveType.Decimal(decimalWith.Precision, decimalWith.Scale);
    }

    private static bool IsEnumerableType(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)]
        Type type)
    {
        if (type == typeof(string)) return false;
        if (type.IsArray) return true;
        if (type.ImplementsInterface(typeof(IEnumerable<>))) return true;
        return type is { IsInterface: true, IsGenericType: true }
               && type.GetGenericTypeDefinition() == typeof(IEnumerable<>);
    }
}