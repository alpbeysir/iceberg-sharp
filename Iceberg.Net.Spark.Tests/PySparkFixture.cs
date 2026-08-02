using System.Diagnostics;
using System.Globalization;
using Iceberg.Net.Catalog;
using Python.Runtime;

namespace Iceberg.Net.Spark.Tests;

public sealed class PySparkFixture : IAsyncLifetime
{
    private const string SparkPackages =
        "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0," +
        "org.apache.iceberg:iceberg-aws-bundle:1.11.0";

    private readonly SemaphoreSlim _gate = new(1, 1);
    private PyObject? _spark;

    public async ValueTask InitializeAsync()
    {
        string projectDirectory = Path.Combine(AppContext.BaseDirectory, "Python");
        await RunProcess(
            Environment.GetEnvironmentVariable("UV") ?? "uv",
            ["sync", "--frozen", "--project", projectDirectory]);

        string pythonExecutable = OperatingSystem.IsWindows()
            ? Path.Combine(projectDirectory, ".venv", "Scripts", "python.exe")
            : Path.Combine(projectDirectory, ".venv", "bin", "python");
        PythonConfiguration configuration = ReadPythonConfiguration(projectDirectory);

        Runtime.PythonDLL = configuration.Library;
        PythonEngine.PythonHome = configuration.Home;
        PythonEngine.PythonPath = string.Join(Path.PathSeparator, configuration.Paths);
        Environment.SetEnvironmentVariable("PYSPARK_PYTHON", pythonExecutable);
        Environment.SetEnvironmentVariable("PYSPARK_DRIVER_PYTHON", pythonExecutable);
        Environment.SetEnvironmentVariable("AWS_ACCESS_KEY_ID", "admin");
        Environment.SetEnvironmentVariable("AWS_SECRET_ACCESS_KEY", "key");
        Environment.SetEnvironmentVariable("AWS_REGION", "us-east-1");
        Environment.SetEnvironmentVariable("SPARK_LOCAL_IP", "127.0.0.1");
        Environment.SetEnvironmentVariable(
            "PYSPARK_SUBMIT_ARGS",
            $"--packages {SparkPackages} pyspark-shell");
        Environment.SetEnvironmentVariable(
            "SPARK_HOME",
            Path.Combine(configuration.SitePackages, "pyspark"));

        PythonEngine.Initialize();
        PythonEngine.BeginAllowThreads();

        using (Py.GIL())
        {
            using PyObject os = Py.Import("os");
            using PyObject environment = (PyObject)((dynamic)os).environ;
            SetPythonEnvironment(
                environment,
                "SPARK_HOME",
                Path.Combine(configuration.SitePackages, "pyspark"));
            SetPythonEnvironment(environment, "PYSPARK_PYTHON", pythonExecutable);
            SetPythonEnvironment(environment, "PYSPARK_DRIVER_PYTHON", pythonExecutable);
            SetPythonEnvironment(environment, "AWS_ACCESS_KEY_ID", "admin");
            SetPythonEnvironment(environment, "AWS_SECRET_ACCESS_KEY", "key");
            SetPythonEnvironment(environment, "AWS_REGION", "us-east-1");
            SetPythonEnvironment(environment, "SPARK_LOCAL_IP", "127.0.0.1");
            SetPythonEnvironment(
                environment,
                "PYSPARK_SUBMIT_ARGS",
                $"--packages {SparkPackages} pyspark-shell");

            using PyObject pysparkSql = Py.Import("pyspark.sql");
            dynamic sql = pysparkSql;
            dynamic builder = sql.SparkSession.builder;
            dynamic spark = builder
                .master("local[2]")
                .appName("Iceberg.Net.Tests")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.sql.session.timeZone", "UTC")
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg.type", "rest")
                .config("spark.sql.catalog.iceberg.uri", "http://127.0.0.1:8181")
                .config("spark.sql.catalog.iceberg.warehouse", "warehouse")
                .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.iceberg.s3.endpoint", "http://127.0.0.1:8333")
                .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
                .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin")
                .config("spark.sql.catalog.iceberg.s3.secret-access-key", "key")
                .config("spark.sql.catalog.iceberg.rest-metrics-reporting-enabled", "false")
                .getOrCreate();
            _spark = (PyObject)spark;
            spark.sparkContext.setLogLevel("WARN");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_spark is not null)
            {
                using (Py.GIL())
                {
                    dynamic spark = _spark;
                    spark.stop();
                    _spark.Dispose();
                    _spark = null;
                }
            }

            if (PythonEngine.IsInitialized) PythonEngine.Shutdown();
        }
        finally
        {
            _gate.Release();
            _gate.Dispose();
        }
    }

    public async Task<List<object?>> ReadTable(Identifier identifier)
    {
        await _gate.WaitAsync(TestContext.Current.CancellationToken);
        try
        {
            using (Py.GIL())
            {
                dynamic spark = _spark ?? throw new InvalidOperationException("PySpark is not initialized");
                string tableName = string.Join(
                    ".",
                    new[] { "iceberg" }.Concat(identifier).Select(QuoteIdentifier));
                using PyObject dataFrame = (PyObject)spark.table(tableName);
                dynamic frame = dataFrame;
                using PyObject rows = (PyObject)frame.collect();

                var result = new List<object?>(checked((int)rows.Length()));
                for (int index = 0; index < rows.Length(); index++)
                {
                    using PyObject row = rows.GetItem(index);
                    dynamic dynamicRow = row;
                    using PyObject dictionary = (PyObject)dynamicRow.asDict(recursive: true);
                    result.Add(ToManaged(dictionary));
                }

                return result;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task WritePartitionedTable(
        Identifier identifier,
        IReadOnlyList<(long Id, string Category, long Region)> rows)
    {
        await _gate.WaitAsync(TestContext.Current.CancellationToken);
        try
        {
            using (Py.GIL())
            {
                dynamic spark = _spark ?? throw new InvalidOperationException("PySpark is not initialized");
                using PyList pythonRows = new();
                foreach ((long id, string category, long region) in rows)
                {
                    using PyInt pythonId = new(id);
                    using PyString pythonCategory = new(category);
                    using PyInt pythonRegion = new(region);
                    using PyTuple row = new([pythonId, pythonCategory, pythonRegion]);
                    pythonRows.Append(row);
                }

                using PyString idColumn = new("id");
                using PyString categoryColumn = new("category");
                using PyString regionColumn = new("region");
                using PyList columns = new([idColumn, categoryColumn, regionColumn]);
                using PyObject dataFrame = (PyObject)spark.createDataFrame(pythonRows, columns);
                dynamic writer = ((dynamic)dataFrame).write;
                string tableName = string.Join(
                    ".",
                    new[] { "iceberg" }.Concat(identifier).Select(QuoteIdentifier));
                writer
                    .format("iceberg")
                    .mode("overwrite")
                    .partitionBy("category", "region")
                    .saveAsTable(tableName);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task WriteAllPrimitiveTypesPartitionedTable(Identifier identifier)
    {
        await _gate.WaitAsync(TestContext.Current.CancellationToken);
        try
        {
            using (Py.GIL())
            {
                dynamic spark = _spark ?? throw new InvalidOperationException("PySpark is not initialized");
                using PyObject typesModule = Py.Import("pyspark.sql.types");
                using PyObject dateTimeModule = Py.Import("datetime");
                using PyObject decimalModule = Py.Import("decimal");
                using PyObject builtinsModule = Py.Import("builtins");
                dynamic dateTime = dateTimeModule;
                dynamic decimalType = decimalModule;
                dynamic builtins = builtinsModule;

                using PyObject schema = CreateAllPrimitiveTypesSchema(typesModule);
                using PyObject booleanFactory = builtinsModule.GetAttr("bool");
                dynamic createBoolean = booleanFactory;
                using PyObject booleanValue = (PyObject)createBoolean(1);
                using PyObject decimalValue = (PyObject)decimalType.Decimal(
                    "12345678901234567890.123456789012345678");
                using PyObject dateValue = (PyObject)dateTime.date(2024, 2, 29);
                using PyObject timestampValue = (PyObject)dateTime.datetime(
                    2024,
                    2,
                    29,
                    12,
                    34,
                    56,
                    123456);
                using PyObject timeZone = dateTimeModule.GetAttr("timezone");
                using PyObject utc = timeZone.GetAttr("utc");
                using PyObject timestampTzValue = (PyObject)dateTime.datetime(
                    2024,
                    2,
                    29,
                    12,
                    34,
                    56,
                    654321,
                    tzinfo: utc);
                using PyInt binaryByte0 = new(0);
                using PyInt binaryByte1 = new(1);
                using PyInt binaryByte2 = new(2);
                using PyInt binaryByte255 = new(255);
                using PyList binaryBytes = new(
                    [binaryByte0, binaryByte1, binaryByte2, binaryByte255]);
                using PyObject binaryValue = (PyObject)builtins.bytes(binaryBytes);
                using PyInt id = new(1);
                using PyInt intValue = new(34);
                using PyInt longValue = new(1234567890123L);
                using PyFloat floatValue = new(1.25);
                using PyFloat doubleValue = new(-2.5);
                using PyString stringValue = new("iceberg");
                using PyTuple populatedRow = new(
                [
                    id,
                    booleanValue,
                    intValue,
                    longValue,
                    floatValue,
                    doubleValue,
                    decimalValue,
                    dateValue,
                    timestampValue,
                    timestampTzValue,
                    stringValue,
                    binaryValue
                ]);
                using PyTuple nullRow = CreateNullPartitionRow();
                using PyList rows = new([populatedRow, nullRow]);
                using PyObject dataFrame = (PyObject)spark.createDataFrame(rows, schema);
                dynamic writer = ((dynamic)dataFrame).write;
                string tableName = string.Join(
                    ".",
                    new[] { "iceberg" }.Concat(identifier).Select(QuoteIdentifier));
                writer
                    .format("iceberg")
                    .mode("overwrite")
                    .partitionBy(
                        "boolean_value",
                        "int_value",
                        "long_value",
                        "float_value",
                        "double_value",
                        "decimal_value",
                        "date_value",
                        "timestamp_value",
                        "timestamptz_value",
                        "string_value",
                        "binary_value")
                    .saveAsTable(tableName);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private static PyObject CreateAllPrimitiveTypesSchema(PyObject typesModule)
    {
        using PyList fields = new();
        AppendField(fields, typesModule, "id", "LongType");
        AppendField(fields, typesModule, "boolean_value", "BooleanType");
        AppendField(fields, typesModule, "int_value", "IntegerType");
        AppendField(fields, typesModule, "long_value", "LongType");
        AppendField(fields, typesModule, "float_value", "FloatType");
        AppendField(fields, typesModule, "double_value", "DoubleType");
        AppendField(fields, typesModule, "decimal_value", "DecimalType");
        AppendField(fields, typesModule, "date_value", "DateType");
        AppendField(fields, typesModule, "timestamp_value", "TimestampNTZType");
        AppendField(fields, typesModule, "timestamptz_value", "TimestampType");
        AppendField(fields, typesModule, "string_value", "StringType");
        AppendField(fields, typesModule, "binary_value", "BinaryType");

        dynamic types = typesModule;
        return (PyObject)types.StructType(fields);
    }

    private static void AppendField(
        PyList fields,
        PyObject typesModule,
        string name,
        string typeName)
    {
        using PyObject typeFactory = typesModule.GetAttr(typeName);
        dynamic factory = typeFactory;
        using PyObject dataType = typeName switch
        {
            "DecimalType" => (PyObject)factory(38, 18),
            _ => (PyObject)factory()
        };
        dynamic types = typesModule;
        using PyObject field = (PyObject)types.StructField(name, dataType, nullable: true);
        fields.Append(field);
    }

    private static PyTuple CreateNullPartitionRow()
    {
        PyObject[] values = new PyObject[12];
        values[0] = new PyInt(2);
        for (int index = 1; index < values.Length; index++)
            values[index] = PyObject.FromManagedObject(null!);

        try
        {
            return new PyTuple(values);
        }
        finally
        {
            foreach (PyObject value in values) value.Dispose();
        }
    }

    private static object? ToManaged(PyObject value)
    {
        if (value.IsNone()) return null;

        using PyObject pythonType = value.GetPythonType();
        using PyObject typeNameValue = pythonType.GetAttr("__name__");
        string typeName = typeNameValue.As<string>();

        return typeName switch
        {
            "bool" => value.As<bool>(),
            "int" => value.As<long>(),
            "float" => value.As<double>(),
            "str" => value.As<string>(),
            "bytes" or "bytearray" => value.As<byte[]>(),
            "Decimal" => decimal.Parse(
                value.ToString() ?? throw new InvalidOperationException("Decimal had no value"),
                CultureInfo.InvariantCulture),
            "date" => ReadDate(value),
            "datetime" => ReadDateTime(value),
            "dict" => ReadDictionary(value),
            "list" or "tuple" => ReadList(value),
            _ => throw new NotSupportedException($"Unsupported Python value type '{typeName}'")
        };
    }

    private static void SetPythonEnvironment(PyObject environment, string key, string value)
    {
        using PyString pythonValue = new(value);
        dynamic dynamicEnvironment = environment;
        dynamicEnvironment.__setitem__(key, pythonValue);
    }

    private static string QuoteIdentifier(string identifier) =>
        $"`{identifier.Replace("`", "``")}`";

    private static DateOnly ReadDate(PyObject value) => new(
        ReadIntAttribute(value, "year"),
        ReadIntAttribute(value, "month"),
        ReadIntAttribute(value, "day"));

    private static DateTime ReadDateTime(PyObject value) => new(
        ReadIntAttribute(value, "year"),
        ReadIntAttribute(value, "month"),
        ReadIntAttribute(value, "day"),
        ReadIntAttribute(value, "hour"),
        ReadIntAttribute(value, "minute"),
        ReadIntAttribute(value, "second"),
        ReadIntAttribute(value, "microsecond") / 1000,
        ReadIntAttribute(value, "microsecond") % 1000,
        DateTimeKind.Unspecified);

    private static int ReadIntAttribute(PyObject value, string name)
    {
        using PyObject attribute = value.GetAttr(name);
        return attribute.As<int>();
    }

    private static Dictionary<object, object?> ReadDictionary(PyObject value)
    {
        using PyDict dictionary = new(value);
        using PyObject items = dictionary.Items();
        var result = new Dictionary<object, object?>(checked((int)items.Length()));
        for (int index = 0; index < items.Length(); index++)
        {
            using PyObject pair = items.GetItem(index);
            using PyObject key = pair.GetItem(0);
            using PyObject itemValue = pair.GetItem(1);
            result[ToManaged(key)!] = ToManaged(itemValue);
        }

        return result;
    }

    private static List<object?> ReadList(PyObject value)
    {
        var result = new List<object?>(checked((int)value.Length()));
        for (int index = 0; index < value.Length(); index++)
        {
            using PyObject item = value.GetItem(index);
            result.Add(ToManaged(item));
        }

        return result;
    }

    private static PythonConfiguration ReadPythonConfiguration(string projectDirectory)
    {
        string virtualEnvironment = Path.Combine(projectDirectory, ".venv");
        string configurationPath = Path.Combine(virtualEnvironment, "pyvenv.cfg");
        string? interpreterBin = File.ReadLines(configurationPath)
            .Select(line => line.Split('=', 2, StringSplitOptions.TrimEntries))
            .Where(parts => parts.Length == 2 && parts[0] == "home")
            .Select(parts => parts[1])
            .SingleOrDefault();
        if (interpreterBin is null)
            throw new InvalidOperationException($"Could not find 'home' in '{configurationPath}'");

        string home = Directory.GetParent(interpreterBin)?.FullName
                      ?? throw new InvalidOperationException($"Invalid Python home '{interpreterBin}'");
        string sitePackages = OperatingSystem.IsWindows()
            ? Path.Combine(virtualEnvironment, "Lib", "site-packages")
            : Path.Combine(virtualEnvironment, "lib", "python3.12", "site-packages");
        string standardLibrary = OperatingSystem.IsWindows()
            ? Path.Combine(home, "Lib")
            : Path.Combine(home, "lib", "python3.12");

        string[] libraryCandidates = OperatingSystem.IsWindows()
            ? [Path.Combine(home, "python312.dll")]
            : OperatingSystem.IsMacOS()
                ? [Path.Combine(home, "lib", "libpython3.12.dylib")]
                :
                [
                    Path.Combine(home, "lib", "libpython3.12.so"),
                    Path.Combine(home, "lib", "libpython3.12.so.1.0")
                ];
        string library = libraryCandidates.FirstOrDefault(File.Exists)
                         ?? throw new InvalidOperationException(
                             $"Could not find the Python shared library under '{home}'");

        return new PythonConfiguration(
            library,
            home,
            sitePackages,
            [sitePackages, standardLibrary, Path.Combine(standardLibrary, "lib-dynload")]);
    }

    private static async Task RunProcess(string executable, IReadOnlyList<string> arguments)
    {
        ProcessStartInfo startInfo = new(executable)
        {
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false
        };
        foreach (string argument in arguments) startInfo.ArgumentList.Add(argument);

        using Process process = Process.Start(startInfo)
                                ?? throw new InvalidOperationException($"Could not start '{executable}'");
        Task<string> standardOutput = process.StandardOutput.ReadToEndAsync();
        Task<string> standardError = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        string output = await standardOutput;
        string error = await standardError;
        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"'{executable}' exited with code {process.ExitCode}:{Environment.NewLine}{error}");
    }

    private sealed record PythonConfiguration(
        string Library,
        string Home,
        string SitePackages,
        string[] Paths);
}
