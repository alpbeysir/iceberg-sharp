using System.Data.Common;
using DuckDB.NET.Data;

namespace Iceberg.Net.DuckUtils;

public class DuckDbCatalog
{
    private readonly DuckDBConnection _connection = new("Data Source=file.db");
    public readonly string CatalogName = "test_catalog";

    public async ValueTask InitializeAsync()
    {
        await _connection.OpenAsync();
        await using DuckDBCommand command = _connection.CreateCommand();
        command.CommandText =
            $"""
             ATTACH 'warehouse' AS {CatalogName} (
                TYPE iceberg,
                ENDPOINT 'http://localhost:8181',
                AUTHORIZATION_TYPE none
             );
             """;
        await command.ExecuteNonQueryAsync();

        command.CommandText =
            """
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                ENDPOINT '127.0.0.1:8333',
                PROVIDER config,
                KEY_ID 'admin',
                SECRET 'key',
                USE_SSL false,
                URL_STYLE path
            );
            """;
        await command.ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync();
        await Task.CompletedTask;
    }

    public async Task<DbDataReader> ExecuteQuery(string sql)
    {
        DuckDBCommand command = _connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteReaderAsync();
    }
}