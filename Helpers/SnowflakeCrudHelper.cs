using Snowflake.Data.Client;
using System.Data;
using System.Text;

namespace TRANSFER_IN_PLAN.Helpers;

/// <summary>
/// Reusable Snowflake CRUD helper — eliminates boilerplate across 22+ planning controllers.
/// Pattern matches the proven ARS controllers (ArsStMasterController, ArsDisplayMasterController).
/// </summary>
public static class SnowflakeCrudHelper
{
    // ── Connection ────────────────────────────────────────────

    public static SnowflakeDbConnection Open(string connStr)
    {
        var conn = new SnowflakeDbConnection { ConnectionString = connStr };
        conn.Open();
        return conn;
    }

    public static async Task<SnowflakeDbConnection> OpenAsync(string connStr)
    {
        var conn = new SnowflakeDbConnection { ConnectionString = connStr };
        await conn.OpenAsync();
        return conn;
    }

    // ── Reader Helpers (null-safe) ────────────────────────────

    public static string Str(IDataReader r, int i) => r.IsDBNull(i) ? "" : r.GetString(i);
    public static string? StrNull(IDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);
    public static int Int(IDataReader r, int i) => r.IsDBNull(i) ? 0 : Convert.ToInt32(r.GetValue(i));
    public static int? IntNull(IDataReader r, int i) => r.IsDBNull(i) ? null : Convert.ToInt32(r.GetValue(i));
    public static decimal Dec(IDataReader r, int i) => r.IsDBNull(i) ? 0m : Convert.ToDecimal(r.GetValue(i));
    public static decimal? DecNull(IDataReader r, int i) => r.IsDBNull(i) ? null : Convert.ToDecimal(r.GetValue(i));
    public static DateTime? DateNull(IDataReader r, int i) => r.IsDBNull(i) ? null : Convert.ToDateTime(r.GetValue(i));
    public static bool Bool(IDataReader r, int i) => !r.IsDBNull(i) && Convert.ToBoolean(r.GetValue(i));

    // ── Parameter Helpers ─────────────────────────────────────

    public static SnowflakeDbParameter Param(string name, object? value, DbType dbType = DbType.String)
        => new() { ParameterName = name, Value = value ?? DBNull.Value, DbType = dbType };

    public static SnowflakeDbParameter CloneParam(SnowflakeDbParameter src)
        => new() { ParameterName = src.ParameterName, Value = src.Value, DbType = src.DbType };

    // ── COUNT ─────────────────────────────────────────────────

    public static async Task<int> CountAsync(SnowflakeDbConnection conn, string table, string? where = null, List<SnowflakeDbParameter>? parms = null)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}{(string.IsNullOrEmpty(where) ? "" : " WHERE " + where)}";
        if (parms != null) foreach (var p in parms) cmd.Parameters.Add(CloneParam(p));
        return Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
    }

    // ── DISTINCT VALUES (for dropdown filters) ────────────────

    public static async Task<List<string>> DistinctAsync(SnowflakeDbConnection conn, string table, string column, string? where = null)
    {
        var list = new List<string>();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL{(string.IsNullOrEmpty(where) ? "" : " AND " + where)} ORDER BY {column}";
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync()) list.Add(r.GetString(0));
        return list;
    }

    // ── PAGED QUERY ───────────────────────────────────────────

    public static async Task<List<T>> PagedQueryAsync<T>(
        SnowflakeDbConnection conn, string table, string columns, string? where,
        List<SnowflakeDbParameter>? parms, string orderBy, int page, int pageSize,
        Func<IDataReader, T> mapper)
    {
        var list = new List<T>();
        int offset = (page - 1) * pageSize;
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {columns} FROM {table}{(string.IsNullOrEmpty(where) ? "" : " WHERE " + where)} ORDER BY {orderBy} LIMIT {pageSize} OFFSET {offset}";
        if (parms != null) foreach (var p in parms) cmd.Parameters.Add(CloneParam(p));
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync()) list.Add(mapper(r));
        return list;
    }

    // ── FIND BY ID ────────────────────────────────────────────

    public static async Task<T?> FindByIdAsync<T>(SnowflakeDbConnection conn, string table, string columns, int id, Func<IDataReader, T> mapper) where T : class
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {columns} FROM {table} WHERE ID = ?";
        cmd.Parameters.Add(Param("1", id, DbType.Int32));
        await using var r = await cmd.ExecuteReaderAsync();
        return await r.ReadAsync() ? mapper(r) : null;
    }

    // ── INSERT ────────────────────────────────────────────────

    public static async Task InsertAsync(SnowflakeDbConnection conn, string table, string[] columns, object?[] values)
    {
        await using var cmd = conn.CreateCommand();
        var placeholders = string.Join(",", columns.Select((_, i) => "?"));
        cmd.CommandText = $"INSERT INTO {table} ({string.Join(",", columns)}) VALUES ({placeholders})";
        for (int i = 0; i < values.Length; i++)
            cmd.Parameters.Add(Param((i + 1).ToString(), values[i]));
        await cmd.ExecuteNonQueryAsync();
    }

    // ── UPDATE ────────────────────────────────────────────────

    public static async Task UpdateAsync(SnowflakeDbConnection conn, string table, string[] columns, object?[] values, int id)
    {
        await using var cmd = conn.CreateCommand();
        var sets = string.Join(",", columns.Select((c, i) => $"{c}=?"));
        cmd.CommandText = $"UPDATE {table} SET {sets} WHERE ID=?";
        for (int i = 0; i < values.Length; i++)
            cmd.Parameters.Add(Param((i + 1).ToString(), values[i]));
        cmd.Parameters.Add(Param((values.Length + 1).ToString(), id, DbType.Int32));
        await cmd.ExecuteNonQueryAsync();
    }

    // ── DELETE ─────────────────────────────────────────────────

    public static async Task DeleteAsync(SnowflakeDbConnection conn, string table, int id)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM {table} WHERE ID=?";
        cmd.Parameters.Add(Param("1", id, DbType.Int32));
        await cmd.ExecuteNonQueryAsync();
    }

    // ── FILTER BUILDER ────────────────────────────────────────

    public static (string? where, List<SnowflakeDbParameter> parms) BuildFilter(Dictionary<string, string?> filters)
    {
        var conditions = new List<string>();
        var parms = new List<SnowflakeDbParameter>();
        int idx = 0;
        foreach (var (col, val) in filters)
        {
            if (string.IsNullOrEmpty(val)) continue;
            idx++;
            conditions.Add($"{col} = ?");
            parms.Add(Param(idx.ToString(), val));
        }
        return (conditions.Count > 0 ? string.Join(" AND ", conditions) : null, parms);
    }

    // ── CSV EXPORT HELPER ─────────────────────────────────────

    public static async Task StreamCsvAsync(SnowflakeDbConnection conn, string sql, StreamWriter writer, string headerLine, int timeout = 300)
    {
        await writer.WriteLineAsync(headerLine);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = timeout;
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            var sb = new StringBuilder();
            for (int i = 0; i < r.FieldCount; i++)
            {
                if (i > 0) sb.Append(',');
                var val = r.IsDBNull(i) ? "" : r.GetValue(i)?.ToString() ?? "";
                if (val.Contains(',') || val.Contains('"') || val.Contains('\n'))
                    sb.Append('"').Append(val.Replace("\"", "\"\"")).Append('"');
                else sb.Append(val);
            }
            await writer.WriteLineAsync(sb.ToString());
        }
        await writer.FlushAsync();
    }

    // ── SCALAR QUERY ──────────────────────────────────────────

    public static async Task<object?> ScalarAsync(SnowflakeDbConnection conn, string sql)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }

    // ── EXECUTE NON-QUERY ─────────────────────────────────────

    public static async Task ExecAsync(SnowflakeDbConnection conn, string sql, int timeout = 60)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = timeout;
        await cmd.ExecuteNonQueryAsync();
    }
}
