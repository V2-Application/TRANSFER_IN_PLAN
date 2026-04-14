using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsAutoSaleController : Controller
{
    private readonly string _connStr;
    public ArsAutoSaleController(IConfiguration config) =>
        _connStr = config.GetConnectionString("Snowflake")!;

    private SnowflakeDbConnection OpenConn()
    {
        var conn = new SnowflakeDbConnection { ConnectionString = _connStr };
        conn.Open();
        return conn;
    }

    public async Task<IActionResult> Index(string? st, string? mj, int page = 1, int pageSize = 100)
    {
        var where = new StringBuilder();
        var parms = new List<SnowflakeDbParameter>();
        int pIdx = 1;

        if (!string.IsNullOrEmpty(st))
        {
            where.Append(" AND ST = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (pIdx++).ToString(), Value = st, DbType = DbType.String });
        }
        if (!string.IsNullOrEmpty(mj))
        {
            where.Append(" AND MJ = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (pIdx++).ToString(), Value = mj, DbType = DbType.String });
        }

        string filter = where.Length > 0 ? " WHERE " + where.ToString()[5..] : "";

        using var conn = OpenConn();

        // Total rows (unfiltered)
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM ARS_ST_MJ_AUTO_SALE";
            ViewBag.TotalRows = Convert.ToInt32(await Task.Run(() => cmd.ExecuteScalar()));
        }

        // Filtered count
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM ARS_ST_MJ_AUTO_SALE{filter}";
            foreach (var p in parms) cmd.Parameters.Add(CloneParam(p));
            ViewBag.TotalCount = Convert.ToInt32(await Task.Run(() => cmd.ExecuteScalar()));
        }

        // Distinct stores list
        var stores = new List<string>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT DISTINCT ST FROM ARS_ST_MJ_AUTO_SALE ORDER BY ST";
            using var rdr = await Task.Run(() => cmd.ExecuteReader());
            while (rdr.Read()) stores.Add(rdr.GetString(0));
        }
        ViewBag.Stores = stores;

        // Distinct MJ list
        var majCats = new List<string>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT DISTINCT MJ FROM ARS_ST_MJ_AUTO_SALE ORDER BY MJ";
            using var rdr = await Task.Run(() => cmd.ExecuteReader());
            while (rdr.Read()) majCats.Add(rdr.GetString(0));
        }
        ViewBag.MajCats = majCats;

        ViewBag.TotalStores = stores.Count;
        ViewBag.TotalCats = majCats.Count;
        ViewBag.Page = page;
        ViewBag.PageSize = pageSize;
        ViewBag.St = st;
        ViewBag.Mj = mj;

        // Paged data
        int offset = (page - 1) * pageSize;
        var data = new List<ArsStMjAutoSale>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT ID, ST, MJ, CM_REM_DAYS, NM_DAYS, CM_AUTO_SALE_Q, NM_AUTO_SALE_Q FROM ARS_ST_MJ_AUTO_SALE{filter} ORDER BY ST, MJ LIMIT {pageSize} OFFSET {offset}";
            foreach (var p in parms) cmd.Parameters.Add(CloneParam(p));
            using var rdr = await Task.Run(() => cmd.ExecuteReader());
            while (rdr.Read())
            {
                data.Add(new ArsStMjAutoSale
                {
                    Id = rdr.GetInt32(0),
                    St = rdr.IsDBNull(1) ? "" : rdr.GetString(1),
                    Mj = rdr.IsDBNull(2) ? "" : rdr.GetString(2),
                    CmRemDays = rdr.IsDBNull(3) ? null : rdr.GetDecimal(3),
                    NmDays = rdr.IsDBNull(4) ? null : rdr.GetDecimal(4),
                    CmAutoSaleQ = rdr.IsDBNull(5) ? null : rdr.GetDecimal(5),
                    NmAutoSaleQ = rdr.IsDBNull(6) ? null : rdr.GetDecimal(6)
                });
            }
        }

        return View(data);
    }

    public IActionResult Create() => View(new ArsStMjAutoSale());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ArsStMjAutoSale model)
    {
        if (!ModelState.IsValid) return View(model);

        using var conn = OpenConn();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO ARS_ST_MJ_AUTO_SALE (ST, MJ, CM_REM_DAYS, NM_DAYS, CM_AUTO_SALE_Q, NM_AUTO_SALE_Q) VALUES (?, ?, ?, ?, ?, ?)";
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "1", Value = model.St, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "2", Value = model.Mj, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "3", Value = (object?)model.CmRemDays ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "4", Value = (object?)model.NmDays ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "5", Value = (object?)model.CmAutoSaleQ ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "6", Value = (object?)model.NmAutoSaleQ ?? DBNull.Value, DbType = DbType.Decimal });
        await Task.Run(() => cmd.ExecuteNonQuery());

        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Edit(int id)
    {
        var item = await FindById(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(ArsStMjAutoSale model)
    {
        if (!ModelState.IsValid) return View(model);

        using var conn = OpenConn();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE ARS_ST_MJ_AUTO_SALE SET ST=?, MJ=?, CM_REM_DAYS=?, NM_DAYS=?, CM_AUTO_SALE_Q=?, NM_AUTO_SALE_Q=? WHERE ID=?";
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "1", Value = model.St, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "2", Value = model.Mj, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "3", Value = (object?)model.CmRemDays ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "4", Value = (object?)model.NmDays ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "5", Value = (object?)model.CmAutoSaleQ ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "6", Value = (object?)model.NmAutoSaleQ ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "7", Value = model.Id, DbType = DbType.Int32 });
        await Task.Run(() => cmd.ExecuteNonQuery());

        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Delete(int id)
    {
        var item = await FindById(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        using var conn = OpenConn();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM ARS_ST_MJ_AUTO_SALE WHERE ID=?";
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "1", Value = id, DbType = DbType.Int32 });
        await Task.Run(() => cmd.ExecuteNonQuery());

        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? st, string? mj)
    {
        var where = new StringBuilder();
        var parms = new List<SnowflakeDbParameter>();
        int pIdx = 1;

        if (!string.IsNullOrEmpty(st))
        {
            where.Append(" AND ST = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (pIdx++).ToString(), Value = st, DbType = DbType.String });
        }
        if (!string.IsNullOrEmpty(mj))
        {
            where.Append(" AND MJ = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (pIdx++).ToString(), Value = mj, DbType = DbType.String });
        }

        string filter = where.Length > 0 ? " WHERE " + where.ToString()[5..] : "";

        using var conn = OpenConn();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT ST, MJ, CM_REM_DAYS, NM_DAYS, CM_AUTO_SALE_Q, NM_AUTO_SALE_Q FROM ARS_ST_MJ_AUTO_SALE{filter} ORDER BY ST, MJ";
        foreach (var p in parms) cmd.Parameters.Add(p);

        var sb = new StringBuilder();
        sb.AppendLine("ST,MJ,CM_REM_DAYS,NM_DAYS,CM_AUTO_SALE_Q,NM_AUTO_SALE_Q,CM_PD_SALE_Q,NM_PD_SALE_Q");
        using var rdr = await Task.Run(() => cmd.ExecuteReader());
        while (rdr.Read())
        {
            var sSt = rdr.IsDBNull(0) ? "" : rdr.GetString(0);
            var sMj = rdr.IsDBNull(1) ? "" : rdr.GetString(1);
            var cmRemDays = rdr.IsDBNull(2) ? (decimal?)null : rdr.GetDecimal(2);
            var nmDays = rdr.IsDBNull(3) ? (decimal?)null : rdr.GetDecimal(3);
            var cmAutoSaleQ = rdr.IsDBNull(4) ? (decimal?)null : rdr.GetDecimal(4);
            var nmAutoSaleQ = rdr.IsDBNull(5) ? (decimal?)null : rdr.GetDecimal(5);

            // Compute per-day sale (same logic as model NotMapped properties)
            decimal cmPdSaleQ = cmRemDays > 0 ? (cmAutoSaleQ ?? 0) / (cmRemDays ?? 1) : 0;
            decimal nmPdSaleQ = nmDays > 0 ? (nmAutoSaleQ ?? 0) / (nmDays ?? 1) : 0;

            sb.AppendLine($"{sSt},{sMj},{cmRemDays},{nmDays},{cmAutoSaleQ},{nmAutoSaleQ},{cmPdSaleQ:F4},{nmPdSaleQ:F4}");
        }

        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ARS_Auto_Sale.csv");
    }

    // -- helpers ----------------------------------------------------------

    private async Task<ArsStMjAutoSale?> FindById(int id)
    {
        using var conn = OpenConn();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT ID, ST, MJ, CM_REM_DAYS, NM_DAYS, CM_AUTO_SALE_Q, NM_AUTO_SALE_Q FROM ARS_ST_MJ_AUTO_SALE WHERE ID=?";
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "1", Value = id, DbType = DbType.Int32 });
        using var rdr = await Task.Run(() => cmd.ExecuteReader());
        if (!rdr.Read()) return null;
        return new ArsStMjAutoSale
        {
            Id = rdr.GetInt32(0),
            St = rdr.IsDBNull(1) ? "" : rdr.GetString(1),
            Mj = rdr.IsDBNull(2) ? "" : rdr.GetString(2),
            CmRemDays = rdr.IsDBNull(3) ? null : rdr.GetDecimal(3),
            NmDays = rdr.IsDBNull(4) ? null : rdr.GetDecimal(4),
            CmAutoSaleQ = rdr.IsDBNull(5) ? null : rdr.GetDecimal(5),
            NmAutoSaleQ = rdr.IsDBNull(6) ? null : rdr.GetDecimal(6)
        };
    }

    private static SnowflakeDbParameter CloneParam(SnowflakeDbParameter src) =>
        new() { ParameterName = src.ParameterName, Value = src.Value, DbType = src.DbType };
}
