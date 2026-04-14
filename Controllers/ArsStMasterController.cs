using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsStMasterController : Controller
{
    private readonly string _connStr;
    public ArsStMasterController(IConfiguration config) =>
        _connStr = config.GetConnectionString("Snowflake")!;

    private SnowflakeDbConnection OpenConn()
    {
        var conn = new SnowflakeDbConnection { ConnectionString = _connStr };
        conn.Open();
        return conn;
    }

    // ── Column list shared across queries ────────────────────────
    private const string AllCols = "ID, ST_CD, ST_NM, HUB_CD, HUB_NM, DIRECT_HUB, TAGGED_RDC, DH24_DC_TO_HUB_INTRA, DH24_HUB_TO_ST_INTRA, DW01_DC_TO_HUB_INTRA, DW01_HUB_TO_ST_INTRA, ST_OP_DT, ST_STAT, SALE_COVER_DAYS, PRD_DAYS";

    public async Task<IActionResult> Index(string? st, string? taggedRdc, string? hubCd, int page = 1, int pageSize = 100)
    {
        var where = new StringBuilder();
        var parms = new List<SnowflakeDbParameter>();
        int pIdx = 1;

        if (!string.IsNullOrEmpty(st))
        {
            where.Append(" AND ST_CD = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (pIdx++).ToString(), Value = st, DbType = DbType.String });
        }
        if (!string.IsNullOrEmpty(taggedRdc))
        {
            where.Append(" AND TAGGED_RDC = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (pIdx++).ToString(), Value = taggedRdc, DbType = DbType.String });
        }
        if (!string.IsNullOrEmpty(hubCd))
        {
            where.Append(" AND HUB_CD = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (pIdx++).ToString(), Value = hubCd, DbType = DbType.String });
        }

        string filter = where.Length > 0 ? " WHERE " + where.ToString()[5..] : "";

        using var conn = OpenConn();

        // Total rows (unfiltered)
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM ARS_ST_MASTER";
            ViewBag.TotalRows = Convert.ToInt32(await Task.Run(() => cmd.ExecuteScalar()));
        }

        // Filtered count
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM ARS_ST_MASTER{filter}";
            foreach (var p in parms) cmd.Parameters.Add(CloneParam(p));
            ViewBag.TotalCount = Convert.ToInt32(await Task.Run(() => cmd.ExecuteScalar()));
        }

        // Distinct stores
        var stores = new List<string>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT DISTINCT ST_CD FROM ARS_ST_MASTER ORDER BY ST_CD";
            using var rdr = await Task.Run(() => cmd.ExecuteReader());
            while (rdr.Read()) stores.Add(rdr.GetString(0));
        }
        ViewBag.Stores = stores;

        // Distinct RDCs
        var rdcs = new List<string>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT DISTINCT TAGGED_RDC FROM ARS_ST_MASTER WHERE TAGGED_RDC IS NOT NULL ORDER BY TAGGED_RDC";
            using var rdr = await Task.Run(() => cmd.ExecuteReader());
            while (rdr.Read()) rdcs.Add(rdr.GetString(0));
        }
        ViewBag.Rdcs = rdcs;

        // Distinct Hubs
        var hubs = new List<string>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT DISTINCT HUB_CD FROM ARS_ST_MASTER WHERE HUB_CD IS NOT NULL ORDER BY HUB_CD";
            using var rdr = await Task.Run(() => cmd.ExecuteReader());
            while (rdr.Read()) hubs.Add(rdr.GetString(0));
        }
        ViewBag.Hubs = hubs;

        ViewBag.TotalStores = stores.Count;
        ViewBag.TotalRdcs = rdcs.Count;
        ViewBag.TotalHubs = hubs.Count;
        ViewBag.Page = page;
        ViewBag.PageSize = pageSize;
        ViewBag.St = st;
        ViewBag.TaggedRdc = taggedRdc;
        ViewBag.HubCd = hubCd;

        // Paged data
        int offset = (page - 1) * pageSize;
        var data = new List<ArsStMaster>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT {AllCols} FROM ARS_ST_MASTER{filter} ORDER BY ST_CD LIMIT {pageSize} OFFSET {offset}";
            foreach (var p in parms) cmd.Parameters.Add(CloneParam(p));
            using var rdr = await Task.Run(() => cmd.ExecuteReader());
            while (rdr.Read()) data.Add(ReadRow(rdr));
        }

        return View(data);
    }

    public IActionResult Create() => View(new ArsStMaster());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ArsStMaster model)
    {
        if (!ModelState.IsValid) return View(model);

        using var conn = OpenConn();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO ARS_ST_MASTER
            (ST_CD, ST_NM, HUB_CD, HUB_NM, DIRECT_HUB, TAGGED_RDC,
             DH24_DC_TO_HUB_INTRA, DH24_HUB_TO_ST_INTRA, DW01_DC_TO_HUB_INTRA, DW01_HUB_TO_ST_INTRA,
             ST_OP_DT, ST_STAT, SALE_COVER_DAYS, PRD_DAYS)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "1",  Value = model.StCd,               DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "2",  Value = (object?)model.StNm       ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "3",  Value = (object?)model.HubCd      ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "4",  Value = (object?)model.HubNm      ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "5",  Value = (object?)model.DirectHub  ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "6",  Value = (object?)model.TaggedRdc  ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "7",  Value = (object?)model.Dh24DcToHubIntra ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "8",  Value = (object?)model.Dh24HubToStIntra ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "9",  Value = (object?)model.Dw01DcToHubIntra ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "10", Value = (object?)model.Dw01HubToStIntra ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "11", Value = (object?)model.StOpDt     ?? DBNull.Value, DbType = DbType.Date });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "12", Value = (object?)model.StStat     ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "13", Value = (object?)model.SaleCoverDays ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "14", Value = (object?)model.PrdDays    ?? DBNull.Value, DbType = DbType.Decimal });
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
    public async Task<IActionResult> Edit(ArsStMaster model)
    {
        if (!ModelState.IsValid) return View(model);

        using var conn = OpenConn();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"UPDATE ARS_ST_MASTER SET
            ST_CD=?, ST_NM=?, HUB_CD=?, HUB_NM=?, DIRECT_HUB=?, TAGGED_RDC=?,
            DH24_DC_TO_HUB_INTRA=?, DH24_HUB_TO_ST_INTRA=?, DW01_DC_TO_HUB_INTRA=?, DW01_HUB_TO_ST_INTRA=?,
            ST_OP_DT=?, ST_STAT=?, SALE_COVER_DAYS=?, PRD_DAYS=?
            WHERE ID=?";
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "1",  Value = model.StCd,               DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "2",  Value = (object?)model.StNm       ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "3",  Value = (object?)model.HubCd      ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "4",  Value = (object?)model.HubNm      ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "5",  Value = (object?)model.DirectHub  ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "6",  Value = (object?)model.TaggedRdc  ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "7",  Value = (object?)model.Dh24DcToHubIntra ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "8",  Value = (object?)model.Dh24HubToStIntra ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "9",  Value = (object?)model.Dw01DcToHubIntra ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "10", Value = (object?)model.Dw01HubToStIntra ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "11", Value = (object?)model.StOpDt     ?? DBNull.Value, DbType = DbType.Date });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "12", Value = (object?)model.StStat     ?? DBNull.Value, DbType = DbType.String });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "13", Value = (object?)model.SaleCoverDays ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "14", Value = (object?)model.PrdDays    ?? DBNull.Value, DbType = DbType.Decimal });
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "15", Value = model.Id,                  DbType = DbType.Int32 });
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
        cmd.CommandText = "DELETE FROM ARS_ST_MASTER WHERE ID=?";
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "1", Value = id, DbType = DbType.Int32 });
        await Task.Run(() => cmd.ExecuteNonQuery());

        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? st, string? taggedRdc, string? hubCd)
    {
        var where = new StringBuilder();
        var parms = new List<SnowflakeDbParameter>();
        int pIdx = 1;

        if (!string.IsNullOrEmpty(st))
        {
            where.Append(" AND ST_CD = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (pIdx++).ToString(), Value = st, DbType = DbType.String });
        }
        if (!string.IsNullOrEmpty(taggedRdc))
        {
            where.Append(" AND TAGGED_RDC = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (pIdx++).ToString(), Value = taggedRdc, DbType = DbType.String });
        }
        if (!string.IsNullOrEmpty(hubCd))
        {
            where.Append(" AND HUB_CD = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (pIdx++).ToString(), Value = hubCd, DbType = DbType.String });
        }

        string filter = where.Length > 0 ? " WHERE " + where.ToString()[5..] : "";

        using var conn = OpenConn();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {AllCols} FROM ARS_ST_MASTER{filter} ORDER BY ST_CD";
        foreach (var p in parms) cmd.Parameters.Add(p);

        var sb = new StringBuilder();
        sb.AppendLine("ST_CD,ST_NM,HUB_CD,HUB_NM,DIRECT_HUB,TAGGED_RDC,DH24_DC_TO_HUB_INTRA,DH24_HUB_TO_ST_INTRA,DW01_DC_TO_HUB_INTRA,DW01_HUB_TO_ST_INTRA,ST_OP_DT,ST_STAT,SALE_COVER_DAYS,PRD_DAYS,INTRA_DAYS,TTL_ALC_DAYS");
        using var rdr = await Task.Run(() => cmd.ExecuteReader());
        while (rdr.Read())
        {
            var r = ReadRow(rdr);
            sb.AppendLine($"{r.StCd},{r.StNm},{r.HubCd},{r.HubNm},{r.DirectHub},{r.TaggedRdc},{r.Dh24DcToHubIntra},{r.Dh24HubToStIntra},{r.Dw01DcToHubIntra},{r.Dw01HubToStIntra},{r.StOpDt:yyyy-MM-dd},{r.StStat},{r.SaleCoverDays},{r.PrdDays},{r.IntraDays:F2},{r.TtlAlcDays:F2}");
        }

        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ARS_ST_MASTER.csv");
    }

    // ── helpers ──────────────────────────────────────────────────

    private async Task<ArsStMaster?> FindById(int id)
    {
        using var conn = OpenConn();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {AllCols} FROM ARS_ST_MASTER WHERE ID=?";
        cmd.Parameters.Add(new SnowflakeDbParameter { ParameterName = "1", Value = id, DbType = DbType.Int32 });
        using var rdr = await Task.Run(() => cmd.ExecuteReader());
        if (!rdr.Read()) return null;
        return ReadRow(rdr);
    }

    private static ArsStMaster ReadRow(IDataReader rdr) => new()
    {
        Id                = rdr.GetInt32(0),
        StCd              = rdr.IsDBNull(1)  ? "" : rdr.GetString(1),
        StNm              = rdr.IsDBNull(2)  ? null : rdr.GetString(2),
        HubCd             = rdr.IsDBNull(3)  ? null : rdr.GetString(3),
        HubNm             = rdr.IsDBNull(4)  ? null : rdr.GetString(4),
        DirectHub         = rdr.IsDBNull(5)  ? null : rdr.GetString(5),
        TaggedRdc         = rdr.IsDBNull(6)  ? null : rdr.GetString(6),
        Dh24DcToHubIntra  = rdr.IsDBNull(7)  ? null : rdr.GetDecimal(7),
        Dh24HubToStIntra  = rdr.IsDBNull(8)  ? null : rdr.GetDecimal(8),
        Dw01DcToHubIntra  = rdr.IsDBNull(9)  ? null : rdr.GetDecimal(9),
        Dw01HubToStIntra  = rdr.IsDBNull(10) ? null : rdr.GetDecimal(10),
        StOpDt            = rdr.IsDBNull(11) ? null : rdr.GetDateTime(11),
        StStat            = rdr.IsDBNull(12) ? null : rdr.GetString(12),
        SaleCoverDays     = rdr.IsDBNull(13) ? null : rdr.GetDecimal(13),
        PrdDays           = rdr.IsDBNull(14) ? null : rdr.GetDecimal(14)
    };

    private static SnowflakeDbParameter CloneParam(SnowflakeDbParameter src) =>
        new() { ParameterName = src.ParameterName, Value = src.Value, DbType = src.DbType };
}
