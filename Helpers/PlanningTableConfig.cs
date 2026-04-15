namespace TRANSFER_IN_PLAN.Helpers;

/// <summary>
/// Centralized table metadata for all 22 planning bulk-upload tables.
/// Snowflake column names (underscores, no hyphens/spaces).
/// Used by BulkUploadController and individual CRUD controllers.
/// </summary>
public static class PlanningTableConfig
{
    public record TableDef(string DisplayName, string SnowflakeTable, string[] Columns, Type[] Types, string[] Headers, string[] SampleRow);

    public static readonly Dictionary<string, TableDef> Tables = new()
    {
        ["WeekCalendar"] = new(
            "Week Calendar",
            "WEEK_CALENDAR",
            new[] { "WEEK_ID","WEEK_SEQ","FY_WEEK","FY_YEAR","CAL_YEAR","YEAR_WEEK","WK_ST_DT","WK_END_DT" },
            new[] { typeof(int),typeof(int),typeof(int),typeof(int),typeof(int),typeof(string),typeof(DateTime),typeof(DateTime) },
            new[] { "WeekId","WeekSeq","FyWeek","FyYear","CalYear","YearWeek","WkStDt(yyyy-MM-dd)","WkEndDt(yyyy-MM-dd)" },
            new[] { "1","1","1","2026","2026","2026-W01","2026-04-03","2026-04-09" }),

        ["StoreMaster"] = new(
            "Store Master",
            "MASTER_ST_MASTER",
            new[] { "ST_CD","ST_NM","RDC_CD","RDC_NM","HUB_CD","HUB_NM","STATUS","GRID_ST_STS","OP_DATE","AREA","STATE","REF_STATE","SALE_GRP","REF_ST_CD","REF_ST_NM","REF_GRP_NEW","REF_GRP_OLD","DATE" },
            Enumerable.Repeat(typeof(string), 8).Append(typeof(DateTime)).Concat(Enumerable.Repeat(typeof(string), 8)).Append(typeof(DateTime)).ToArray(),
            new[] { "StCd","StNm","RdcCd","RdcNm","HubCd","HubNm","Status","GridStSts","OpDate","Area","State","RefState","SaleGrp","RefStCd","RefStNm","RefGrpNew","RefGrpOld","Date" },
            new[] { "ST001","Store One","RDC01","RDC North","HUB01","Hub Delhi","NEW","A","2026-01-01","NORTH","DELHI","DELHI","GRP-N1","ST001","Ref Store","NGRP-1","NGRP-OLD","2026-04-01" }),

        ["BinCapacity"] = new(
            "Bin Capacity",
            "MASTER_BIN_CAPACITY",
            new[] { "MAJ_CAT","BIN_CAP_DC_TEAM","BIN_CAP" },
            new[] { typeof(string),typeof(decimal),typeof(decimal) },
            new[] { "MajCat","BinCapDcTeam","BinCap" },
            new[] { "APPAREL","150","120" }),

        ["SaleQty"] = new(
            "Sale Quantity (48 Weeks)",
            "QTY_SALE_QTY",
            new string[] { "ST_CD","MAJ_CAT" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK_{w}")).Append("COL_2").ToArray(),
            new Type[] { typeof(string),typeof(string) }.Concat(Enumerable.Repeat(typeof(decimal), 49)).ToArray(),
            new string[] { "StCd","MajCat" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK_{w}")).Append("Col2").ToArray(),
            new string[] { "ST001","APPAREL" }.Concat(Enumerable.Repeat("100", 48)).Append("0").ToArray()),

        ["DispQty"] = new(
            "Display Quantity (48 Weeks)",
            "QTY_DISP_QTY",
            new string[] { "ST_CD","MAJ_CAT" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK_{w}")).Append("COL_2").ToArray(),
            new Type[] { typeof(string),typeof(string) }.Concat(Enumerable.Repeat(typeof(decimal), 49)).ToArray(),
            new string[] { "StCd","MajCat" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK_{w}")).Append("Col2").ToArray(),
            new string[] { "ST001","APPAREL" }.Concat(Enumerable.Repeat("50", 48)).Append("0").ToArray()),

        ["StoreStock"] = new(
            "Store Stock",
            "QTY_ST_STK_Q",
            new[] { "ST_CD","MAJ_CAT","STK_QTY","DATE" },
            new[] { typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
            new[] { "StCd","MajCat","StkQty","Date(yyyy-MM-dd)" },
            new[] { "ST001","APPAREL","120","2026-04-01" }),

        ["DcStock"] = new(
            "DC Stock",
            "QTY_MSA_AND_GRT",
            new[] { "RDC_CD","RDC","MAJ_CAT","DC_STK_Q","GRT_STK_Q","W_GRT_STK_Q","DATE" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(decimal),typeof(decimal),typeof(DateTime) },
            new[] { "RdcCd","Rdc","MajCat","DcStkQ","GrtStkQ","WGrtStkQ","Date(yyyy-MM-dd)" },
            new[] { "RDC01","RDC North","APPAREL","500","200","300","2026-04-01" }),

        ["DelPending"] = new(
            "Delivery Pending",
            "QTY_DEL_PENDING",
            new[] { "RDC_CD","MAJ_CAT","DEL_PEND_Q","DATE" },
            new[] { typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
            new[] { "RdcCd","MajCat","DelPendQ","Date(yyyy-MM-dd)" },
            new[] { "RDC01","APPAREL","75","2026-04-01" }),

        ["GrtContribution"] = new(
            "GRT Contribution (48 Weeks)",
            "MASTER_GRT_CONTRIBUTION",
            new string[] { "SSN" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK_{w}")).ToArray(),
            new Type[] { typeof(string) }.Concat(Enumerable.Repeat(typeof(decimal), 48)).ToArray(),
            new string[] { "SSN" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK_{w}")).ToArray(),
            new string[] { "A" }.Concat(Enumerable.Repeat("0.35", 48)).ToArray()),

        ["ProductHierarchy"] = new(
            "Product Hierarchy",
            "MASTER_PRODUCT_HIERARCHY",
            new[] { "SEG","DIV","SUB_DIV","MAJ_CAT_NM","SSN" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(string),typeof(string) },
            new[] { "Seg","Div","SubDiv","MajCatNm","Ssn" },
            new[] { "APP","MENS","MU","M_PW_SHIRT_FS","A" }),

        // ── Contribution Tables (4) ──
        ["ContMacroMvgr"] = new("Contribution Macro MVGR", "ST_MAJ_CAT_MACRO_MVGR_PLAN",
            new[] { "ST_CD","MAJ_CAT_CD","DISP_MVGR_MATRIX","CONT_PCT" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal) },
            new[] { "StCd","MajCatCd","DispMvgrMatrix","ContPct" },
            new[] { "HA10","IB_B_SUIT_FS","WSH","0.01" }),

        ["ContSz"] = new("Contribution Size", "ST_MAJ_CAT_SZ_PLAN",
            new[] { "ST_CD","MAJ_CAT_CD","SZ","CONT_PCT" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal) },
            new[] { "StCd","MajCatCd","Sz","ContPct" },
            new[] { "HA10","IB_B_SUIT_FS","O-3M","0.16" }),

        ["ContSeg"] = new("Contribution Segment", "ST_MAJ_CAT_SEG_PLAN",
            new[] { "ST_CD","MAJ_CAT_CD","SEG","CONT_PCT" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal) },
            new[] { "StCd","MajCatCd","Seg","ContPct" },
            new[] { "HA10","FW_W_FUR_SLIPPER","GM","0.01" }),

        ["ContVnd"] = new("Contribution Vendor", "ST_MAJ_CAT_VND_PLAN",
            new[] { "ST_CD","MAJ_CAT_CD","M_VND_CD","CONT_PCT" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal) },
            new[] { "StCd","MajCatCd","MVndCd","ContPct" },
            new[] { "HA10","IB_ROMPER_SU","200854","0.50" }),

        // ── Sub Store Stock (4) ──
        ["SubStStkMvgr"] = new("Sub St Stk MVGR", "SUB_ST_STK_MVGR",
            new[] { "ST_CD","MAJ_CAT","SUB_VALUE","STK_QTY","DATE" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
            new[] { "StCd","MajCat","SubValue","StkQty","Date(yyyy-MM-dd)" },
            new[] { "HA10","IB_B_SUIT_FS","WSH","120","2026-04-01" }),

        ["SubStStkSz"] = new("Sub St Stk Size", "SUB_ST_STK_SZ",
            new[] { "ST_CD","MAJ_CAT","SUB_VALUE","STK_QTY","DATE" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
            new[] { "StCd","MajCat","SubValue","StkQty","Date(yyyy-MM-dd)" },
            new[] { "HA10","IB_B_SUIT_FS","O-3M","80","2026-04-01" }),

        ["SubStStkSeg"] = new("Sub St Stk Segment", "SUB_ST_STK_SEG",
            new[] { "ST_CD","MAJ_CAT","SUB_VALUE","STK_QTY","DATE" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
            new[] { "StCd","MajCat","SubValue","StkQty","Date(yyyy-MM-dd)" },
            new[] { "HA10","FW_K_FUR_SLIPPER","GM","90","2026-04-01" }),

        ["SubStStkVnd"] = new("Sub St Stk Vendor", "SUB_ST_STK_VND",
            new[] { "ST_CD","MAJ_CAT","SUB_VALUE","STK_QTY","DATE" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
            new[] { "StCd","MajCat","SubValue","StkQty","Date(yyyy-MM-dd)" },
            new[] { "HA10","IB_ROMPER_SU","200854","60","2026-04-01" }),

        // ── Sub DC Stock (4) ──
        ["SubDcStkMvgr"] = new("Sub DC Stk MVGR", "SUB_DC_STK_MVGR",
            new[] { "RDC_CD","MAJ_CAT","SUB_VALUE","DC_STK_Q","GRT_STK_Q","W_GRT_STK_Q","DATE" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(decimal),typeof(decimal),typeof(DateTime) },
            new[] { "RdcCd","MajCat","SubValue","DcStkQ","GrtStkQ","WGrtStkQ","Date(yyyy-MM-dd)" },
            new[] { "DW01","IB_B_SUIT_FS","WSH","500","200","150","2026-04-01" }),

        ["SubDcStkSz"] = new("Sub DC Stk Size", "SUB_DC_STK_SZ",
            new[] { "RDC_CD","MAJ_CAT","SUB_VALUE","DC_STK_Q","GRT_STK_Q","W_GRT_STK_Q","DATE" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(decimal),typeof(decimal),typeof(DateTime) },
            new[] { "RdcCd","MajCat","SubValue","DcStkQ","GrtStkQ","WGrtStkQ","Date(yyyy-MM-dd)" },
            new[] { "DW01","IB_B_SUIT_FS","O-3M","400","180","120","2026-04-01" }),

        ["SubDcStkSeg"] = new("Sub DC Stk Segment", "SUB_DC_STK_SEG",
            new[] { "RDC_CD","MAJ_CAT","SUB_VALUE","DC_STK_Q","GRT_STK_Q","W_GRT_STK_Q","DATE" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(decimal),typeof(decimal),typeof(DateTime) },
            new[] { "RdcCd","MajCat","SubValue","DcStkQ","GrtStkQ","WGrtStkQ","Date(yyyy-MM-dd)" },
            new[] { "DW01","FW_K_FUR_SLIPPER","GM","350","150","100","2026-04-01" }),

        ["SubDcStkVnd"] = new("Sub DC Stk Vendor", "SUB_DC_STK_VND",
            new[] { "RDC_CD","MAJ_CAT","SUB_VALUE","DC_STK_Q","GRT_STK_Q","W_GRT_STK_Q","DATE" },
            new[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(decimal),typeof(decimal),typeof(DateTime) },
            new[] { "RdcCd","MajCat","SubValue","DcStkQ","GrtStkQ","WGrtStkQ","Date(yyyy-MM-dd)" },
            new[] { "DW01","IB_ROMPER_SU","200854","300","120","80","2026-04-01" }),
    };

    public static TableDef? Get(string key) => Tables.GetValueOrDefault(key);
}
