using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Data;

public class PlanningDbContext : DbContext
{
    public PlanningDbContext(DbContextOptions<PlanningDbContext> options)
        : base(options)
    {
    }

    public DbSet<WeekCalendar> WeekCalendars { get; set; }
    public DbSet<StoreMaster> StoreMasters { get; set; }
    public DbSet<BinCapacity> BinCapacities { get; set; }
    public DbSet<SaleQty> SaleQties { get; set; }
    public DbSet<DispQty> DispQties { get; set; }
    public DbSet<StoreStock> StoreStocks { get; set; }
    public DbSet<DcStock> DcStocks { get; set; }
    public DbSet<TrfInPlan> TrfInPlans { get; set; }
    public DbSet<PurchasePlan> PurchasePlans { get; set; }
    public DbSet<DelPending> DelPendings { get; set; }
    public DbSet<GrtContribution> GrtContributions { get; set; }
    public DbSet<ProductHierarchy> ProductHierarchies { get; set; }
    public DbSet<ContMacroMvgr> ContMacroMvgrs { get; set; }
    public DbSet<ContSz> ContSzs { get; set; }
    public DbSet<ContSeg> ContSegs { get; set; }
    public DbSet<ContVnd> ContVnds { get; set; }
    public DbSet<SubStStkMvgr> SubStStkMvgrs { get; set; }
    public DbSet<SubStStkSz> SubStStkSzs { get; set; }
    public DbSet<SubStStkSeg> SubStStkSegs { get; set; }
    public DbSet<SubStStkVnd> SubStStkVnds { get; set; }
    public DbSet<SubDcStkMvgr> SubDcStkMvgrs { get; set; }
    public DbSet<SubDcStkSz> SubDcStkSzs { get; set; }
    public DbSet<SubDcStkSeg> SubDcStkSegs { get; set; }
    public DbSet<SubDcStkVnd> SubDcStkVnds { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Configure SaleQty composite key
        modelBuilder.Entity<SaleQty>()
            .HasKey(x => new { x.StCd, x.MajCat });

        // Configure DispQty composite key
        modelBuilder.Entity<DispQty>()
            .HasKey(x => new { x.StCd, x.MajCat });

        // Configure table mappings with proper schema
        modelBuilder.Entity<WeekCalendar>()
            .ToTable("WEEK_CALENDAR", schema: "dbo");

        modelBuilder.Entity<StoreMaster>()
            .ToTable("MASTER_ST_MASTER", schema: "dbo");

        modelBuilder.Entity<BinCapacity>()
            .ToTable("MASTER_BIN_CAPACITY", schema: "dbo");

        modelBuilder.Entity<SaleQty>()
            .ToTable("QTY_SALE_QTY", schema: "dbo");

        modelBuilder.Entity<DispQty>()
            .ToTable("QTY_DISP_QTY", schema: "dbo");

        modelBuilder.Entity<StoreStock>()
            .ToTable("QTY_ST_STK_Q", schema: "dbo");

        modelBuilder.Entity<DcStock>()
            .ToTable("QTY_MSA_AND_GRT", schema: "dbo");

        modelBuilder.Entity<TrfInPlan>()
            .ToTable("TRF_IN_PLAN", schema: "dbo");

        modelBuilder.Entity<PurchasePlan>()
            .ToTable("PURCHASE_PLAN", schema: "dbo");

        modelBuilder.Entity<DelPending>()
            .ToTable("QTY_DEL_PENDING", schema: "dbo");

        modelBuilder.Entity<GrtContribution>()
            .ToTable("MASTER_GRT_CONTRIBUTION", schema: "dbo");

        modelBuilder.Entity<ProductHierarchy>()
            .ToTable("MASTER_PRODUCT_HIERARCHY", schema: "dbo");

        modelBuilder.Entity<ContMacroMvgr>()
            .ToTable("ST_MAJ_CAT_MACRO_MVGR_PLAN", schema: "dbo");

        modelBuilder.Entity<ContSz>()
            .ToTable("ST_MAJ_CAT_SZ_PLAN", schema: "dbo");

        modelBuilder.Entity<ContSeg>()
            .ToTable("ST_MAJ_CAT_SEG_PLAN", schema: "dbo");

        modelBuilder.Entity<ContVnd>()
            .ToTable("ST_MAJ_CAT_VND_PLAN", schema: "dbo");

        modelBuilder.Entity<SubStStkMvgr>()
            .ToTable("SUB_ST_STK_MVGR", schema: "dbo");

        modelBuilder.Entity<SubStStkSz>()
            .ToTable("SUB_ST_STK_SZ", schema: "dbo");

        modelBuilder.Entity<SubStStkSeg>()
            .ToTable("SUB_ST_STK_SEG", schema: "dbo");

        modelBuilder.Entity<SubStStkVnd>()
            .ToTable("SUB_ST_STK_VND", schema: "dbo");

        modelBuilder.Entity<SubDcStkMvgr>()
            .ToTable("SUB_DC_STK_MVGR", schema: "dbo");

        modelBuilder.Entity<SubDcStkSz>()
            .ToTable("SUB_DC_STK_SZ", schema: "dbo");

        modelBuilder.Entity<SubDcStkSeg>()
            .ToTable("SUB_DC_STK_SEG", schema: "dbo");

        modelBuilder.Entity<SubDcStkVnd>()
            .ToTable("SUB_DC_STK_VND", schema: "dbo");
    }
}
