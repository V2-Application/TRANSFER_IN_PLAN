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
    }
}
