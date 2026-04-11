using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Data;

public class DataV2DbContext : DbContext
{
    public DataV2DbContext(DbContextOptions<DataV2DbContext> options) : base(options) { }

    public DbSet<ArsStMjDisplayMaster> ArsDisplayMasters { get; set; }
    public DbSet<ArsStMjAutoSale> ArsAutoSales { get; set; }
    public DbSet<ArsStArtAutoSale> ArsArtAutoSales { get; set; }
    public DbSet<ArsHoldDaysMaster> ArsHoldDays { get; set; }
    public DbSet<ArsStMaster> ArsStMasters { get; set; }
    public DbSet<EtStockData> EtStockData { get; set; }
    public DbSet<ViewEtMsaStock> ViewEtMsaStock { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ArsStMjDisplayMaster>()
            .HasIndex(e => new { e.St, e.Mj }).IsUnique();

        modelBuilder.Entity<ArsStMjAutoSale>()
            .HasIndex(e => new { e.St, e.Mj }).IsUnique();

        modelBuilder.Entity<ArsStArtAutoSale>()
            .HasIndex(e => new { e.St, e.GenArt, e.Clr }).IsUnique();

        modelBuilder.Entity<ArsHoldDaysMaster>()
            .HasIndex(e => new { e.St, e.Mj }).IsUnique();

        modelBuilder.Entity<ArsStMaster>()
            .HasIndex(e => e.StCd).IsUnique();
    }
}
