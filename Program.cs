using Microsoft.AspNetCore.Http.Features;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Services;
using Newtonsoft.Json;

var builder = WebApplication.CreateBuilder(args);

// Configure structured logging
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddDebug();

// ── Increase upload limits for large Excel files (30k rows) ──
builder.Services.Configure<FormOptions>(options =>
{
    options.MultipartBodyLengthLimit = 500 * 1024 * 1024; // 500 MB
    options.ValueLengthLimit         = int.MaxValue;
    options.MultipartHeadersLengthLimit = int.MaxValue;
});

// Kestrel: allow large request bodies
builder.WebHost.ConfigureKestrel(serverOptions =>
{
    serverOptions.Limits.MaxRequestBodySize = 500 * 1024 * 1024; // 500 MB
    serverOptions.Limits.KeepAliveTimeout   = TimeSpan.FromMinutes(10);
    serverOptions.Limits.RequestHeadersTimeout = TimeSpan.FromMinutes(5);
});

// Add services to the container
builder.Services.AddDbContext<PlanningDbContext>(options =>
{
    options.UseSqlServer(
        builder.Configuration.GetConnectionString("PlanningDatabase"),
        sqlOptions => sqlOptions.CommandTimeout(300) // 5-minute SQL command timeout
    );
});

builder.Services.AddMemoryCache();
builder.Services.AddScoped<PlanService>();
builder.Services.AddSingleton<PlanJobService>();
builder.Services.AddSingleton<SubLevelJobService>();

// Add session for TempData support
builder.Services.AddSession(options =>
{
    options.IdleTimeout      = TimeSpan.FromMinutes(30);
    options.Cookie.HttpOnly  = true;
    options.Cookie.IsEssential = true;
});

builder.Services.AddControllersWithViews()
    .AddNewtonsoftJson(options =>
    {
        options.SerializerSettings.DateFormatString   = "yyyy-MM-dd";
        options.SerializerSettings.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;
    });

var app = builder.Build();

// Configure the HTTP request pipeline
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseRouting();
app.UseSession();
app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");

app.Run();
