# Transfer In Plan Management System - Installation & Deployment Guide

## Quick Start

### Prerequisites
- .NET 8.0 SDK or later
- SQL Server 2016 or later
- Visual Studio 2022 / VS Code (optional)
- Admin rights to install packages

### Step 1: Database Connection Setup

1. Open `appsettings.json`
2. Update the connection string:
```json
"ConnectionStrings": {
  "PlanningDatabase": "Server=YOUR_SERVER;Database=planning;User Id=YOUR_USER;Password=YOUR_PASSWORD;TrustServerCertificate=True;"
}
```

Replace:
- `YOUR_SERVER`: Your SQL Server name or IP
- `YOUR_USER`: Database login username
- `YOUR_PASSWORD`: Database login password

### Step 2: Install Dependencies

```bash
cd TRANSFER_IN_PLAN
dotnet restore
```

### Step 3: Apply Database Migrations

```bash
dotnet ef database update
```

This will create/update all tables based on the models.

### Step 4: Build the Application

```bash
dotnet build
```

### Step 5: Run the Application

```bash
dotnet run
```

The application will start on:
- HTTPS: `https://localhost:7123`
- HTTP: `http://localhost:5241`

## Production Deployment

### Using IIS

1. Publish the application:
```bash
dotnet publish -c Release -o ./publish
```

2. Create an IIS Application Pool (.NET Integrated Pipeline)
3. Create an IIS Website pointing to the published folder
4. Update connection string in `appsettings.json`
5. Configure HTTPS certificate
6. Start the application pool

### Using Docker

1. Create a Dockerfile:
```dockerfile
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY publish/ .
ENTRYPOINT ["dotnet", "TRANSFER_IN_PLAN.dll"]
```

2. Build and run:
```bash
dotnet publish -c Release -o ./publish
docker build -t transfer-in-plan .
docker run -p 80:80 -e ConnectionStrings:PlanningDatabase="..." transfer-in-plan
```

### Using Azure

1. Create an Azure App Service (ASP.NET Core)
2. Configure Application Settings with connection string
3. Deploy from Visual Studio or GitHub

## Troubleshooting

### Connection Timeout
- Verify SQL Server is accessible
- Check firewall rules
- Confirm credentials are correct
- Test connection with SSMS first

### Migration Errors
```bash
# Reset database (CAUTION: Data loss!)
dotnet ef database drop
dotnet ef database update
```

### Port Already in Use
Edit `launchSettings.json` and change ports, or:
```bash
dotnet run --urls "https://localhost:7124;http://localhost:5242"
```

### Missing Packages
```bash
dotnet add package Microsoft.EntityFrameworkCore.SqlServer --version 8.0.0
dotnet add package EPPlus --version 7.0.0
```

## Configuration Files

### appsettings.json
```json
{
  "ConnectionStrings": {
    "PlanningDatabase": "..."
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information"
    }
  }
}
```

### appsettings.Production.json (create for production)
```json
{
  "ConnectionStrings": {
    "PlanningDatabase": "..."
  },
  "Logging": {
    "LogLevel": {
      "Default": "Warning"
    }
  }
}
```

## Database Schema

Ensure these tables exist in SQL Server [planning] database:

1. `WEEK_CALENDAR` - Weekly calendar
2. `MASTER_ST_MASTER` - Store master
3. `MASTER_BIN_CAPACITY` - Bin capacity
4. `QTY_SALE_QTY` - Sales quantities
5. `QTY_DISP_QTY` - Display quantities
6. `QTY_ST_STK_Q` - Store stock
7. `QTY_MSA_AND_GRT` - DC stock
8. `TRF_IN_PLAN` - Transfer in plans (output table)

The application will create these tables automatically on first migration.

## Stored Procedure

Ensure this stored procedure exists:
```
SP_GENERATE_TRF_IN_PLAN
```

Parameters:
- @StartWeekID (int)
- @EndWeekID (int)
- @StoreCode (nvarchar, nullable)
- @MajCat (nvarchar, nullable)
- @CoverDaysCM1 (decimal)
- @CoverDaysCM2 (decimal)

## Security Best Practices

### Development
- Store connection string in `appsettings.Development.json` (not in version control)
- Use SQL Server authentication with limited permissions
- Enable HTTPS locally

### Production
- Store connection string in environment variables or Azure Key Vault
- Use strong database passwords
- Implement ASP.NET Core Identity for authentication
- Configure HTTPS with valid certificate
- Set `ASPNETCORE_ENVIRONMENT` to "Production"
- Enable logging for auditing

### Network
- Configure firewall to allow only necessary ports
- Use VPN for remote database access
- Implement rate limiting for API endpoints
- Use Web Application Firewall (WAF)

## Monitoring

### Logging
Logs are configured in `appsettings.json`:
```json
"Logging": {
  "LogLevel": {
    "Default": "Information",
    "Microsoft.EntityFrameworkCore": "Information"
  }
}
```

### Performance
- Monitor SQL Server query performance
- Check IIS CPU and memory usage
- Monitor application pool recycling
- Track database connection pool status

## Backup & Recovery

### Database Backup
```bash
# SQL Server Management Studio
Backup Database [planning] to Disk = 'C:\Backups\planning.bak'

# Or via PowerShell
Backup-SqlDatabase -ServerInstance "SERVER" -Database "planning" -BackupFile "C:\Backups\planning.bak"
```

### Application Backup
- Backup published files directory
- Backup configuration files (appsettings.json)
- Backup database

## Updates & Maintenance

### Update NuGet Packages
```bash
dotnet list package --outdated
dotnet package update
```

### Update .NET Runtime
```bash
dotnet --list-sdks
# Install new SDK from microsoft.com
```

### Rollback Plan
1. Keep previous published version
2. Update connection string if needed
3. Restore from database backup if required
4. Restart IIS application pool

## Support Resources

- [ASP.NET Core Documentation](https://docs.microsoft.com/en-us/aspnet/core/)
- [Entity Framework Core Documentation](https://docs.microsoft.com/en-us/ef/core/)
- [SQL Server Documentation](https://docs.microsoft.com/en-us/sql/)
- [Bootstrap Documentation](https://getbootstrap.com/)
- [Chart.js Documentation](https://www.chartjs.org/)

---

**Version**: 1.0
**Last Updated**: April 3, 2024
