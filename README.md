# Transfer In Plan Management System

A complete ASP.NET Core 8.0 MVC web application for managing Transfer In Plans with MSSQL database integration.

## Features

- **Dashboard**: Summary statistics with charts showing transfer in trends by category and week
- **Reference Table Management**: CRUD operations for 8 reference tables
- **Plan Generation**: Execute stored procedure `SP_GENERATE_TRF_IN_PLAN` with parameterized execution
- **Plan Output Viewer**: View and filter generated plans with pivot functionality
- **Excel Export**: Export plan data to Excel format using EPPlus
- **Professional UI**: Modern Bootstrap 5.3 layout with responsive design and dark sidebar navigation

## System Requirements

- .NET 8.0 SDK or later
- SQL Server 2016 or later
- Visual Studio 2022 or Visual Studio Code
- Modern web browser (Chrome, Edge, Firefox, Safari)

## Project Structure

```
TRANSFER_IN_PLAN/
├── Controllers/              # MVC Controllers
│   ├── HomeController.cs
│   ├── WeekCalendarController.cs
│   ├── StoreMasterController.cs
│   ├── BinCapacityController.cs
│   ├── SaleQtyController.cs
│   ├── DispQtyController.cs
│   ├── StoreStockController.cs
│   ├── DcStockController.cs
│   ├── PlanController.cs
│   └── ErrorController.cs
├── Models/                   # Data Models
│   ├── WeekCalendar.cs
│   ├── StoreMaster.cs
│   ├── BinCapacity.cs
│   ├── SaleQty.cs
│   ├── DispQty.cs
│   ├── StoreStock.cs
│   ├── DcStock.cs
│   ├── TrfInPlan.cs
│   ├── SpExecutionParams.cs
│   └── DashboardViewModel.cs
├── Data/                     # Database Context
│   └── PlanningDbContext.cs
├── Services/                 # Business Logic
│   └── PlanService.cs
├── Views/                    # Razor Views
│   ├── Home/
│   ├── Plan/
│   ├── WeekCalendar/
│   ├── StoreMaster/
│   ├── BinCapacity/
│   ├── SaleQty/
│   ├── DispQty/
│   ├── StoreStock/
│   ├── DcStock/
│   └── Shared/
├── wwwroot/                  # Static Files
│   ├── css/site.css
│   └── js/site.js
├── Properties/
│   └── launchSettings.json
├── Program.cs                # Application Entry Point
├── appsettings.json          # Configuration File
├── TRANSFER_IN_PLAN.csproj   # Project File
└── README.md                 # This File
```

## Getting Started

### 1. Clone or Download the Project

```bash
cd /sessions/beautiful-confident-clarke/TRANSFER_IN_PLAN
```

### 2. Configure the Database Connection

Edit `appsettings.json` and update the connection string:

```json
{
  "ConnectionStrings": {
    "PlanningDatabase": "Server=YOUR_SERVER;Database=planning;User Id=YOUR_USER;Password=YOUR_PASSWORD;TrustServerCertificate=True;"
  }
}
```

Replace:
- `YOUR_SERVER`: Your SQL Server instance name
- `YOUR_USER`: Database user login
- `YOUR_PASSWORD`: Database user password

### 3. Restore NuGet Packages

```bash
dotnet restore
```

### 4. Run Database Migrations

```bash
dotnet ef database update
```

If migrations don't exist yet, create them:

```bash
dotnet ef migrations add InitialCreate
dotnet ef database update
```

### 5. Build the Application

```bash
dotnet build
```

### 6. Run the Application

```bash
dotnet run
```

The application will be available at:
- HTTP: `http://localhost:5241`
- HTTPS: `https://localhost:7123`

## Database Schema

### Reference Tables

1. **WEEK_CALENDAR** - Weekly fiscal calendar
2. **MASTER_ST_MASTER** - Store master data
3. **MASTER_BIN_CAPACITY** - Bin capacity by category
4. **QTY_SALE_QTY** - Weekly sales quantity (48 weeks)
5. **QTY_DISP_QTY** - Weekly display quantity (48 weeks)
6. **QTY_ST_STK_Q** - Store stock quantities
7. **QTY_MSA_AND_GRT** - DC/GRT stock quantities
8. **TRF_IN_PLAN** - Transfer in plan output

## Key Features

### Dashboard
- Total stores count
- Total categories count
- Total plan rows
- Last execution date/time
- Bar chart: Transfer in quantity by category
- Line chart: Weekly transfer in trend
- Tables: Top 10 short and excess stores

### Plan Execution
Execute the stored procedure with parameters:
- Start Week ID
- End Week ID
- Optional: Store Code filter
- Optional: Major Category filter
- Cover Days CM1 (default: 14)
- Cover Days CM2 (default: 0)

### Plan Output Viewer
- Filter by week range
- Filter by store code
- Filter by major category
- DataTable with sorting and pagination
- Export to Excel functionality

### CRUD Operations
Full CRUD operations for all 8 reference tables with:
- Data validation
- Error handling
- Responsive forms
- Data tables with pagination and search

## API Endpoints

### Home
- `GET /` - Dashboard

### Reference Tables
- `GET /WeekCalendar` - List
- `GET /WeekCalendar/Create` - Create Form
- `POST /WeekCalendar/Create` - Save
- `GET /WeekCalendar/Edit/{id}` - Edit Form
- `POST /WeekCalendar/Edit/{id}` - Update
- `GET /WeekCalendar/Delete/{id}` - Delete Confirmation
- `POST /WeekCalendar/Delete/{id}` - Delete

Similar endpoints for:
- `/StoreMaster`
- `/BinCapacity`
- `/SaleQty`
- `/DispQty`
- `/StoreStock`
- `/DcStock`

### Plan Management
- `GET /Plan/Execute` - Plan execution form
- `POST /Plan/Execute` - Execute stored procedure
- `GET /Plan/Output` - View plan output
- `POST /Plan/ExportExcel` - Export to Excel

## Configuration Files

### Program.cs
- Configures Entity Framework Core
- Registers DbContext
- Configures MVC and Newtonsoft JSON
- Sets up routing

### appsettings.json
- Database connection string
- Logging configuration

### launchSettings.json
- Development server configuration
- HTTPS settings
- IIS Express configuration

## Security Considerations

1. **Connection String**: Keep credentials secure, use environment variables in production
2. **SQL Injection**: Uses Entity Framework Core parameterized queries
3. **CSRF Protection**: Enabled via AntiForgeryToken in forms
4. **HTTPS**: Configured for secure communication
5. **Authentication**: Add ASP.NET Core Identity for production use

## Performance Notes

1. **DataTables**: Configured with pagination (25 rows per page)
2. **Database Queries**: Uses async/await for non-blocking operations
3. **Indexes**: Ensure proper database indexes on frequently queried columns
4. **Caching**: Consider adding output caching for dashboard data

## Troubleshooting

### Connection String Errors
- Verify SQL Server is running
- Check server name and credentials
- Ensure database `planning` exists
- Verify user has appropriate permissions

### Migration Errors
- Clear existing migrations folder if starting fresh
- Run `dotnet ef database drop` to reset
- Recreate migrations from scratch

### Port Already in Use
- Change ports in `launchSettings.json`
- Or use: `dotnet run --urls "https://localhost:7124"`

## Development Notes

### Adding New Tables
1. Create model in `Models/` folder with Column attributes
2. Add DbSet in `PlanningDbContext.cs`
3. Configure in `OnModelCreating` if needed
4. Create migration: `dotnet ef migrations add AddNewTable`
5. Apply: `dotnet ef database update`

### Adding New Views
1. Create `.cshtml` file in appropriate controller folder
2. Reference model with `@model ModelName` at top
3. Use Bootstrap 5 classes for styling
4. Include DataTables initialization for tables

### JavaScript
- Site-wide JavaScript in `wwwroot/js/site.js`
- DataTables for all table grids
- Chart.js for dashboard charts
- Bootstrap for modals and tooltips

## Dependencies

- Microsoft.EntityFrameworkCore.SqlServer 8.0.0
- Microsoft.EntityFrameworkCore.Tools 8.0.0
- EPPlus 7.0.0 (Excel export)
- Microsoft.AspNetCore.Mvc.NewtonsoftJson 8.0.0
- Bootstrap 5.3 (CDN)
- Chart.js (CDN)
- DataTables (CDN)
- jQuery 3.7.0 (CDN)

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review Entity Framework Core documentation
3. Check ASP.NET Core documentation
4. Verify database schema matches models

## License

This project is provided as-is for internal use.

---

**Last Updated**: April 3, 2024
**Version**: 1.0
**Target Framework**: .NET 8.0
