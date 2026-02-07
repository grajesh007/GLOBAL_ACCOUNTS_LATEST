# Kapil Group ERP API

## Project Structure

This solution contains three projects:

1. **Kapil_Group_ERP_API** - The main Web API project
2. **Kapil_Group_Infrastructure** - Data access layer with Entity Framework Core
3. **Kapil_Group_Repository** - Repository pattern interfaces and entities

## Technology Stack

- .NET Core 8.0
- Entity Framework Core 8.0
- SQL Server
- Swagger/OpenAPI

## Prerequisites

- Visual Studio 2019 or later
- .NET 8.0 SDK
- SQL Server

## Getting Started

### 1. Open the Solution
- Open `KapilGroupERP.sln` in Visual Studio 2019

### 2. Update Database Connection String
- Navigate to `Kapil_Group_ERP_API/appsettings.json`
- Update the `DefaultConnection` connection string with your SQL Server details:
```json
"ConnectionStrings": {
  "DefaultConnection": "Server=YOUR_SERVER;Database=KapilGroupERP;User Id=YOUR_USER;Password=YOUR_PASSWORD;TrustServerCertificate=True;"
}
```

### 3. Register Services in Program.cs
Add the following to `Program.cs` after `var builder = WebApplication.CreateBuilder(args);`:

```csharp
// Add DbContext
builder.Services.AddDbContext<ApplicationDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));

// Add Repository and UnitOfWork
builder.Services.AddScoped(typeof(IRepository<>), typeof(Repository<>));
builder.Services.AddScoped<IUnitOfWork, UnitOfWork>();
```

Don't forget to add the required using statements:
```csharp
using Kapil_Group_Infrastructure.Data;
using Kapil_Group_Infrastructure.Repositories;
using Kapil_Group_Repository.Interfaces;
using Microsoft.EntityFrameworkCore;
```

### 4. Create Your First Migration
Open Package Manager Console and run:
```
Add-Migration InitialCreate -Project Kapil_Group_Infrastructure
Update-Database -Project Kapil_Group_Infrastructure
```

### 5. Run the Application
- Set `Kapil_Group_ERP_API` as the startup project
- Press F5 to run
- Swagger UI will open at `https://localhost:{port}/swagger`

## Project Architecture

### Repository Pattern
- `IRepository<T>` - Generic repository interface
- `Repository<T>` - Generic repository implementation
- `IUnitOfWork` - Unit of work interface for transaction management
- `UnitOfWork` - Unit of work implementation

### Base Entity
All entities should inherit from `BaseEntity` which provides:
- Id
- CreatedDate
- ModifiedDate
- CreatedBy
- ModifiedBy
- IsActive

### Example Entity Creation
```csharp
// In Kapil_Group_Repository/Entities
public class User : BaseEntity
{
    public string Name { get; set; }
    public string Email { get; set; }
}

// Add to ApplicationDbContext
public DbSet<User> Users { get; set; }
```

### Example Controller Usage
```csharp
[ApiController]
[Route("api/[controller]")]
public class UsersController : ControllerBase
{
    private readonly IUnitOfWork _unitOfWork;

    public UsersController(IUnitOfWork unitOfWork)
    {
        _unitOfWork = unitOfWork;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll()
    {
        var users = await _unitOfWork.Repository<User>().GetAllAsync();
        return Ok(users);
    }
}
```

## Next Steps

1. Create your entity models in `Kapil_Group_Repository/Entities`
2. Add DbSets to `ApplicationDbContext`
3. Create migrations and update database
4. Create controllers in `Kapil_Group_ERP_API/Controllers`
5. Implement business logic

## Notes

- The solution uses the Repository and Unit of Work patterns for data access
- All projects target .NET 8.0
- Entity Framework Core is used for ORM
- Swagger is configured for API documentation
