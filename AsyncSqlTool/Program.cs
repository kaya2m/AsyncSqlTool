using AsyncSqlTool.Components;
using AsyncSqlTool.Data;
using AsyncSqlTool.Services;
using Microsoft.EntityFrameworkCore;
using SapConnection.Components.Services.Interface;

var builder = WebApplication.CreateBuilder(args);
// Add services to the container.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddDevExpressBlazor(options =>
{
    options.BootstrapVersion = DevExpress.Blazor.BootstrapVersion.v5;
});

builder.Services.AddMvc();
builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();

// Entity Framework DbContext
builder.Services.AddDbContext<ApplicationDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));

// HANA DB bağlantısı - Eski servis (geriye uyumluluk için)
builder.Services.AddScoped<IHanaDbContext, HanaDbContext>();

// Yeni veritabanı servisleri
builder.Services.AddScoped<IDatabaseConnectionFactory, DatabaseConnectionFactory>();
builder.Services.AddScoped<QueryService>();

// Eski SQL Server servisi (geriye uyumluluk için)
builder.Services.AddScoped<SapDynamicQueryService>();

// DevExpress Blazor
var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

// Veritabanını oluştur (geliştirme için)
if (app.Environment.IsDevelopment())
{
    using (var scope = app.Services.CreateScope())
    {
        var services = scope.ServiceProvider;
        try
        {
            var context = services.GetRequiredService<ApplicationDbContext>();
            context.Database.EnsureCreated();
        }
        catch (Exception ex)
        {
            var logger = services.GetRequiredService<ILogger<Program>>();
            logger.LogError(ex, "Veritabanı oluşturulurken bir hata oluştu.");
        }
    }
}

app.Run();