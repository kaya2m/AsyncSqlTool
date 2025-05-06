using AsyncSqlTool.Components;
using AsyncSqlTool.Data;
using AsyncSqlTool.Services;
using Microsoft.EntityFrameworkCore;
using SapConnection.Components.Services.Interface;
using Quartz;

var builder = WebApplication.CreateBuilder(args);

// 1. Blazor ve MVC Bileşenlerini Kaydet
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();
builder.Services.AddMvc();
builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();
builder.Services.AddDevExpressBlazor(options =>
{
    options.BootstrapVersion = DevExpress.Blazor.BootstrapVersion.v5;
});

// 2. Veritabanı Bağlantıları ve Servisler
builder.Services.AddDbContext<ApplicationDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));
builder.Services.AddScoped<IHanaDbContext, HanaDbContext>();
builder.Services.AddScoped<IDatabaseConnectionFactory, DatabaseConnectionFactory>();

// 3. Uygulama Servisleri
builder.Services.AddScoped<QueryService>();
builder.Services.AddScoped<SapDynamicQueryService>();

// 4. Quartz Scheduler ve Bağımlı Servisler
builder.Services.AddQuartz(q =>
{
    q.UseMicrosoftDependencyInjectionJobFactory();
});

builder.Services.AddQuartzHostedService(options =>
{
    options.WaitForJobsToComplete = true;
});

// Quartz ile ilgili servisler
builder.Services.AddScoped<QueryExecutionJob>();
builder.Services.AddHostedService<QuerySchedulerHostedService>();
builder.Services.AddScoped<QuerySchedulerClient>(); 

// Uygulama Oluşturma
var app = builder.Build();

// Middleware Yapılandırması
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

// Geliştirme Ortamı Ayarları
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