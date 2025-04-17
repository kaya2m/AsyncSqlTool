using AsyncSqlTool.Models;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Reflection.Emit;

namespace AsyncSqlTool.Data
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
        : base(options)
        {
        }

        public DbSet<DatabaseConnection> DatabaseConnections { get; set; }
        public DbSet<SavedQuery> SavedQueries { get; set; }
        public DbSet<QueryColumnMapping> QueryColumnMappings { get; set; }
        public DbSet<QueryExecutionLog> QueryExecutionLogs { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<SavedQuery>()
                .HasOne(q => q.DatabaseConnection)
                .WithMany(d => d.SavedQueries)
                .HasForeignKey(q => q.DatabaseConnectionId)
                .OnDelete(DeleteBehavior.Cascade);

            modelBuilder.Entity<QueryColumnMapping>()
                .HasOne(c => c.SavedQuery)
                .WithMany(q => q.ColumnMappings)
                .HasForeignKey(c => c.SavedQueryId)
                .OnDelete(DeleteBehavior.Cascade);

            modelBuilder.Entity<QueryExecutionLog>()
                .HasOne(l => l.SavedQuery)
                .WithMany(q => q.ExecutionLogs)
                .HasForeignKey(l => l.SavedQueryId)
                .OnDelete(DeleteBehavior.Cascade);

            // Demo veri
            modelBuilder.Entity<DatabaseConnection>().HasData(
                new DatabaseConnection
                {
                    Id = 1,
                    Name = "SAP HANA",
                    DatabaseType = "HANA",
                    ConnectionString = "Server=192.168.0.62:30013;DATABASENAME=NDB;Current Schema=ASDCANLI;UserName=BIQUERYUSR;Password=A2n!#qw185%",
                    Description = "SAP HANA veritabanı bağlantısı",
                    IsActive = true
                },
                new DatabaseConnection
                {
                    Id = 2,
                    Name = "SQL Server",
                    DatabaseType = "MSSQL",
                    ConnectionString = "Data Source=ASDDEV;Initial Catalog=SAPHANADB;Persist Security Info=True;User ID=asd;Password=teori.123;MultipleActiveResultSets=true;TrustServerCertificate=True",
                    Description = "SQL Server veritabanı bağlantısı",
                    IsActive = true
                }
            );
        }
    }
}