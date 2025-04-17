using AsyncSqlTool.Models;
using System.Data;

namespace AsyncSqlTool.Data
{
    public interface IDatabaseConnectionFactory
    {
        Task<IDbConnection> CreateConnectionAsync(DatabaseConnection dbConnection);
        Task<List<Dictionary<string, object>>> ExecuteQueryAsync(DatabaseConnection dbConnection, string query);
        Task<int> ExecuteNonQueryAsync(DatabaseConnection dbConnection, string query);
    }
}
