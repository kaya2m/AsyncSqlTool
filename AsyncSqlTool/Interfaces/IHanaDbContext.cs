namespace SapConnection.Components.Services.Interface
{
    public interface IHanaDbContext
    {
        Task<List<T>> QueryAsync<T>(string query) where T : class, new();
        Task<List<Dictionary<string, object>>> QueryAsDictionaryAsync(string query);
        Task<List<Dictionary<string, object>>> QueryAsDictionaryWithParamsAsync(string query, object parameters);
        Task<T> ExecuteScalarAsync<T>(string query);
        Task<int> ExecuteNonQueryAsync(string query);
        Task<List<Dictionary<string, object>>> QuerySapAsync(string query);
    }
}
