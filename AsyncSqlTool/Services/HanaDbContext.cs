using Sap.Data.Hana;
using SapConnection.Components.Services.Interface;
using System.Data;

namespace AsyncSqlTool.Services
{
    public class HanaDbContext : IHanaDbContext, IDisposable
    {
        private readonly string _connectionString;
        private readonly ILogger<HanaDbContext> _logger;
        private HanaConnection _connection;
        private bool _disposed = false;

        public HanaDbContext(IConfiguration configuration, ILogger<HanaDbContext> logger)
        {
            _connectionString = configuration.GetConnectionString("HANADB");
            _logger = logger;
            // Initialize the connection
            _connection = new HanaConnection(_connectionString);
        }

        /// <summary>
        /// Generic veri çekme metodu
        /// </summary>
        public async Task<List<T>> QueryAsync<T>(string query) where T : class, new()
        {
            try
            {
                var results = new List<T>();
                // Use a new connection for thread safety
                using (var connection = new HanaConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    using (var cmd = new HanaCommand(query, connection))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        var properties = typeof(T).GetProperties();

                        while (await reader.ReadAsync())
                        {
                            var item = new T();

                            foreach (var prop in properties)
                            {
                                try
                                {
                                    // Check if the column exists before getting its index
                                    int ordinal;
                                    try
                                    {
                                        ordinal = reader.GetOrdinal(prop.Name);
                                    }
                                    catch (IndexOutOfRangeException)
                                    {
                                        continue; // Column doesn't exist, skip this property
                                    }

                                    if (!reader.IsDBNull(ordinal))
                                    {
                                        object value = reader.GetValue(ordinal);

                                        // Handle type conversions safely
                                        if (value is DateTime dateTime && prop.PropertyType == typeof(string))
                                        {
                                            value = dateTime.ToString("dd.MM.yyyy");
                                        }

                                        try
                                        {
                                            prop.SetValue(item, Convert.ChangeType(value, prop.PropertyType));
                                        }
                                        catch (InvalidCastException)
                                        {
                                            _logger.LogWarning($"Failed to convert value '{value}' to type {prop.PropertyType} for property {prop.Name}");
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogWarning(ex, $"Error setting property {prop.Name}");
                                }
                            }

                            results.Add(item);
                        }
                    }
                }
                return results;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query on HANA database");
                throw;
            }
        }

        /// <summary>
        /// Dictionary listesi olarak veri çekme metodu
        /// </summary>
        public async Task<List<Dictionary<string, object>>> QueryAsDictionaryAsync(string query)
        {
            try
            {
                var data = new List<Dictionary<string, object>>();
                using (var connection = new HanaConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    using (var cmd = new HanaCommand(query, connection))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var row = new Dictionary<string, object>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                var columnName = reader.GetName(i);
                                var value = reader.IsDBNull(i) ? null : reader.GetValue(i);

                                if (value is DateTime dateTime)
                                {
                                    value = dateTime.ToString("dd.MM.yyyy");
                                }

                                row[columnName] = value;
                            }
                            data.Add(row);
                        }
                    }
                }
                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in QueryAsDictionaryAsync method");
                throw;
            }
        }

        /// <summary>
        /// Parametreli Dictionary listesi veri çekme metodu
        /// </summary>
        public async Task<List<Dictionary<string, object>>> QueryAsDictionaryWithParamsAsync(string query, object parameters = null)
        {
            try
            {
                var data = new List<Dictionary<string, object>>();
                using (var connection = new HanaConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    using (var cmd = new HanaCommand(query, connection))
                    {
                        if (parameters != null)
                        {
                            var properties = parameters.GetType().GetProperties();
                            foreach (var prop in properties)
                            {
                                var paramName = prop.Name;
                                var paramValue = prop.GetValue(parameters);
                                cmd.Parameters.AddWithValue("@" + paramName, paramValue ?? DBNull.Value);
                            }
                        }

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var row = new Dictionary<string, object>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    var columnName = reader.GetName(i);
                                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                                    if (value is DateTime dateTime)
                                    {
                                        value = dateTime.ToString("dd.MM.yyyy");
                                    }
                                    row[columnName] = value;
                                }
                                data.Add(row);
                            }
                        }
                    }
                }
                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in QueryAsDictionaryWithParamsAsync method");
                throw;
            }
        }

        /// <summary>
        /// Tek bir değer döndüren sorgu için
        /// </summary>
        public async Task<T> ExecuteScalarAsync<T>(string query)
        {
            try
            {
                using (var connection = new HanaConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    using (var cmd = new HanaCommand(query, connection))
                    {
                        var result = await cmd.ExecuteScalarAsync();
                        return result == DBNull.Value ? default : (T)Convert.ChangeType(result, typeof(T));
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing scalar query on HANA database");
                throw;
            }
        }

        /// <summary>
        /// INSERT, UPDATE, DELETE sorguları için
        /// </summary>
        public async Task<int> ExecuteNonQueryAsync(string query)
        {
            try
            {
                using (var connection = new HanaConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    using (var cmd = new HanaCommand(query, connection))
                    {
                        return await cmd.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing non-query on HANA database");
                throw;
            }
        }

        #region IDisposable Implementation

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // Yönetilen kaynakları temizle
                    if (_connection != null)
                    {
                        if (_connection.State == ConnectionState.Open)
                        {
                            _connection.Close();
                        }
                        _connection.Dispose();
                        _connection = null;
                    }
                }

                _disposed = true;
            }
        }

        // Implement interface method to maintain compatibility
        public async Task<List<Dictionary<string, object>>> QuerySapAsync(string query)
        {
            return await QueryAsDictionaryAsync(query);
        }

        ~HanaDbContext()
        {
            Dispose(false);
        }

        #endregion
    }
}