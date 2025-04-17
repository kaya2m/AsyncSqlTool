using AsyncSqlTool.Models;
using Microsoft.Data.SqlClient;
using MySql.Data.MySqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Sap.Data.Hana;
using System.Data;

namespace AsyncSqlTool.Data
{
    public class DatabaseConnectionFactory : IDatabaseConnectionFactory
    {
        private readonly ILogger<DatabaseConnectionFactory> _logger;

        public DatabaseConnectionFactory(ILogger<DatabaseConnectionFactory> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Veritabanı türüne göre bağlantı nesnesi oluşturur
        /// </summary>
        public async Task<IDbConnection> CreateConnectionAsync(DatabaseConnection dbConnection)
        {
            if (dbConnection == null)
            {
                throw new ArgumentNullException(nameof(dbConnection));
            }

            IDbConnection connection = dbConnection.DatabaseType.ToUpper() switch
            {
                "HANA" => new HanaConnection(dbConnection.ConnectionString),
                "MSSQL" => new SqlConnection(dbConnection.ConnectionString),
                "MYSQL" => new MySqlConnection(dbConnection.ConnectionString),
                "POSTGRESQL" => new NpgsqlConnection(dbConnection.ConnectionString),
                "ORACLE" => new OracleConnection(dbConnection.ConnectionString),
                _ => throw new NotSupportedException($"Desteklenmeyen veritabanı türü: {dbConnection.DatabaseType}")
            };

            await OpenConnectionAsync(connection);
            return connection;
        }

        /// <summary>
        /// Veritabanında sorgu çalıştırır ve sonuçları döndürür
        /// </summary>
        public async Task<List<Dictionary<string, object>>> ExecuteQueryAsync(DatabaseConnection dbConnection, string query)
        {
            var results = new List<Dictionary<string, object>>();

            try
            {
                using var connection = await CreateConnectionAsync(dbConnection);
                IDbCommand command = connection.CreateCommand();
                command.CommandText = query;
                command.CommandTimeout = 120; // 2 dakika

                using var reader = await ExecuteReaderAsync(command);
                while (await ReadAsync(reader))
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var columnName = reader.GetName(i);
                        var value = reader.IsDBNull(i) ? null : reader.GetValue(i);

                        // Tarih formatını standartlaştır
                        if (value is DateTime dateTime)
                        {
                            value = dateTime.ToString("dd.MM.yyyy HH:mm:ss");
                        }

                        row[columnName] = value;
                    }
                    results.Add(row);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Veritabanı sorgusu çalıştırılırken hata oluştu: {ex.Message}");
                throw;
            }

            return results;
        }

        /// <summary>
        /// INSERT, UPDATE, DELETE gibi sorgular için
        /// </summary>
        public async Task<int> ExecuteNonQueryAsync(DatabaseConnection dbConnection, string query)
        {
            try
            {
                using var connection = await CreateConnectionAsync(dbConnection);
                IDbCommand command = connection.CreateCommand();
                command.CommandText = query;
                command.CommandTimeout = 120; // 2 dakika

                return await ExecuteNonQueryAsync(command);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Veritabanı non-query çalıştırılırken hata oluştu: {ex.Message}");
                throw;
            }
        }

        #region Yardımcı Metotlar

        private async Task OpenConnectionAsync(IDbConnection connection)
        {
            if (connection is SqlConnection sqlConnection)
            {
                await sqlConnection.OpenAsync();
            }
            else if (connection is HanaConnection hanaConnection)
            {
                await hanaConnection.OpenAsync();
            }
            else if (connection is MySqlConnection mySqlConnection)
            {
                await mySqlConnection.OpenAsync();
            }
            else if (connection is NpgsqlConnection npgsqlConnection)
            {
                await npgsqlConnection.OpenAsync();
            }
            else if (connection is OracleConnection oracleConnection)
            {
                await oracleConnection.OpenAsync();
            }
            else
            {
                connection.Open();
            }
        }

        private async Task<IDataReader> ExecuteReaderAsync(IDbCommand command)
        {
            if (command is SqlCommand sqlCommand)
            {
                return await sqlCommand.ExecuteReaderAsync();
            }
            else if (command is HanaCommand hanaCommand)
            {
                return await hanaCommand.ExecuteReaderAsync();
            }
            else if (command is MySqlCommand mySqlCommand)
            {
                return await mySqlCommand.ExecuteReaderAsync();
            }
            else if (command is NpgsqlCommand npgsqlCommand)
            {
                return await npgsqlCommand.ExecuteReaderAsync();
            }
            else if (command is OracleCommand oracleCommand)
            {
                return await oracleCommand.ExecuteReaderAsync();
            }
            else
            {
                return command.ExecuteReader();
            }
        }

        private async Task<bool> ReadAsync(IDataReader reader)
        {
            if (reader is SqlDataReader sqlReader)
            {
                return await sqlReader.ReadAsync();
            }
            else if (reader is HanaDataReader hanaReader)
            {
                return await hanaReader.ReadAsync();
            }
            else if (reader is MySqlDataReader mySqlReader)
            {
                return await mySqlReader.ReadAsync();
            }
            else if (reader is NpgsqlDataReader npgsqlReader)
            {
                return await npgsqlReader.ReadAsync();
            }
            else if (reader is OracleDataReader oracleReader)
            {
                return await oracleReader.ReadAsync();
            }
            else
            {
                return reader.Read();
            }
        }

        private async Task<int> ExecuteNonQueryAsync(IDbCommand command)
        {
            if (command is SqlCommand sqlCommand)
            {
                return await sqlCommand.ExecuteNonQueryAsync();
            }
            else if (command is HanaCommand hanaCommand)
            {
                return await hanaCommand.ExecuteNonQueryAsync();
            }
            else if (command is MySqlCommand mySqlCommand)
            {
                return await mySqlCommand.ExecuteNonQueryAsync();
            }
            else if (command is NpgsqlCommand npgsqlCommand)
            {
                return await npgsqlCommand.ExecuteNonQueryAsync();
            }
            else if (command is OracleCommand oracleCommand)
            {
                return await oracleCommand.ExecuteNonQueryAsync();
            }
            else
            {
                return command.ExecuteNonQuery();
            }
        }

        #endregion
    }
}
