using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using SapConnection.Components.Services.Interface;

namespace AsyncSqlTool.Services
{
    public class SapDynamicQueryService
    {
        private readonly IHanaDbContext _hanaDbContext;
        private readonly string _sqlConnectionString;
        private readonly ILogger<SapDynamicQueryService> _logger;

        public SapDynamicQueryService(
            IHanaDbContext hanaDbContext,
            IConfiguration configuration,
            ILogger<SapDynamicQueryService> logger)
        {
            _hanaDbContext = hanaDbContext;
            _sqlConnectionString = configuration.GetConnectionString("SAPHANADB");
            _logger = logger;
        }

        /// <summary>
        /// SAP sorgusunu çalıştırır ve sonuçları SQL Server'a kaydeder
        /// </summary>
        public async Task<SyncResult> ExecuteQueryAndSaveToSqlAsync(string sapQuery, string sqlTableName, string keyColumn = null)
        {
            var result = new SyncResult
            {
                TableName = sqlTableName
            };

            if (string.IsNullOrWhiteSpace(sapQuery) || string.IsNullOrWhiteSpace(sqlTableName))
            {
                result.Success = false;
                result.Message = "SAP sorgusu ve tablo adı boş olamaz.";
                return result;
            }

            try
            {
                _logger.LogInformation($"SAP sorgusu çalıştırılıyor: {sapQuery}");

                // HanaDbContext ile SAP'dan veriyi çek
                var sapData = await _hanaDbContext.QueryAsDictionaryAsync(sapQuery);

                result.RecordsRetrieved = sapData.Count;

                if (sapData.Count == 0)
                {
                    result.Success = true; // Empty result is not an error
                    result.Message = "SAP sorgusundan veri dönmedi.";
                    return result;
                }

                // SQL Server'a kaydet
                await SaveToSqlServerAsync(sapData, sqlTableName, keyColumn);

                result.Success = true;
                result.Message = $"{sapData.Count} kayıt başarıyla SQL Server'a aktarıldı.";
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"SAP sorgusu çalıştırılırken hata oluştu: {ex.Message}");
                result.Success = false;
                result.Message = $"Hata: {ex.Message}";
                result.Exception = ex;
                return result;
            }
        }

        /// <summary>
        /// Dictionary listesi verisini SQL Server'a kaydeder
        /// </summary>
        private async Task SaveToSqlServerAsync(List<Dictionary<string, object>> data, string tableName, string keyColumn)
        {
            if (data.Count == 0) return;

            // Check for SQL injection
            if (!IsValidSqlIdentifier(tableName))
            {
                throw new ArgumentException("Invalid table name");
            }

            if (!string.IsNullOrEmpty(keyColumn) && !IsValidSqlIdentifier(keyColumn))
            {
                throw new ArgumentException("Invalid key column name");
            }

            using (var connection = new SqlConnection(_sqlConnectionString))
            {
                await connection.OpenAsync();

                // İlk kaydı kullanarak tablo yapısını analiz et
                var firstRecord = data[0];
                var columnDefinitions = new List<(string Name, Type Type, bool IsKey)>();

                foreach (var key in firstRecord.Keys)
                {
                    if (!IsValidSqlIdentifier(key))
                    {
                        _logger.LogWarning($"Skipping invalid column name: {key}");
                        continue;
                    }

                    var value = firstRecord[key];
                    var type = value?.GetType() ?? typeof(string);
                    var isKey = !string.IsNullOrEmpty(keyColumn) && key.Equals(keyColumn, StringComparison.OrdinalIgnoreCase);

                    columnDefinitions.Add((key, type, isKey));
                }

                // Tablo mevcut değilse oluştur
                if (!await TableExistsAsync(connection, tableName))
                {
                    await CreateTableAsync(connection, tableName, columnDefinitions);
                    _logger.LogInformation($"Tablo oluşturuldu: {tableName}");
                }
                else
                {
                    // Mevcut tablo yapısını kontrol et ve gerekirse güncelle
                    await UpdateTableColumnsIfNeededAsync(connection, tableName, columnDefinitions);
                }

                // Veriyi kaydet
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!string.IsNullOrEmpty(keyColumn))
                        {
                            // UPSERT: Her kaydı güncelle veya ekle
                            foreach (var record in data)
                            {
                                var validColumns = record.Keys.Where(k => IsValidSqlIdentifier(k)).ToList();
                                var columns = string.Join(", ", validColumns.Select(k => $"[{k}]"));
                                var parameters = string.Join(", ", validColumns.Select(k => $"@{k}"));
                                var updateSet = string.Join(", ", validColumns
                                    .Where(k => !k.Equals(keyColumn, StringComparison.OrdinalIgnoreCase))
                                    .Select(k => $"[{k}] = @{k}"));

                                if (string.IsNullOrEmpty(updateSet))
                                {
                                    // If there's only a key column and no other columns, skip this record
                                    continue;
                                }

                                var mergeSql = $@"
                                    MERGE INTO [{tableName}] AS target
                                    USING (SELECT @{keyColumn} AS [{keyColumn}]) AS source
                                    ON target.[{keyColumn}] = source.[{keyColumn}]
                                    WHEN MATCHED THEN
                                        UPDATE SET {updateSet}
                                    WHEN NOT MATCHED THEN
                                        INSERT ({columns})
                                        VALUES ({parameters});";

                                using (var command = new SqlCommand(mergeSql, connection, transaction))
                                {
                                    foreach (var key in validColumns)
                                    {
                                        command.Parameters.AddWithValue($"@{key}", record[key] ?? DBNull.Value);
                                    }

                                    await command.ExecuteNonQueryAsync();
                                }
                            }
                        }
                        else
                        {
                            // Anahtar kolon yoksa - önce tabloyu temizle, sonra tüm verileri ekle
                            using (var command = new SqlCommand($"DELETE FROM [{tableName}]", connection, transaction))
                            {
                                await command.ExecuteNonQueryAsync();
                            }

                            // SqlBulkCopy kullanarak toplu veri ekle
                            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                            {
                                bulkCopy.DestinationTableName = tableName;

                                // DataTable oluştur
                                var dataTable = new DataTable();
                                foreach (var column in columnDefinitions)
                                {
                                    dataTable.Columns.Add(column.Name, column.Type);
                                }

                                // DataTable'a verileri ekle
                                foreach (var record in data)
                                {
                                    var row = dataTable.NewRow();

                                    foreach (var column in columnDefinitions)
                                    {
                                        if (record.ContainsKey(column.Name))
                                        {
                                            row[column.Name] = record[column.Name] ?? DBNull.Value;
                                        }
                                    }

                                    dataTable.Rows.Add(row);
                                }

                                await bulkCopy.WriteToServerAsync(dataTable);
                            }
                        }

                        transaction.Commit();
                        _logger.LogInformation($"{data.Count} kayıt SQL Server'a başarıyla aktarıldı.");
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        _logger.LogError(ex, $"SQL Server'a veri aktarılırken hata oluştu: {ex.Message}");
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// SQL injection'a karşı korunmak için geçerli SQL tanımlayıcısı kontrolü
        /// </summary>
        private bool IsValidSqlIdentifier(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier))
                return false;

            // Basic check for SQL identifier validity
            return System.Text.RegularExpressions.Regex.IsMatch(identifier, @"^[a-zA-Z_][a-zA-Z0-9_]*$");
        }

        /// <summary>
        /// Tablo varsa true, yoksa false döner
        /// </summary>
        private async Task<bool> TableExistsAsync(SqlConnection connection, string tableName)
        {
            using (var command = new SqlCommand(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @TableName",
                connection))
            {
                command.Parameters.AddWithValue("@TableName", tableName);
                var result = await command.ExecuteScalarAsync();
                return Convert.ToInt32(result) > 0;
            }
        }

        /// <summary>
        /// SQL Server'da tablo oluşturur
        /// </summary>
        private async Task CreateTableAsync(SqlConnection connection, string tableName, List<(string Name, Type Type, bool IsKey)> columnDefinitions)
        {
            var createTableSql = new System.Text.StringBuilder();
            createTableSql.AppendLine($"CREATE TABLE [{tableName}] (");

            var columnDefinitionStrings = new List<string>();
            foreach (var column in columnDefinitions)
            {
                var sqlType = GetSqlTypeFromClrType(column.Type);
                var primaryKeyClause = column.IsKey ? "PRIMARY KEY" : "";
                columnDefinitionStrings.Add($"[{column.Name}] {sqlType} {primaryKeyClause}");
            }

            createTableSql.AppendLine(string.Join(",\n", columnDefinitionStrings));
            createTableSql.AppendLine(")");

            using (var command = new SqlCommand(createTableSql.ToString(), connection))
            {
                await command.ExecuteNonQueryAsync();
            }
        }

        /// <summary>
        /// Mevcut tablo kolonlarını kontrol eder ve gerekirse günceller
        /// </summary>
        private async Task UpdateTableColumnsIfNeededAsync(SqlConnection connection, string tableName, List<(string Name, Type Type, bool IsKey)> columnDefinitions)
        {
            // Mevcut kolonları al
            using (var command = new SqlCommand(
                $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName",
                connection))
            {
                command.Parameters.AddWithValue("@TableName", tableName);

                var existingColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        existingColumns.Add(reader.GetString(0));
                    }
                }

                // Eksik kolonları ekle
                foreach (var column in columnDefinitions)
                {
                    if (!existingColumns.Contains(column.Name))
                    {
                        var sqlType = GetSqlTypeFromClrType(column.Type);
                        var alterSql = $"ALTER TABLE [{tableName}] ADD [{column.Name}] {sqlType}";

                        using (var alterCommand = new SqlCommand(alterSql, connection))
                        {
                            await alterCommand.ExecuteNonQueryAsync();
                            _logger.LogInformation($"Kolon eklendi: {column.Name} ({sqlType})");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// .NET veri tipini SQL Server veri tipine dönüştürür
        /// </summary>
        private string GetSqlTypeFromClrType(Type type)
        {
            if (type == typeof(int) || type == typeof(Int32)) return "INT";
            if (type == typeof(long) || type == typeof(Int64)) return "BIGINT";
            if (type == typeof(short) || type == typeof(Int16)) return "SMALLINT";
            if (type == typeof(byte)) return "TINYINT";
            if (type == typeof(decimal)) return "DECIMAL(18, 6)";
            if (type == typeof(double)) return "FLOAT";
            if (type == typeof(float)) return "REAL";
            if (type == typeof(bool)) return "BIT";
            if (type == typeof(DateTime)) return "DATETIME";
            if (type == typeof(Guid)) return "UNIQUEIDENTIFIER";
            if (type == typeof(byte[])) return "VARBINARY(MAX)";

            return "NVARCHAR(255)";  // Varsayılan olarak string
        }

        /// <summary>
        /// SQL Server'dan veri sorgular
        /// </summary>
        public async Task<List<Dictionary<string, object>>> QuerySqlServerAsync(string query)
        {
            var result = new List<Dictionary<string, object>>();

            try
            {
                using (var connection = new SqlConnection(_sqlConnectionString))
                {
                    await connection.OpenAsync();

                    using (var command = new SqlCommand(query, connection))
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var row = new Dictionary<string, object>();

                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                var columnName = reader.GetName(i);
                                var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                                row[columnName] = value;
                            }

                            result.Add(row);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"SQL Server'dan veri sorgulanırken hata: {ex.Message}");
                throw;
            }

            return result;
        }

        /// <summary>
        /// SQL Server'daki tabloları listeler
        /// </summary>
        public async Task<List<string>> GetSqlTableNamesAsync()
        {
            var tables = new List<string>();

            try
            {
                using (var connection = new SqlConnection(_sqlConnectionString))
                {
                    await connection.OpenAsync();

                    using (var command = new SqlCommand(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
                        connection))
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            tables.Add(reader.GetString(0));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"SQL Server tabloları listelenirken hata: {ex.Message}");
                throw;
            }

            return tables;
        }

        public async Task<List<Dictionary<string, object>>> QuerySapAsync(string query)
        {
            return await _hanaDbContext.QueryAsDictionaryAsync(query);
        }
    }

    public class SyncResult
    {
        public bool Success { get; set; }
        public string TableName { get; set; }
        public string Message { get; set; }
        public int RecordsRetrieved { get; set; }
        public Exception Exception { get; set; }
    }
}