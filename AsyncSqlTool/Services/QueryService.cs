using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using AsyncSqlTool.Data;
using AsyncSqlTool.Models;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace AsyncSqlTool.Services
{
    public class QueryService
    {
        private readonly ApplicationDbContext _dbContext;
        private readonly IDatabaseConnectionFactory _connectionFactory;
        private readonly IConfiguration _configuration;
        private readonly ILogger<QueryService> _logger;
        private readonly string _targetConnectionString;

        // Çalışan sorguları izlemek için eş zamanlı koruma
        private static readonly HashSet<int> _runningQueries = new HashSet<int>();
        private static readonly object _lockObject = new object();

        public QueryService(
            ApplicationDbContext dbContext,
            IDatabaseConnectionFactory connectionFactory,
            IConfiguration configuration,
            ILogger<QueryService> logger)
        {
            _dbContext = dbContext;
            _connectionFactory = connectionFactory;
            _configuration = configuration;
            _logger = logger;
            _targetConnectionString = configuration.GetConnectionString("DefaultConnection");
        }

        #region Veritabanı Bağlantı Yönetimi

        /// <summary>
        /// Tüm veritabanı bağlantılarını getirir
        /// </summary>
        public async Task<List<DatabaseConnection>> GetAllDatabaseConnectionsAsync()
        {
            return await _dbContext.DatabaseConnections
                .Where(c => c.IsActive)
                .OrderBy(c => c.Name)
                .ToListAsync();
        }

        /// <summary>
        /// ID'ye göre veritabanı bağlantısını getirir
        /// </summary>
        public async Task<DatabaseConnection> GetDatabaseConnectionByIdAsync(int id)
        {
            return await _dbContext.DatabaseConnections.FindAsync(id);
        }

        /// <summary>
        /// Yeni veritabanı bağlantısı ekler
        /// </summary>
        public async Task<DatabaseConnection> AddDatabaseConnectionAsync(DatabaseConnection connection)
        {
            connection.CreatedAt = DateTime.Now;
            _dbContext.DatabaseConnections.Add(connection);
            await _dbContext.SaveChangesAsync();
            return connection;
        }

        /// <summary>
        /// Veritabanı bağlantısını günceller
        /// </summary>
        public async Task<bool> UpdateDatabaseConnectionAsync(DatabaseConnection connection)
        {
            var existingConnection = await _dbContext.DatabaseConnections.FindAsync(connection.Id);
            if (existingConnection == null)
                return false;

            existingConnection.Name = connection.Name;
            existingConnection.DatabaseType = connection.DatabaseType;
            existingConnection.ConnectionString = connection.ConnectionString;
            existingConnection.Description = connection.Description;
            existingConnection.IsActive = connection.IsActive;
            existingConnection.UpdatedAt = DateTime.Now;

            await _dbContext.SaveChangesAsync();
            return true;
        }

        /// <summary>
        /// Veritabanı bağlantısını siler
        /// </summary>
        public async Task<bool> DeleteDatabaseConnectionAsync(int id)
        {
            var connection = await _dbContext.DatabaseConnections.FindAsync(id);
            if (connection == null)
                return false;

            // İlişkili sorguları kontrol et
            var relatedQueries = await _dbContext.SavedQueries
                .Where(q => q.DatabaseConnectionId == id)
                .ToListAsync();

            if (relatedQueries.Any())
            {
                throw new InvalidOperationException("Bu bağlantıyı kullanan kaydedilmiş sorgular var. Önce bu sorguları silmelisiniz.");
            }

            _dbContext.DatabaseConnections.Remove(connection);
            await _dbContext.SaveChangesAsync();
            return true;
        }

        /// <summary>
        /// Veritabanı bağlantısını test eder
        /// </summary>
        public async Task<(bool success, string message)> TestDatabaseConnectionAsync(DatabaseConnection connection)
        {
            try
            {
                using var dbConnection = await _connectionFactory.CreateConnectionAsync(connection);

                // Bağlantının açık olduğundan emin ol
                if (dbConnection.State != System.Data.ConnectionState.Open)
                     dbConnection.Open();

                return (true, "Bağlantı başarılı!");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Veritabanı bağlantı testi başarısız: {ex.Message}");
                return (false, $"Bağlantı hatası: {ex.Message}");
            }
        }

        #endregion

        #region Sorgu Yönetimi

        /// <summary>
        /// Tüm kaydedilmiş sorguları getirir
        /// </summary>
        public async Task<List<SavedQuery>> GetAllSavedQueriesAsync()
        {
            return await _dbContext.SavedQueries
                .Include(q => q.DatabaseConnection)
                .OrderBy(q => q.Name)
                .ToListAsync();
        }

        /// <summary>
        /// ID'ye göre kaydedilmiş sorguyu getirir
        /// </summary>
        public async Task<SavedQuery> GetSavedQueryByIdAsync(int id)
        {
            return await _dbContext.SavedQueries
                .Include(q => q.DatabaseConnection)
                .Include(q => q.ColumnMappings)
                .FirstOrDefaultAsync(q => q.Id == id);
        }

        /// <summary>
        /// Yeni sorgu kaydeder
        /// </summary>
        public async Task<SavedQuery> AddSavedQueryAsync(SavedQuery query)
        {
            query.CreatedAt = DateTime.Now;
            _dbContext.SavedQueries.Add(query);
            await _dbContext.SaveChangesAsync();
            return query;
        }

        /// <summary>
        /// Kaydedilmiş sorguyu günceller
        /// </summary>
        public async Task<bool> UpdateSavedQueryAsync(SavedQuery query)
        {
            var existingQuery = await _dbContext.SavedQueries
                .Include(q => q.ColumnMappings)
                .FirstOrDefaultAsync(q => q.Id == query.Id);

            if (existingQuery == null)
                return false;

            existingQuery.Name = query.Name;
            existingQuery.QueryText = query.QueryText;
            existingQuery.TargetTableName = query.TargetTableName;
            existingQuery.KeyColumn = query.KeyColumn;
            existingQuery.Description = query.Description;
            existingQuery.IsScheduled = query.IsScheduled;
            existingQuery.ScheduleExpression = query.ScheduleExpression;
            existingQuery.DatabaseConnectionId = query.DatabaseConnectionId;

            // Kolon eşleştirmelerini güncelle
            if (query.ColumnMappings != null)
            {
                // Mevcut eşleştirmeleri kaldır
                _dbContext.RemoveRange(existingQuery.ColumnMappings);

                // Yeni eşleştirmeleri ekle
                existingQuery.ColumnMappings = query.ColumnMappings;
            }

            await _dbContext.SaveChangesAsync();
            return true;
        }

        /// <summary>
        /// Kaydedilmiş sorguyu siler
        /// </summary>
        public async Task<bool> DeleteSavedQueryAsync(int id)
        {
            var query = await _dbContext.SavedQueries
                .Include(q => q.ColumnMappings)
                .FirstOrDefaultAsync(q => q.Id == id);

            if (query == null)
                return false;

            // İlişkili kolon eşleştirmelerini sil
            if (query.ColumnMappings != null && query.ColumnMappings.Any())
            {
                _dbContext.RemoveRange(query.ColumnMappings);
            }

            // İlişkili çalıştırma loglarını sil
            var executionLogs = await _dbContext.QueryExecutionLogs
                .Where(l => l.SavedQueryId == id)
                .ToListAsync();

            if (executionLogs.Any())
            {
                _dbContext.RemoveRange(executionLogs);
            }

            _dbContext.SavedQueries.Remove(query);
            await _dbContext.SaveChangesAsync();
            return true;
        }

        #endregion

        #region Sorgu Çalıştırma ve Veri Aktarımı

        /// <summary>
        /// Sorguyu önizleme için çalıştırır
        /// </summary>
        public async Task<(List<Dictionary<string, object>> data, string message, bool success)> PreviewQueryAsync(int databaseConnectionId, string query)
        {
            try
            {
                var dbConnection = await GetDatabaseConnectionByIdAsync(databaseConnectionId);
                if (dbConnection == null)
                {
                    return (null, "Veritabanı bağlantısı bulunamadı.", false);
                }

                var data = await _connectionFactory.ExecuteQueryAsync(dbConnection, query);
                return (data, $"{data.Count} kayıt bulundu.", true);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Sorgu önizleme hatası: {ex.Message}");
                return (null, $"Hata: {ex.Message}", false);
            }
        }

        /// <summary>
        /// Kaydedilmiş sorguyu çalıştırır ve hedef tabloya aktarır
        /// </summary>
        public async Task<(bool success, string message, int recordCount)> ExecuteSavedQueryAsync(int queryId)
        {
            bool alreadyRunning = false;

            lock (_lockObject)
            {
                alreadyRunning = !_runningQueries.Add(queryId);
            }

            if (alreadyRunning)
            {
                return (false, "Bu sorgu zaten çalışıyor. Lütfen tamamlanmasını bekleyin.", 0);
            }

            try
            {
                var savedQuery = await GetSavedQueryByIdAsync(queryId);
                if (savedQuery == null)
                {
                    return (false, "Sorgu bulunamadı.", 0);
                }

                return await ExecuteQueryAndSaveToSqlAsync(
                    savedQuery.DatabaseConnectionId,
                    savedQuery.QueryText,
                    savedQuery.TargetTableName,
                    savedQuery.KeyColumn,
                    queryId);
            }
            finally
            {
                lock (_lockObject)
                {
                    _runningQueries.Remove(queryId);
                }
            }
        }

        /// <summary>
        /// Sorguyu çalıştırır ve sonuçları SQL Server'a kaydeder
        /// </summary>
        public async Task<(bool success, string message, int recordCount)> ExecuteQueryAndSaveToSqlAsync(
            int databaseConnectionId,
            string queryText,
            string targetTableName,
            string keyColumn = null,
            int? savedQueryId = null)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            var log = new QueryExecutionLog
            {
                SavedQueryId = savedQueryId ?? 0,
                ExecutionTime = DateTime.Now,
                IsSuccess = false,
                RecordsAffected = 0
            };

            try
            {
                var dbConnection = await GetDatabaseConnectionByIdAsync(databaseConnectionId);
                if (dbConnection == null)
                {
                    log.Message = "Veritabanı bağlantısı bulunamadı.";
                    await SaveExecutionLogAsync(log);
                    return (false, log.Message, 0);
                }

                _logger.LogInformation($"Sorgu çalıştırılıyor: {queryText}");

                // Sorguyu çalıştır ve verileri al
                var sourceData = await _connectionFactory.ExecuteQueryAsync(dbConnection, queryText);
                log.RecordsAffected = sourceData.Count;

                if (sourceData.Count == 0)
                {
                    log.Message = "Sorgudan veri dönmedi.";
                    log.IsSuccess = true;
                    await SaveExecutionLogAsync(log);
                    return (true, log.Message, 0);
                }

                // SQL Server'a kaydet
                await SaveToSqlServerAsync(sourceData, targetTableName, keyColumn, savedQueryId);

                log.Message = $"{sourceData.Count} kayıt başarıyla SQL Server'a aktarıldı.";
                log.IsSuccess = true;

                stopwatch.Stop();
                log.ExecutionDuration = stopwatch.Elapsed;

                await SaveExecutionLogAsync(log);

                // Sorgu çalıştırıldı bilgisini güncelle
                if (savedQueryId.HasValue)
                {
                    var savedQuery = await _dbContext.SavedQueries.FindAsync(savedQueryId.Value);
                    if (savedQuery != null)
                    {
                        savedQuery.LastExecuted = DateTime.Now;
                        await _dbContext.SaveChangesAsync();
                    }
                }

                return (true, log.Message, sourceData.Count);
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                log.ExecutionDuration = stopwatch.Elapsed;

                _logger.LogError(ex, $"Sorgu çalıştırılırken hata oluştu: {ex.Message}");
                log.Message = $"Hata: {ex.Message}";
                log.ErrorDetails = ex.ToString();

                await SaveExecutionLogAsync(log);

                return (false, log.Message, 0);
            }
        }

        /// <summary>
        /// Çalıştırma logunu kaydeder
        /// </summary>
        private async Task SaveExecutionLogAsync(QueryExecutionLog log)
        {
            if (log.SavedQueryId > 0)
            {
                _dbContext.QueryExecutionLogs.Add(log);
                await _dbContext.SaveChangesAsync();
            }
        }

        /// <summary>
        /// Dictionary listesi verisini SQL Server'a kaydeder
        /// </summary>
        private async Task SaveToSqlServerAsync(List<Dictionary<string, object>> data, string tableName, string keyColumn, int? savedQueryId = null)
        {
            if (data.Count == 0) return;

            // SQL injection kontrolü
            if (!IsValidSqlIdentifier(tableName))
            {
                throw new ArgumentException("Geçersiz tablo adı");
            }

            if (!string.IsNullOrEmpty(keyColumn) && !IsValidSqlIdentifier(keyColumn))
            {
                throw new ArgumentException("Geçersiz anahtar kolon adı");
            }

            using (var connection = new SqlConnection(_targetConnectionString))
            {
                await connection.OpenAsync();

                // İlk kaydı kullanarak tablo yapısını analiz et
                var firstRecord = data[0];
                var columnDefinitions = new List<(string Name, Type Type, bool IsKey)>();

                foreach (var key in firstRecord.Keys)
                {
                    if (!IsValidSqlIdentifier(key))
                    {
                        _logger.LogWarning($"Geçersiz kolon adı atlanıyor: {key}");
                        continue;
                    }

                    var value = firstRecord[key];
                    var isKey = !string.IsNullOrEmpty(keyColumn) && key.Equals(keyColumn, StringComparison.OrdinalIgnoreCase);

                    // SAP HANA türleri için özel işlem
                    Type type = typeof(string); // Varsayılan tür

                    if (value != null)
                    {
                        var typeName = value.GetType().FullName;

                        // HanaDecimal gibi özel SAP HANA veri türlerini dönüştür
                        if (typeName == "Sap.Data.Hana.HanaDecimal")
                        {
                            type = typeof(decimal);
                        }
                        else
                        {
                            type = value.GetType();
                        }
                    }

                    columnDefinitions.Add((key, type, isKey));
                }

                // Tablo mevcut değilse oluştur
                if (!await TableExistsAsync(connection, tableName))
                {
                    await CreateTableAsync(connection, tableName, columnDefinitions, savedQueryId);
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
                                    // Sadece anahtar kolon varsa ve başka kolon yoksa, bu kaydı atla
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
                                        var paramValue = record[key];

                                        // HanaDecimal ve diğer özel türleri dönüştür
                                        if (paramValue != null)
                                        {
                                            var paramTypeName = paramValue.GetType().FullName;

                                            if (paramTypeName == "Sap.Data.Hana.HanaDecimal")
                                            {
                                                // HanaDecimal'i decimal'e dönüştür
                                                // Eğer doğrudan cast yapılamıyorsa ToString() kullanıp parse etmek gerekebilir
                                                try
                                                {
                                                    decimal decimalValue = Convert.ToDecimal(paramValue.ToString());
                                                    command.Parameters.AddWithValue($"@{key}", decimalValue);
                                                }
                                                catch
                                                {
                                                    // Dönüştürme başarısız olursa null kullan
                                                    command.Parameters.AddWithValue($"@{key}", DBNull.Value);
                                                }
                                            }
                                            else
                                            {
                                                command.Parameters.AddWithValue($"@{key}", paramValue ?? DBNull.Value);
                                            }
                                        }
                                        else
                                        {
                                            command.Parameters.AddWithValue($"@{key}", DBNull.Value);
                                        }
                                    }

                                    await command.ExecuteNonQueryAsync();
                                }
                            }
                        }
                        else
                        {
                            // Anahtar kolon yoksa - önce tabloyu temizle, sonra tüm verileri ekle
                            using (var command = new SqlCommand($"TRUNCATE TABLE [{tableName}]", connection, transaction))
                            {
                                await command.ExecuteNonQueryAsync();
                            }

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
                                        var cellValue = record[column.Name];

                                        // HanaDecimal ve diğer özel veri tipleri için dönüşüm yap
                                        if (cellValue != null)
                                        {
                                            var valueTypeName = cellValue.GetType().FullName;

                                            if (valueTypeName == "Sap.Data.Hana.HanaDecimal")
                                            {
                                                try
                                                {
                                                    row[column.Name] = Convert.ToDecimal(cellValue.ToString());
                                                }
                                                catch
                                                {
                                                    row[column.Name] = DBNull.Value;
                                                }
                                            }
                                            else
                                            {
                                                row[column.Name] = cellValue ?? DBNull.Value;
                                            }
                                        }
                                        else
                                        {
                                            row[column.Name] = DBNull.Value;
                                        }
                                    }
                                }

                                dataTable.Rows.Add(row);
                            }

                            // SqlBulkCopy kullanarak toplu veri ekle
                            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                            {
                                bulkCopy.DestinationTableName = tableName;
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

            // SQL anahtar kelimelerini engelle
            string[] sqlKeywords = new[] { "SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TABLE", "WHERE", "FROM", "JOIN" };
            if (sqlKeywords.Any(k => identifier.ToUpper() == k))
                return false;

            // SQL tanımlayıcı geçerlilik kontrolü - yalnızca harf, rakam ve alt çizgi
            return Regex.IsMatch(identifier, @"^[a-zA-Z_][a-zA-Z0-9_]*$");
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
        private async Task CreateTableAsync(SqlConnection connection, string tableName, List<(string Name, Type Type, bool IsKey)> columnDefinitions, int? savedQueryId = null)
        {
            var createTableSql = new StringBuilder();
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

            // Kolon bilgilerini kaydet
            if (tableName.StartsWith("tmp_") || tableName.StartsWith("temp_") || !savedQueryId.HasValue)
                return; // Geçici tablolar veya savedQueryId yoksa kolon bilgilerini kaydetme

            // Kolon eşleştirme kayıtlarını oluştur
            var columnMappings = columnDefinitions.Select(col => new QueryColumnMapping
            {
                SavedQueryId = savedQueryId.Value,
                SourceColumnName = col.Name,
                TargetColumnName = col.Name,
                DataType = col.Type.Name,
            }).ToList();

            _dbContext.QueryColumnMappings.AddRange(columnMappings);
            await _dbContext.SaveChangesAsync();
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
            if (type == typeof(DateTimeOffset)) return "DATETIMEOFFSET";
            if (type == typeof(TimeSpan)) return "TIME";
            if (type == typeof(Guid)) return "UNIQUEIDENTIFIER";
            if (type == typeof(byte[])) return "VARBINARY(MAX)";

            // String değerler için daha akıllı tip belirleme
            if (type == typeof(string))
            {
                return "NVARCHAR(255)";
            }

            return "NVARCHAR(255)";  // Varsayılan olarak string
        }

        #region SQL Tablosu Yönetimi

        /// <summary>
        /// SQL Server'daki bir tabloyu temizler (TRUNCATE)
        /// </summary>
        public async Task<bool> TruncateTableAsync(string tableName)
        {
            if (!IsValidSqlIdentifier(tableName))
            {
                throw new ArgumentException("Geçersiz tablo adı");
            }

            try
            {
                using (var connection = new SqlConnection(_targetConnectionString))
                {
                    await connection.OpenAsync();

                    // Önce tablonun varlığını kontrol et
                    if (!await TableExistsAsync(connection, tableName))
                    {
                        return false;
                    }

                    using (var command = new SqlCommand($"TRUNCATE TABLE [{tableName}]", connection))
                    {
                        await command.ExecuteNonQueryAsync();
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Tablo temizleme hatası: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// SQL Server'daki bir tabloyu siler (DROP)
        /// </summary>
        public async Task<bool> DropTableAsync(string tableName)
        {
            if (!IsValidSqlIdentifier(tableName))
            {
                throw new ArgumentException("Geçersiz tablo adı");
            }

            try
            {
                using (var connection = new SqlConnection(_targetConnectionString))
                {
                    await connection.OpenAsync();

                    // Önce tablonun varlığını kontrol et
                    if (!await TableExistsAsync(connection, tableName))
                    {
                        return false;
                    }

                    using (var command = new SqlCommand($"DROP TABLE [{tableName}]", connection))
                    {
                        await command.ExecuteNonQueryAsync();
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Tablo silme hatası: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// SQL Server'daki tabloları listeler
        /// </summary>
        public async Task<List<string>> GetSqlTableNamesAsync()
        {
            var tables = new List<string>();

            try
            {
                using (var connection = new SqlConnection(_targetConnectionString))
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

        /// <summary>
        /// SQL Server'da tablo kolonlarını listeler
        /// </summary>
        public async Task<List<Dictionary<string, object>>> GetTableColumnsAsync(string tableName)
        {
            if (!IsValidSqlIdentifier(tableName))
            {
                throw new ArgumentException("Geçersiz tablo adı");
            }

            var columns = new List<Dictionary<string, object>>();

            try
            {
                using (var connection = new SqlConnection(_targetConnectionString))
                {
                    await connection.OpenAsync();

                    // Kolonları getir
                    using (var command = new SqlCommand(
                        @"SELECT 
                            c.COLUMN_NAME,
                            c.DATA_TYPE,
                            c.CHARACTER_MAXIMUM_LENGTH,
                            c.NUMERIC_PRECISION,
                            c.NUMERIC_SCALE,
                            c.IS_NULLABLE,
                            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY,
                            c.ORDINAL_POSITION
                        FROM 
                            INFORMATION_SCHEMA.COLUMNS c
                        LEFT JOIN (
                            SELECT ku.TABLE_CATALOG, ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                                ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                                AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                        ) pk 
                            ON c.TABLE_CATALOG = pk.TABLE_CATALOG 
                            AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA 
                            AND c.TABLE_NAME = pk.TABLE_NAME 
                            AND c.COLUMN_NAME = pk.COLUMN_NAME
                        WHERE c.TABLE_NAME = @TableName
                        ORDER BY c.ORDINAL_POSITION",
                        connection))
                    {
                        command.Parameters.AddWithValue("@TableName", tableName);
                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var column = new Dictionary<string, object>
                                {
                                    { "Name", reader["COLUMN_NAME"] },
                                    { "DataType", reader["DATA_TYPE"] },
                                    { "MaxLength", reader.IsDBNull(reader.GetOrdinal("CHARACTER_MAXIMUM_LENGTH")) ? null : reader["CHARACTER_MAXIMUM_LENGTH"] },
                                    { "Precision", reader.IsDBNull(reader.GetOrdinal("NUMERIC_PRECISION")) ? null : reader["NUMERIC_PRECISION"] },
                                    { "Scale", reader.IsDBNull(reader.GetOrdinal("NUMERIC_SCALE")) ? null : reader["NUMERIC_SCALE"] },
                                    { "IsNullable", reader["IS_NULLABLE"].ToString().Equals("YES", StringComparison.OrdinalIgnoreCase) },
                                    { "IsPrimaryKey", Convert.ToBoolean(reader["IS_PRIMARY_KEY"]) },
                                    { "OrdinalPosition", reader["ORDINAL_POSITION"] }
                                };
                                columns.Add(column);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Tablo kolonları listelenirken hata: {ex.Message}");
                throw;
            }

            return columns;
        }

        /// <summary>
        /// SQL Server'daki tablodan veri sorgular
        /// </summary>
        public async Task<List<Dictionary<string, object>>> QuerySqlTableAsync(
     string tableName,
     int? take = null,
     string columns = "*",
     string whereClause = null)
        {
            if (!IsValidSqlIdentifier(tableName))
            {
                throw new ArgumentException("Geçersiz tablo adı");
            }

            // Kolonları güvenli hale getir
            string safeColumns = columns;
            if (string.IsNullOrWhiteSpace(safeColumns) || safeColumns.Trim() == "")
            {
                safeColumns = "*";
            }

            // TOP ifadesini sorguda doğru şekilde kullan
            string topClause = take.HasValue ? $"TOP {take.Value}" : "";

            var query = $"SELECT {topClause} {safeColumns} FROM [{tableName}]";

            if (!string.IsNullOrWhiteSpace(whereClause))
            {
                query += $" WHERE {whereClause}";
            }

            try
            {
                using (var connection = new SqlConnection(_targetConnectionString))
                {
                    await connection.OpenAsync();
                    using (var command = new SqlCommand(query, connection))
                    {
                        var result = new List<Dictionary<string, object>>();
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
                        return result;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"SQL tablo sorgu hatası: {ex.Message}");
                throw;
            }
        }

        #endregion
        #endregion
    }
}