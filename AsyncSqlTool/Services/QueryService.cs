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
        // QueryService.cs içindeki ExecuteQueryAndSaveToSqlAsync metodunun imzasını genişletmemiz gerekiyor:

        public async Task<(bool success, string message, int recordCount)> ExecuteQueryAndSaveToSqlAsync(
            int databaseConnectionId,
            string queryText,
            string targetTableName,
            string keyColumn = null,
            int? savedQueryId = null,
            List<QueryColumnMapping> columnMappings = null)  
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

                // Parametre olarak gelen kolon eşleştirmelerini kullan
                List<QueryColumnMapping> mappingsToUse = columnMappings ?? new List<QueryColumnMapping>();

                if (mappingsToUse.Count == 0 && savedQueryId.HasValue)
                {
                    mappingsToUse = await _dbContext.QueryColumnMappings
                        .Where(m => m.SavedQueryId == savedQueryId.Value)
                        .ToListAsync();
                }

                // Hala kolon eşleştirmeleri bulunamadıysa, aynı sorgu ve tablo adına sahip
                // kaydedilmiş sorgulardan eşleştirmeleri getirmeyi dene
                if (mappingsToUse.Count == 0)
                {
                    var savedQuery = await _dbContext.SavedQueries
                        .FirstOrDefaultAsync(q => q.TargetTableName == targetTableName &&
                                            q.KeyColumn == keyColumn &&
                                            q.QueryText == queryText);

                    if (savedQuery != null)
                    {
                        mappingsToUse = await _dbContext.QueryColumnMappings
                            .Where(m => m.SavedQueryId == savedQuery.Id)
                            .ToListAsync();
                    }
                }

                // SQL Server'a kaydet (kolon eşleştirmelerini de parametre olarak geçir)
                await SaveToSqlServerAsync(sourceData, targetTableName, keyColumn, savedQueryId, mappingsToUse);

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
        private async Task SaveToSqlServerAsync(List<Dictionary<string, object>> data, string tableName, string keyColumn, int? savedQueryId = null, List<QueryColumnMapping> columnMappings = null)
        {
            if (data.Count == 0) return;

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

                var firstRecord = data[0];

                // Bu yeni yapı SQL tipini de saklayacak
                var columnDefinitions = new List<(string Name, Type Type, bool IsKey, string SqlType)>();

                foreach (var key in firstRecord.Keys)
                {
                    var value = firstRecord[key];
                    var isKey = !string.IsNullOrEmpty(keyColumn) && key.Equals(keyColumn, StringComparison.OrdinalIgnoreCase);

                    var columnMapping = columnMappings?.FirstOrDefault(m => m.SourceColumnName.Equals(key, StringComparison.OrdinalIgnoreCase));

                    if (columnMapping != null)
                    {
                        string sqlType = GetSqlTypeFromMapping(columnMapping);
                        Type clrType = GetClrTypeFromSqlType(columnMapping.DataType, columnMapping.Length, columnMapping.Precision);

                        columnDefinitions.Add((key, clrType, columnMapping.IsPrimaryKey, sqlType));
                    }
                    else
                    {
                        Type clrType = typeof(string);

                        if (value != null)
                        {
                            var typeName = value.GetType().FullName;
                            if (typeName == "Sap.Data.Hana.HanaDecimal")
                            {
                                clrType = typeof(decimal);
                            }
                            else
                            {
                                clrType = value.GetType();
                            }
                        }

                        string sqlType = GetSqlTypeFromClrType(clrType);

                        columnDefinitions.Add((key, clrType, isKey, sqlType));
                    }
                }

                if (!await TableExistsAsync(connection, tableName))
                {
                    await CreateTableAsync(connection, tableName, columnDefinitions, savedQueryId);
                    _logger.LogInformation($"Tablo oluşturuldu: {tableName}");
                }
                else
                {
                    await UpdateTableColumnsIfNeededAsync(connection, tableName, columnDefinitions, columnMappings);
                }

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!string.IsNullOrEmpty(keyColumn))
                        {
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

                                        if (paramValue != null)
                                        {
                                            var paramTypeName = paramValue.GetType().FullName;

                                            if (paramTypeName == "Sap.Data.Hana.HanaDecimal")
                                            {
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
                            using (var command = new SqlCommand($"TRUNCATE TABLE [{tableName}]", connection, transaction))
                            {
                                await command.ExecuteNonQueryAsync();
                            }

                            var dataTable = new DataTable();
                            foreach (var column in columnDefinitions)
                            {
                                dataTable.Columns.Add(column.Name, column.Type);
                            }

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
        private async Task CreateTableAsync(SqlConnection connection, string tableName,
     List<(string Name, Type Type, bool IsKey, string SqlType)> columnDefinitions, int? savedQueryId = null)
        {
            var createTableSql = new StringBuilder();
            createTableSql.AppendLine($"CREATE TABLE [{tableName}] (");

            var columnDefinitionStrings = new List<string>();
            foreach (var column in columnDefinitions)
            {
                // SQL tipini doğrudan kullan
                var primaryKeyClause = column.IsKey ? "PRIMARY KEY" : "";
                columnDefinitionStrings.Add($"[{column.Name}] {column.SqlType} {primaryKeyClause}");
            }

            createTableSql.AppendLine(string.Join(",\n", columnDefinitionStrings));
            createTableSql.AppendLine(")");

            using (var command = new SqlCommand(createTableSql.ToString(), connection))
            {
                await command.ExecuteNonQueryAsync();
            }


            if (tableName.StartsWith("tmp_") || tableName.StartsWith("temp_") || !savedQueryId.HasValue)
                return;


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
        private async Task UpdateTableColumnsIfNeededAsync(SqlConnection connection, string tableName,
           List<(string Name, Type Type, bool IsKey, string SqlType)> columnDefinitions,
           List<QueryColumnMapping> columnMappings = null)
        {
            // Mevcut kolonları al (bu kısım değişmedi)
            var existingColumns = new Dictionary<string, (string DataType, int? Length, int? Precision, bool IsPrimaryKey)>();

            using (var command = new SqlCommand(
                @"SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
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
        WHERE c.TABLE_NAME = @TableName",
                connection))
            {
                command.Parameters.AddWithValue("@TableName", tableName);
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var columnName = reader.GetString(0);
                        var dataType = reader.GetString(1);

                        int? length = null;
                        if (!reader.IsDBNull(2))
                            length = reader.GetInt32(2);

                        int? precision = null;
                        if (!reader.IsDBNull(3))
                            precision = reader.GetInt32(3);

                        bool isPrimaryKey = Convert.ToBoolean(reader.GetValue(5));

                        existingColumns.Add(columnName, (dataType, length, precision, isPrimaryKey));
                    }
                }
            }

            foreach (var column in columnDefinitions)
            {
                // Kolon tipini doğrudan kullan
                string sqlType = column.SqlType;

                // Kolon yoksa ekle
                if (!existingColumns.ContainsKey(column.Name))
                {
                    var alterSql = $"ALTER TABLE [{tableName}] ADD [{column.Name}] {sqlType}";

                    using (var alterCommand = new SqlCommand(alterSql, connection))
                    {
                        await alterCommand.ExecuteNonQueryAsync();
                        _logger.LogInformation($"Kolon eklendi: {column.Name} ({sqlType})");
                    }
                }
                else
                {
                    // Kolon tipini güncelle (gerekiyorsa)
                    var existingColumn = existingColumns[column.Name];
                    string existingSqlType = $"{existingColumn.DataType}";

                    if (existingColumn.Length.HasValue)
                        existingSqlType += $"({existingColumn.Length.Value})";
                    else if (existingColumn.Precision.HasValue)
                        existingSqlType += $"({existingColumn.Precision.Value})";

                    // SQL tipi farklıysa güncelle
                    if (!IsSameColumnType(existingSqlType, sqlType))
                    {
                        try
                        {
                            // Veri tipi değiştirilirken verileri kaybetmemek için ALTER COLUMN kullan
                            var alterSql = $"ALTER TABLE [{tableName}] ALTER COLUMN [{column.Name}] {sqlType}";

                            using (var alterCommand = new SqlCommand(alterSql, connection))
                            {
                                await alterCommand.ExecuteNonQueryAsync();
                                _logger.LogInformation($"Kolon tipi güncellendi: {column.Name} ({existingSqlType} -> {sqlType})");
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning($"Kolon tipi güncellenirken hata: {ex.Message}. Mevcut tip korunuyor: {column.Name}");
                        }
                    }

                    // Eksik PK ekle veya kaldır
                    bool primaryKeyNeedsUpdate = existingColumn.IsPrimaryKey != column.IsKey;
                    if (primaryKeyNeedsUpdate)
                    {
                        await UpdatePrimaryKeyConstraintAsync(connection, tableName, column.Name, column.IsKey);
                    }
                }
            }
        }

        /// <summary>
        /// Kolon eşleştirmesine göre SQL veri tipini belirler - Tüm veri tipleri için kullanıcı ayarlarını dikkate alır
        /// </summary>
        private string GetSqlTypeFromMapping(QueryColumnMapping mapping)
        {
            if (mapping == null || string.IsNullOrEmpty(mapping.DataType))
                return "NVARCHAR(255)"; // Varsayılan tip

            string sqlType = mapping.DataType.ToUpper();

            // Tırnak içindeki parantezleri temizle (örn: "VARCHAR(100)" -> "VARCHAR")
            string baseType = sqlType;
            if (baseType.Contains("("))
                baseType = baseType.Substring(0, baseType.IndexOf("("));

            // Veri tipine göre özel işlem
            switch (baseType)
            {
                // Uzunluk gerektiren tipler
                case "CHAR":
                case "NCHAR":
                case "VARCHAR":
                case "NVARCHAR":
                    int length = mapping.Length > 0 ? mapping.Length : 255;
                    if (length > 8000 || length == -1) // -1 MAX demek
                        return $"{baseType}(MAX)";
                    else
                        return $"{baseType}({length})";

                // Precision ve Scale gerektiren tipler
                case "DECIMAL":
                case "NUMERIC":
                    int precision = mapping.Precision > 0 ? mapping.Precision : 18;
                    int scale = mapping.Scale > 0 ? mapping.Scale : 2;
                    return $"{baseType}({precision},{scale})";

                // Precision gerektiren tipler
                case "FLOAT":
                    int floatPrecision = mapping.Precision > 0 ? mapping.Precision : 53;
                    return $"{baseType}({floatPrecision})";

                // Uzunluk gerektiren binary tipler
                case "BINARY":
                case "VARBINARY":
                    int binLength = mapping.Length > 0 ? mapping.Length : 50;
                    if (binLength > 8000 || binLength == -1)
                        return $"{baseType}(MAX)";
                    else
                        return $"{baseType}({binLength})";

                // Datetime precision gerektiren tipler
                case "DATETIME2":
                case "DATETIMEOFFSET":
                case "TIME":
                    int dtPrecision = mapping.Precision >= 0 && mapping.Precision <= 7 ? mapping.Precision : 7;
                    return $"{baseType}({dtPrecision})";

                // Sabit uzunluk tipler - parametre gerektirmez
                case "BIT":
                case "TINYINT":
                case "SMALLINT":
                case "INT":
                case "BIGINT":
                case "SMALLMONEY":
                case "MONEY":
                case "REAL":
                case "DATE":
                case "DATETIME":
                case "SMALLDATETIME":
                case "TIMESTAMP":
                case "UNIQUEIDENTIFIER":
                case "IMAGE":
                case "TEXT":
                case "NTEXT":
                case "XML":
                default:
                    return baseType;
            }
        }

        /// <summary>
        /// İki SQL veri tipinin aynı olup olmadığını kontrol eder 
        /// </summary>
        private bool IsSameColumnType(string type1, string type2)
        {
            // Basit karşılaştırma için boşlukları temizle ve büyük harfe çevir
            string normalizedType1 = type1.Replace(" ", "").ToUpper();
            string normalizedType2 = type2.Replace(" ", "").ToUpper();

            // Temel tipleri karşılaştır (parantez içindeki uzunluk/hassasiyet değerlerini yok say)
            string baseType1 = normalizedType1;
            string baseType2 = normalizedType2;

            if (normalizedType1.Contains("("))
                baseType1 = normalizedType1.Substring(0, normalizedType1.IndexOf("("));

            if (normalizedType2.Contains("("))
                baseType2 = normalizedType2.Substring(0, normalizedType2.IndexOf("("));

            // Temelde aynı tip değilse false döndür
            if (baseType1 != baseType2)
                return false;

            // VARCHAR, NVARCHAR gibi string tipler için uzunluğu kontrol et
            if (baseType1.Contains("VARCHAR") || baseType1.Contains("CHAR"))
            {
                // MAX değeri ile sayısal değer arasında fark varsa false döndür
                bool type1HasMax = normalizedType1.Contains("(MAX)");
                bool type2HasMax = normalizedType2.Contains("(MAX)");

                if (type1HasMax != type2HasMax)
                    return false;

                // Eğer MAX değil ise ve uzunlukları farklı ise false döndür
                if (!type1HasMax && !type2HasMax)
                {
                    try
                    {
                        int length1 = ExtractNumberFromType(normalizedType1);
                        int length2 = ExtractNumberFromType(normalizedType2);

                        // Eğer uzunluk düşürülüyorsa false döndür (veri kaybı olabilir)
                        if (length1 > length2)
                            return false;
                    }
                    catch
                    {
                        // Uzunluk çıkarılamazsa farklı kabul et
                        return false;
                    }
                }
            }

            // DECIMAL, NUMERIC gibi sayısal tipler için hassasiyet/ölçek kontrolü
            if (baseType1 == "DECIMAL" || baseType1 == "NUMERIC")
            {
                try
                {
                    string precision1 = ExtractPrecisionFromType(normalizedType1);
                    string precision2 = ExtractPrecisionFromType(normalizedType2);

                    if (precision1 != precision2)
                        return false;
                }
                catch
                {
                    // Hassasiyet çıkarılamazsa farklı kabul et
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Parantez içinden sayısal değeri çıkarır, örn: NVARCHAR(100) -> 100
        /// </summary>
        private int ExtractNumberFromType(string type)
        {
            int start = type.IndexOf("(") + 1;
            int end = type.IndexOf(")");
            if (start > 0 && end > start)
            {
                string numberStr = type.Substring(start, end - start);
                if (int.TryParse(numberStr, out int number))
                    return number;
            }
            return 0;
        }

        /// <summary>
        /// Parantez içinden hassasiyet/ölçek değerini çıkarır, örn: DECIMAL(18,2) -> "18,2"
        /// </summary>
        private string ExtractPrecisionFromType(string type)
        {
            int start = type.IndexOf("(") + 1;
            int end = type.IndexOf(")");
            if (start > 0 && end > start)
            {
                return type.Substring(start, end - start);
            }
            return "";
        }

        /// <summary>
        /// Tablo için primary key kısıtlamasını günceller
        /// </summary>
        private async Task UpdatePrimaryKeyConstraintAsync(SqlConnection connection, string tableName, string columnName, bool addAsPrimaryKey)
        {
            try
            {
                // Mevcut PK kısıtlaması varsa bul ve kaldır
                string constraintName = null;
                using (var command = new SqlCommand(
                    @"SELECT kcu.CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_NAME = @TableName",
                    connection))
                {
                    command.Parameters.AddWithValue("@TableName", tableName);
                    var result = await command.ExecuteScalarAsync();
                    if (result != null)
                    {
                        constraintName = result.ToString();

                        // Mevcut PK kısıtlamasını kaldır
                        using (var dropCommand = new SqlCommand($"ALTER TABLE [{tableName}] DROP CONSTRAINT [{constraintName}]", connection))
                        {
                            await dropCommand.ExecuteNonQueryAsync();
                            _logger.LogInformation($"Mevcut primary key kısıtlaması kaldırıldı: {constraintName}");
                        }
                    }
                }

                // Yeni primary key eklenecekse
                if (addAsPrimaryKey)
                {
                    // Yeni PK kısıtlaması oluştur
                    string newConstraintName = $"PK_{tableName}_{columnName}";
                    using (var addCommand = new SqlCommand($"ALTER TABLE [{tableName}] ADD CONSTRAINT [{newConstraintName}] PRIMARY KEY ([{columnName}])", connection))
                    {
                        await addCommand.ExecuteNonQueryAsync();
                        _logger.LogInformation($"Yeni primary key kısıtlaması eklendi: {newConstraintName} ({columnName})");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Primary key güncellenirken hata: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// SQL tipinden CLR tipini belirler (yazılamayan yeni helper metod)
        /// </summary>
        private Type GetClrTypeFromSqlType(string sqlType, int length = 0, int precision = 0)
        {
            // SQL tipi büyük harfe çevir ve parantez içindeki değerleri temizle
            string normalizedType = sqlType.ToUpper();
            if (normalizedType.Contains("("))
                normalizedType = normalizedType.Substring(0, normalizedType.IndexOf("("));

            switch (normalizedType)
            {
                case "INT":
                case "INTEGER":
                    return typeof(int);
                case "BIGINT":
                    return typeof(long);
                case "SMALLINT":
                    return typeof(short);
                case "TINYINT":
                    return typeof(byte);
                case "BIT":
                    return typeof(bool);
                case "DECIMAL":
                case "NUMERIC":
                case "MONEY":
                case "SMALLMONEY":
                    return typeof(decimal);
                case "FLOAT":
                    return typeof(double);
                case "REAL":
                    return typeof(float);
                case "DATE":
                case "DATETIME":
                case "DATETIME2":
                case "SMALLDATETIME":
                    return typeof(DateTime);
                case "DATETIMEOFFSET":
                    return typeof(DateTimeOffset);
                case "TIME":
                    return typeof(TimeSpan);
                case "UNIQUEIDENTIFIER":
                    return typeof(Guid);
                case "VARBINARY":
                case "BINARY":
                case "IMAGE":
                    return typeof(byte[]);

                default:
                    // Varsayılan olarak string tipini döndür
                    return typeof(string);
            }
        }

        /// <summary>
        /// CLR (C#) veri tipini SQL Server veri tipine dönüştürür
        /// </summary>
        private string GetSqlTypeFromClrType(Type type, int? length = null, int? precision = null, int? scale = null)
        {
            if (type == typeof(int) || type == typeof(Int32))
                return "INT";

            if (type == typeof(long) || type == typeof(Int64))
                return "BIGINT";

            if (type == typeof(short) || type == typeof(Int16))
                return "SMALLINT";

            if (type == typeof(byte))
                return "TINYINT";

            if (type == typeof(bool))
                return "BIT";

            if (type == typeof(decimal))
            {
                int p = precision.HasValue && precision.Value > 0 ? precision.Value : 18;
                int s = scale.HasValue && scale.Value >= 0 ? scale.Value : 2;
                return $"DECIMAL({p},{s})";
            }

            if (type == typeof(double))
            {
                int p = precision.HasValue && precision.Value > 0 ? precision.Value : 53;
                return $"FLOAT({p})";
            }

            if (type == typeof(float))
                return "REAL";

            if (type == typeof(DateTime))
            {
                if (precision.HasValue)
                    return $"DATETIME2({(precision.Value >= 0 && precision.Value <= 7 ? precision.Value : 7)})";
                else
                    return "DATETIME";
            }

            if (type == typeof(DateTimeOffset))
            {
                if (precision.HasValue)
                    return $"DATETIMEOFFSET({(precision.Value >= 0 && precision.Value <= 7 ? precision.Value : 7)})";
                else
                    return "DATETIMEOFFSET";
            }

            if (type == typeof(TimeSpan))
            {
                if (precision.HasValue)
                    return $"TIME({(precision.Value >= 0 && precision.Value <= 7 ? precision.Value : 7)})";
                else
                    return "TIME";
            }

            if (type == typeof(Guid))
                return "UNIQUEIDENTIFIER";

            if (type == typeof(byte[]))
            {
                if (length.HasValue)
                {
                    if (length.Value > 8000 || length.Value == -1)
                        return "VARBINARY(MAX)";
                    else
                        return $"VARBINARY({length.Value})";
                }
                else
                    return "VARBINARY(MAX)";
            }

            // String değerler için
            if (type == typeof(string))
            {
                if (length.HasValue)
                {
                    if (length.Value > 8000 || length.Value == -1)
                        return "NVARCHAR(MAX)";
                    else
                        return $"NVARCHAR({length.Value})";
                }
                else
                    return "NVARCHAR(255)";
            }

            // Bilinmeyen tipler için varsayılan
            return "NVARCHAR(255)";
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