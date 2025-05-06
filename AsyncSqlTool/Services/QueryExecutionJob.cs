using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Quartz;

namespace AsyncSqlTool.Services
{
    /// <summary>
    /// Zamanlanmış sorgu çalıştırma görevi
    /// </summary>
    [DisallowConcurrentExecution]
    public class QueryExecutionJob : IJob
    {
        private readonly QueryService _queryService;
        private readonly ILogger<QueryExecutionJob> _logger;

        public QueryExecutionJob(
            QueryService queryService,
            ILogger<QueryExecutionJob> logger)
        {
            _queryService = queryService;
            _logger = logger;
        }

        public async Task Execute(IJobExecutionContext context)
        {
            var queryId = context.JobDetail.JobDataMap.GetInt("queryId");
            _logger.LogInformation($"Zamanlanmış sorgu görevi başlatılıyor, ID: {queryId}, Tetikleme: {context.Trigger.Key}");

            try
            {
                var result = await _queryService.ExecuteScheduledQueryAsync(queryId);

                if (result.success)
                {
                    _logger.LogInformation(
                        $"Zamanlanmış sorgu başarıyla tamamlandı, ID: {queryId}, " +
                        $" Sonuç: {result.message}");
                }
                else
                {
                    _logger.LogWarning(
                        $"Zamanlanmış sorgu başarısız oldu, ID: {queryId}, " +
                        $"Hata: {result.message}");
                }

                // İşlem detaylarını Job veri haritasına ekle
                context.Result = new
                {
                    QueryId = queryId,
                    Success = result.success,
                    Message = result.message,
                    CompletionTime = DateTime.Now
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    $"Zamanlanmış sorgu çalıştırılırken beklenmeyen hata oluştu, ID: {queryId}: {ex.Message}");

                // Hatayı fırlat, böylece Quartz işleme alabilir ve tekrar çalıştırabilir
                throw new JobExecutionException(
                    $"Sorgu {queryId} için beklenmeyen hata: {ex.Message}", ex, false);
            }
        }
    }
}