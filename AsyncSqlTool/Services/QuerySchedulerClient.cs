using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AsyncSqlTool.Models;
using AsyncSqlTool.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Quartz;
using Quartz.Impl.Matchers;

namespace AsyncSqlTool.Services
{
    /// <summary>
    /// Zamanlı görevlerin yönetimi için UI tarafında kullanılacak istemci servisi
    /// </summary>
    public class QuerySchedulerClient
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<QuerySchedulerClient> _logger;

        public QuerySchedulerClient(
            IServiceProvider serviceProvider,
            ILogger<QuerySchedulerClient> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }

        /// <summary>
        /// Sorgu için zamanlanmış görev oluşturma
        /// </summary>
        public async Task<bool> ScheduleQuery(int queryId, string cronExpression, CancellationToken cancellationToken = default)
        {
            var scheduler = QuerySchedulerHostedService.Scheduler;
            if (scheduler == null)
            {
                _logger.LogError("Scheduler henüz başlatılmadı. Görev oluşturulamadı.");
                return false;
            }

            try
            {
                // Mevcut görevi varsa kaldır
                await UnscheduleQuery(queryId, cancellationToken);

                var jobDetail = JobBuilder.Create<QueryExecutionJob>()
                    .WithIdentity($"query-{queryId}", "queries")
                    .UsingJobData("queryId", queryId)
                    .Build();

                var trigger = TriggerBuilder.Create()
                    .WithIdentity($"trigger-{queryId}", "queries")
                    .WithCronSchedule(cronExpression)
                    .WithDescription($"Zamanlanmış sorgu çalıştırma - ID: {queryId}")
                    .Build();

                await scheduler.ScheduleJob(jobDetail, trigger, cancellationToken);

                // İlk tetikleme zamanını kaydet
                DateTimeOffset? nextFireTime = trigger.GetNextFireTimeUtc();

                using (var scope = _serviceProvider.CreateScope())
                {
                    var dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
                    var query = await dbContext.SavedQueries.FindAsync(queryId);

                    if (query != null)
                    {
                        query.IsScheduled = true;
                        query.ScheduleExpression = cronExpression;
                        query.NextScheduledRun = nextFireTime?.LocalDateTime;
                        await dbContext.SaveChangesAsync(cancellationToken);
                    }
                }

                _logger.LogInformation($"Sorgu zamanlandı, ID: {queryId}, Cron: {cronExpression}, Sonraki çalışma: {nextFireTime}");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Sorgu zamanlanırken hata oluştu, ID: {queryId}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Zamanlanmış görevi kaldırma
        /// </summary>
        public async Task<bool> UnscheduleQuery(int queryId, CancellationToken cancellationToken = default)
        {
            var scheduler = QuerySchedulerHostedService.Scheduler;
            if (scheduler == null)
            {
                _logger.LogError("Scheduler henüz başlatılmadı. Görev kaldırılamadı.");
                return false;
            }

            try
            {
                var jobKey = new JobKey($"query-{queryId}", "queries");
                var triggerKey = new TriggerKey($"trigger-{queryId}", "queries");

                // Önce tetikleyiciyi kaldır
                await scheduler.UnscheduleJob(triggerKey, cancellationToken);

                // Sonra görevi kaldır
                await scheduler.DeleteJob(jobKey, cancellationToken);

                // Veritabanında zamanlanmış durumunu güncelle
                using (var scope = _serviceProvider.CreateScope())
                {
                    var dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
                    var query = await dbContext.SavedQueries.FindAsync(queryId);

                    if (query != null)
                    {
                        query.IsScheduled = false;
                        query.NextScheduledRun = null;
                        await dbContext.SaveChangesAsync(cancellationToken);
                    }
                }

                _logger.LogInformation($"Sorgu zamanlama kaldırıldı, ID: {queryId}");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Sorgu zamanlaması kaldırılırken hata oluştu, ID: {queryId}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Tüm zamanlanmış görevlerin listesini getirme
        /// </summary>
        public async Task<List<ScheduledQueryInfo>> GetScheduledQueries(CancellationToken cancellationToken = default)
        {
            var result = new List<ScheduledQueryInfo>();

            try
            {
                // Veritabanından zamanlanmış sorguları al
                using (var scope = _serviceProvider.CreateScope())
                {
                    var dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
                    var scheduledQueries = await dbContext.SavedQueries
                        .Where(q => q.IsScheduled && !string.IsNullOrEmpty(q.ScheduleExpression))
                        .Select(q => new ScheduledQueryInfo
                        {
                            QueryId = q.Id,
                            QueryName = q.Name,
                            CronExpression = q.ScheduleExpression,
                            NextRun = q.NextScheduledRun,
                            Description = q.Description,
                            TargetTable = q.TargetTableName
                        })
                        .ToListAsync(cancellationToken);

                    result.AddRange(scheduledQueries);
                }

                // Quartz'dan gerçek zamanlı bilgileri al
                var scheduler = QuerySchedulerHostedService.Scheduler;
                if (scheduler != null)
                {
                    foreach (var jobKey in await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("queries"), cancellationToken))
                    {
                        var triggers = await scheduler.GetTriggersOfJob(jobKey, cancellationToken);
                        foreach (var trigger in triggers)
                        {
                            int queryId = Convert.ToInt32(jobKey.Name.Replace("query-", ""));
                            var queryInfo = result.FirstOrDefault(q => q.QueryId == queryId);

                            if (queryInfo != null)
                            {
                                var nextFireTime = trigger.GetNextFireTimeUtc();
                                if (nextFireTime.HasValue)
                                {
                                    queryInfo.NextRun = nextFireTime.Value.LocalDateTime;
                                }
                            }
                        }
                    }
                }
                else
                {
                    _logger.LogWarning("Scheduler henüz başlatılmadı, gerçek zamanlı bilgiler alınamadı.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Zamanlanmış sorguları getirirken hata oluştu: {ex.Message}");
            }

            return result;
        }

        /// <summary>
        /// Bir sorguyu manuel olarak çalıştırma
        /// </summary>
        public async Task<bool> ExecuteQueryManually(int queryId, CancellationToken cancellationToken = default)
        {
            var scheduler = QuerySchedulerHostedService.Scheduler;
            if (scheduler == null)
            {
                _logger.LogError("Scheduler henüz başlatılmadı. Sorgu çalıştırılamadı.");
                return false;
            }

            try
            {
                var jobDetail = JobBuilder.Create<QueryExecutionJob>()
                    .WithIdentity($"manual-query-{queryId}-{Guid.NewGuid()}", "manual-queries")
                    .UsingJobData("queryId", queryId)
                    .Build();

                var trigger = TriggerBuilder.Create()
                    .WithIdentity($"manual-trigger-{queryId}-{Guid.NewGuid()}", "manual-queries")
                    .StartNow()
                    .WithDescription($"Manuel sorgu çalıştırma - ID: {queryId}")
                    .Build();

                await scheduler.ScheduleJob(jobDetail, trigger, cancellationToken);

                _logger.LogInformation($"Sorgu manuel olarak çalıştırılıyor, ID: {queryId}");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Sorgu manuel olarak çalıştırılırken hata oluştu, ID: {queryId}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Bir cron ifadesinin sonraki çalışma zamanlarını hesaplama
        /// </summary>
        public List<DateTime> CalculateNextExecutionTimes(string cronExpression, int count = 5)
        {
            try
            {
                var result = new List<DateTime>();
                var cron = new CronExpression(cronExpression);

                DateTimeOffset? nextTime = DateTimeOffset.Now;

                for (int i = 0; i < count; i++)
                {
                    nextTime = cron.GetNextValidTimeAfter(nextTime.Value);

                    if (nextTime.HasValue)
                    {
                        result.Add(nextTime.Value.LocalDateTime);
                    }
                    else
                    {
                        break;
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Cron zamanları hesaplanırken hata oluştu: {ex.Message}");
                return new List<DateTime>();
            }
        }
    }

    /// <summary>
    /// Zamanlanmış sorgu bilgilerini taşıyan model
    /// </summary>
    public class ScheduledQueryInfo
    {
        public int QueryId { get; set; }
        public string QueryName { get; set; }
        public string CronExpression { get; set; }
        public DateTime? NextRun { get; set; }
        public string Description { get; set; }
        public string TargetTable { get; set; }
    }
}