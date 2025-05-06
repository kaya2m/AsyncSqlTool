using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AsyncSqlTool.Models;
using AsyncSqlTool.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Quartz;
using Quartz.Impl;
using Quartz.Spi;
using Quartz.Impl.Matchers;

namespace AsyncSqlTool.Services
{
    /// <summary>
    /// Zamanlanmış görevleri yöneten ve arka planda çalışan hosted servis
    /// </summary>
    public class QuerySchedulerHostedService : IHostedService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<QuerySchedulerHostedService> _logger;
        private readonly ISchedulerFactory _schedulerFactory;

        // Diğer servisler tarafından erişilecek olan scheduler'a static erişim sağlama
        public static IScheduler Scheduler { get; private set; }

        public QuerySchedulerHostedService(
            IServiceProvider serviceProvider,
            ILogger<QuerySchedulerHostedService> logger,
            ISchedulerFactory schedulerFactory)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _schedulerFactory = schedulerFactory;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                _logger.LogInformation("Sorgu zamanlayıcı servisi başlatılıyor...");

                // Scheduler'ı başlat
                var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);

                // Özel job factory kullan
                scheduler.JobFactory = new JobFactory(_serviceProvider);

                await scheduler.Start(cancellationToken);

                // Static referans için atama
                Scheduler = scheduler;

                // Zamanlanmış görevleri yükle
                await LoadScheduledQueries(cancellationToken);

                _logger.LogInformation("Sorgu zamanlayıcı servisi başarıyla başlatıldı.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Sorgu zamanlayıcı servisi başlatılırken hata oluştu: {Message}", ex.Message);
                throw;
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            if (Scheduler != null)
            {
                await Scheduler.Shutdown(cancellationToken);
                _logger.LogInformation("Sorgu zamanlayıcı servisi durduruldu.");
            }
        }

        /// <summary>
        /// Kaydedilmiş zamanlanmış sorguları yükleme
        /// </summary>
        private async Task LoadScheduledQueries(CancellationToken cancellationToken)
        {
            using (var scope = _serviceProvider.CreateScope())
            {
                try
                {
                    var dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
                    var scheduledQueries = await dbContext.SavedQueries
                        .Where(q => q.IsScheduled && !string.IsNullOrEmpty(q.ScheduleExpression))
                        .ToListAsync(cancellationToken);

                    foreach (var query in scheduledQueries)
                    {
                        await ScheduleQuery(query.Id, query.ScheduleExpression, cancellationToken);
                    }

                    _logger.LogInformation($"{scheduledQueries.Count} zamanlanmış görev yüklendi.");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Zamanlanmış sorgular yüklenirken hata oluştu: {Message}", ex.Message);
                }
            }
        }

        /// <summary>
        /// Sorgu için zamanlanmış görev oluşturma (iç kullanım için)
        /// </summary>
        private async Task<bool> ScheduleQuery(int queryId, string cronExpression, CancellationToken cancellationToken = default)
        {
            if (Scheduler == null)
            {
                _logger.LogError("Scheduler henüz başlatılmadı - görev oluşturulamadı.");
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

                await Scheduler.ScheduleJob(jobDetail, trigger, cancellationToken);

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
        /// Zamanlanmış görevi kaldırma (iç kullanım için)
        /// </summary>
        private async Task<bool> UnscheduleQuery(int queryId, CancellationToken cancellationToken = default)
        {
            if (Scheduler == null)
            {
                _logger.LogError("Scheduler henüz başlatılmadı - görev kaldırılamadı.");
                return false;
            }

            try
            {
                var jobKey = new JobKey($"query-{queryId}", "queries");
                var triggerKey = new TriggerKey($"trigger-{queryId}", "queries");

                // Önce tetikleyiciyi kaldır
                await Scheduler.UnscheduleJob(triggerKey, cancellationToken);

                // Sonra görevi kaldır
                await Scheduler.DeleteJob(jobKey, cancellationToken);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Sorgu zamanlaması kaldırılırken hata oluştu, ID: {queryId}: {ex.Message}");
                return false;
            }
        }
    }

    /// <summary>
    /// Quartz Job için özel job factory
    /// </summary>
    public class JobFactory : IJobFactory
    {
        private readonly IServiceProvider _serviceProvider;

        public JobFactory(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public IJob NewJob(TriggerFiredBundle bundle, IScheduler scheduler)
        {
            // Her job çalıştırması için yeni bir scope oluştur
            var scope = _serviceProvider.CreateScope();

            try
            {
                // Job'ı bu scope'tan oluştur
                var job = ActivatorUtilities.CreateInstance(scope.ServiceProvider, bundle.JobDetail.JobType) as IJob;

                // Scope'u job'a ata (IServiceScope implementasyonu varsa)
                return new ScopedJob(job, scope);
            }
            catch
            {
                // Hata durumunda scope'u dispose et
                scope.Dispose();
                throw;
            }
        }

        public void ReturnJob(IJob job)
        {
            // Scope'u dispose et
            (job as ScopedJob)?.Dispose();
        }
    }

    /// <summary>
    /// IServiceScope ile IJob'ı sarmalayan sınıf
    /// </summary>
    public class ScopedJob : IJob, IDisposable
    {
        private readonly IJob _job;
        private readonly IServiceScope _scope;

        public ScopedJob(IJob job, IServiceScope scope)
        {
            _job = job;
            _scope = scope;
        }

        public async Task Execute(IJobExecutionContext context)
        {
            await _job.Execute(context);
        }

        public void Dispose()
        {
            _scope.Dispose();
        }
    }
}