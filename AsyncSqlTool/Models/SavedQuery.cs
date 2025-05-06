using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace AsyncSqlTool.Models
{
    /// <summary>
    /// Kaydedilmiş sorguları tutan model
    /// </summary>
    public class SavedQuery
    {
        [Key]
        public int Id { get; set; }

        [Required]
        [StringLength(100)]
        public string Name { get; set; }

        [Required]
        [StringLength(4000)]
        public string QueryText { get; set; }

        [Required]
        [StringLength(100)]
        public string TargetTableName { get; set; }

        [StringLength(100)]
        public string KeyColumn { get; set; }

        [StringLength(500)]
        public string Description { get; set; }

        public bool IsScheduled { get; set; } = false;
        public DateTime? NextScheduledRun { get; set; }


        [StringLength(50)]
        public string ScheduleExpression { get; set; } // CRON ifadesi olabilir

        public DateTime CreatedAt { get; set; } = DateTime.Now;

        public DateTime? LastExecuted { get; set; }
        public string PreQuery { get; set; }

        public string PostQuery { get; set; }

        public int DatabaseConnectionId { get; set; }

        [ForeignKey("DatabaseConnectionId")]
        public virtual DatabaseConnection DatabaseConnection { get; set; }

        // İlişkiler
        public virtual ICollection<QueryColumnMapping> ColumnMappings { get; set; }
        public virtual ICollection<QueryExecutionLog> ExecutionLogs { get; set; }

    }
}