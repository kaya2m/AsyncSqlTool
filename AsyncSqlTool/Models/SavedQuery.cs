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
        public string Name { get; set; }

        [Required]
        public string QueryText { get; set; }

        [Required]
        public string TargetTableName { get; set; }

        public string KeyColumn { get; set; }

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