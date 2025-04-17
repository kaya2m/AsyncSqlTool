using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace AsyncSqlTool.Models
{
    /// <summary>
    /// Sorgu çalıştırma loglarını tutan model
    /// </summary>
    public class QueryExecutionLog
    {
        [Key]
        public int Id { get; set; }

        public int SavedQueryId { get; set; }

        public DateTime ExecutionTime { get; set; } = DateTime.Now;

        public bool IsSuccess { get; set; }

        public int RecordsAffected { get; set; }

        [StringLength(1000)]
        public string Message { get; set; }

        [StringLength(4000)]
        public string ErrorDetails { get; set; }

        public TimeSpan ExecutionDuration { get; set; }

        [ForeignKey("SavedQueryId")]
        public virtual SavedQuery SavedQuery { get; set; }
    }
}