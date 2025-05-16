using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
namespace AsyncSqlTool.Models
{
    /// <summary>
    /// Sorgu kolonlarını ve türlerini tutan model
    /// </summary>
    public class QueryColumnMapping
    {
        [Key]
        public int Id { get; set; }
        public int SavedQueryId { get; set; }
        [Required]
        [StringLength(100)]
        public string SourceColumnName { get; set; }
        [Required]
        [StringLength(100)]
        public string TargetColumnName { get; set; }
        [Required]
        [StringLength(50)]
        public string DataType { get; set; } // VARCHAR, INT, DECIMAL vb.
        public int Length { get; set; } = 255; // VARCHAR(100) için 100 gibi
        public int Precision { get; set; } // DECIMAL(18,2) için 18 gibi
        public int Scale { get; set; } // DECIMAL(18,2) için 2 gibi
        public bool IsPrimaryKey { get; set; } = false;
        public bool AllowNull { get; set; } = true;
        public int SortOrder { get; set; }
        [ForeignKey("SavedQueryId")]
        public virtual SavedQuery SavedQuery { get; set; }
    }
}