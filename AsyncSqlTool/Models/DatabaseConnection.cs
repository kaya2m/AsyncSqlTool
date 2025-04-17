using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace AsyncSqlTool.Models
{
    /// <summary>
    /// Veritabanı bağlantı bilgilerini tutan model
    /// </summary>
    public class DatabaseConnection
    {
        [Key]
        public int Id { get; set; }

        [Required]
        [StringLength(100)]
        public string Name { get; set; }

        [Required]
        [StringLength(50)]
        public string DatabaseType { get; set; } // HANA, MSSQL, MySQL, PostgreSQL vb.

        [Required]
        [StringLength(1000)]
        public string ConnectionString { get; set; }

        [StringLength(500)]
        public string Description { get; set; }

        public bool IsActive { get; set; } = true;

        public DateTime CreatedAt { get; set; } = DateTime.Now;

        public DateTime? UpdatedAt { get; set; }

        // İlişkiler
        public virtual ICollection<SavedQuery> SavedQueries { get; set; }
    }
}