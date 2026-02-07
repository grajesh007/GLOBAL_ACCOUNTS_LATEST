using Microsoft.EntityFrameworkCore;
using Kapil_Group_Repository.Entities;

namespace Kapil_Group_Infrastructure.Data
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
            : base(options)
        {
        }

        // Example DbSet - add your entities as needed
        public DbSet<Bank>? Banks { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            // Example configuration - adjust as needed
            modelBuilder.Entity<Bank>(b =>
            {
                b.HasKey(x => x.Id);
                b.Property(x => x.BankName).HasMaxLength(200);
            });
        }
    }
}
