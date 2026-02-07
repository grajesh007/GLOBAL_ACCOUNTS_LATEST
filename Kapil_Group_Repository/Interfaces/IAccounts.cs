namespace Kapil_Group_Repository.Interfaces;

public interface IAccounts
{
    List<string> GetBankDetails(string connectionString,string GlobalSchema,string AccountsSchema);
}
