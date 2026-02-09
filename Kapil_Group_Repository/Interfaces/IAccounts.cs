
using Kapil_Group_Repository.Entities;

namespace Kapil_Group_Repository.Interfaces;

public interface IAccounts
{
    List<string> GetBankDetails(string connectionString,string GlobalSchema,string AccountsSchema);
  List<Bank> GetBankDetails1(string connectionString, string globalSchema, string accountsSchema,
 string BranchCode, string CompanyName);
}
