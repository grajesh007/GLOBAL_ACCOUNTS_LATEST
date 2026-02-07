using System;
using System.Collections.Generic;
using System.Data;
using Npgsql;
using Kapil_Group_Repository.Interfaces;

namespace Kapil_Group_Infrastructure.Data;

public class AccountsDAL : IAccounts
{
    /// <summary>
    /// Returns a list of bank names from the database.
    /// Update the SQL in this method to match your actual table/schema.
    /// </summary>
    public List<string> GetBankDetails(string connectionString, string globalSchema, string accountsSchema)
    {
        var bankList = new List<string>();

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is null or empty", nameof(connectionString));

        try
        {
            // Validate/parse connection string to get clearer errors early
            NpgsqlConnectionStringBuilder builder;
            try
            {
                builder = new NpgsqlConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Invalid connection string format", nameof(connectionString), ex);
            }

            // Use explicit type so debugger shows it clearly
            using (NpgsqlConnection con = new NpgsqlConnection(builder.ConnectionString))
            {
                con.Open();
                Console.WriteLine(con.State); // Expect Open

                using var cmd = con.CreateCommand();
                // TODO: Replace the query below with the correct table and column names for your DB
                cmd.CommandText = $"select t1.tbl_mst_bank_configuration_id,bank_account_id, case when (account_number is null or coalesce(account_number,'')='') then account_name else account_name||' - '|| account_number end as bankname,bank_branch,sum(coalesce(bankbookbalance,0)) as bankbookbalance,sum(coalesce(bankbookbalance,0))+sum(coalesce(passbookbalance,0)) passbookbalance,isprimary,isformanbank,is_foreman_payment_bank,is_interest_payment_bank from " + AddDoubleQuotes(accountsSchema) + ".tbl_mst_bank_configuration t1 left join (select t1.tbl_mst_bank_configuration_id as recordid,coalesce(sum( coalesce(debitamount,0)-coalesce(creditamount,0)),0) as bankbookbalance from " + AddDoubleQuotes(accountsSchema) + ".tbl_mst_bank_configuration t1 join " + AddDoubleQuotes(accountsSchema) + ".tbl_trans_total_transactions t2 on t1.bank_account_id=t2.parent_id group by t1.tbl_mst_bank_configuration_id)t2 on t1.tbl_mst_bank_configuration_id=t2.recordid left join (select deposited_bank_id,sum(passbookbalance)passbookbalance from (select bank_configuration_id as deposited_bank_id,paid_amount as passbookbalance  from " + AddDoubleQuotes(accountsSchema) + ".tbl_trans_payment_reference  where clear_status ='N' union all select deposited_bank_id,-total_received_amount as passbookbalance from  " + AddDoubleQuotes(accountsSchema) + ".tbl_trans_receipt_reference  where deposit_status  ='P' and clear_status='N' )x group by deposited_bank_id)t3 on t1.tbl_mst_bank_configuration_id=t3.deposited_bank_id left join " + AddDoubleQuotes(globalSchema) + ".tbl_mst_bank t4 on t1.bank_id=t4.tbl_mst_bank_id where t1.status=true group by t1.tbl_mst_bank_configuration_id,bank_name,case when (account_number is null or coalesce(account_number,'')='') then account_name else account_name||' - '|| account_number end,bank_account_id ,bank_branch,isprimary,isformanbank,is_foreman_payment_bank,is_interest_payment_bank order by bankname;";
                cmd.CommandType = CommandType.Text;

                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    // The first column can be an integer id or string; convert safely to string and avoid nullability warning
                    bankList.Add(reader.IsDBNull(0) ? string.Empty : (Convert.ToString(reader.GetValue(0)) ?? string.Empty));
                }
            }
        }
        catch (Exception ex)
        {
            // Surface a clearer message to the caller and preserve original exception
            throw new InvalidOperationException($"Failed to retrieve bank details (schema={globalSchema}). See inner exception for details.", ex);
        }

        return bankList;
    }

     public static object AddDoubleQuotes(object value)
        {
            return "\"" + value + "\"";
        }
}
