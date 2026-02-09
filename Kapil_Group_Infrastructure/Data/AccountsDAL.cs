using System;
using System.Collections.Generic;
using System.Data;
using Npgsql;
using Kapil_Group_Repository.Interfaces;
using Kapil_Group_Repository.Entities;

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


 public List<Bank> GetBankDetails1(string connectionString, string globalSchema, string accountsSchema,string BranchCode, string CompanyName)
    {
       List<Bank> bankList = new List<Bank>();

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
                cmd.CommandText = $"select tbl_mst_bank_configuration_id,case when (account_number is null or account_number='') then  coalesce(account_name,'') else (coalesce(account_name, '') || ' - ' ||coalesce(account_number, '')) end as bank_name,bank_branch,ifsccode,account_type from " + AddDoubleQuotes(accountsSchema) + ".tbl_mst_bank_configuration t1 join " + AddDoubleQuotes(globalSchema) + ".tbl_mst_bank t2 on t1.bank_id = t2.tbl_mst_bank_id where t1.status = true and t1.company_code='"+CompanyName+"' and t1.branch_code='"+BranchCode+"' order by bank_name;";
                cmd.CommandType = CommandType.Text;

                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    // The first column can be an integer id or string; convert safely to string and avoid nullability warning
                    // bankList.Add(reader.IsDBNull(0) ? string.Empty : (Convert.ToString(reader.GetValue(0)) ?? string.Empty));
                    // bankList.Add(reader.IsDBNull(0) ? string.Empty : (Convert.ToString(reader.GetValue(1)) ?? string.Empty));
                    // bankList.Add(reader.IsDBNull(0) ? string.Empty : (Convert.ToString(reader.GetValue(2)) ?? string.Empty));
                    // bankList.Add(reader.IsDBNull(0) ? string.Empty : (Convert.ToString(reader.GetValue(3)) ?? string.Empty));
                    // bankList.Add(reader.IsDBNull(0) ? string.Empty : (Convert.ToString(reader.GetValue(4)) ?? string.Empty));
                     Bank obj = new Bank();

                obj.tbl_mst_bank_configuration_id =
                    reader.IsDBNull(0) ? 0 : (int)reader.GetInt64(0);
  

                obj.BankName =
                    reader.IsDBNull(1) ? string.Empty : reader.GetString(1);

                obj.bankbranch =
                    reader.IsDBNull(2) ? string.Empty : reader.GetString(2);

                obj.ifsccode =
                    reader.IsDBNull(3) ? string.Empty : reader.GetString(3);

                obj.accounttype =
                    reader.IsDBNull(4) ? string.Empty : reader.GetString(4);

                bankList.Add(obj);
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


                public async Task<List<AccountReportsDTO>> GetBankBookDetails(string con, string fromDate, string toDate, long _pBankAccountId, string AccountsSchema, string GlobalSchema,string CompanyCode, string BranchCode)
        {
            await Task.Run(() =>
            {
                lstcashbook = new List<AccountReportsDTO>();
                try
                {
                    long BankId = Convert.ToInt64(NPGSqlHelper.ExecuteScalar(con, CommandType.Text, "select bank_account_id from   " + AddDoubleQuotes(AccountsSchema) + ".tbl_mst_bank_configuration  where tbl_mst_bank_configuration_id=" + _pBankAccountId + " and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'"));


                    string Query = "select row_number() over (order by transaction_date) as recordid,* from (select transaction_date::text,TRANSACTION_NO,PARTICULARS,narration,DEBITAMOUNT,abs(CREDITAMOUNT)CREDITAMOUNT,abs(balance)balance,case when balance>0 then 'Dr' else 'Cr' end as balancetype from (select *,sum(DEBITAMOUNT+CREDITAMOUNT) OVER(ORDER BY transaction_date,RECORDID)as BALANCE from(SELECT 0 AS RECORDID,CAST('" + FormatDate(fromDate) + "' AS DATE) AS transaction_date,'0' AS TRANSACTION_NO,'Opening Balance' AS PARTICULARS,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)>0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END AS  DEBITAMOUNT,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)<0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END  AS CREDITAMOUNT,'' narration  FROM " + AddDoubleQuotes(AccountsSchema) + ".tbl_trans_total_transactions WHERE transaction_date < '" + FormatDate(fromDate) + "'  AND  account_id=" + BankId + " UNION ALL SELECT tbl_trans_total_transactions_id as RECORDID, transaction_date,TRANSACTION_NO,PARTICULARS,COALESCE(DEBITAMOUNT,0.00) as  DEBITAMOUNT,-COALESCE(CREDITAMOUNT,0.00) as CREDITAMOUNT,narration FROM  " + AddDoubleQuotes(AccountsSchema) + ".tbl_trans_total_transactions WHERE transaction_date BETWEEN  '" + FormatDate(fromDate) + "' AND '" + FormatDate(toDate) + "' AND account_id=" + BankId + " AND ( debitamount<>0 or creditamount<>0) order by transaction_date,RECORDID) as D) x union all select transaction_date::text,TRANSACTION_NO,PARTICULARS,narration,DEBITAMOUNT,abs(CREDITAMOUNT) CREDITAMOUNT,abs(balance) balance,case when balance>0 then 'Dr' else 'Cr' end as balancetype from (SELECT 0 AS RECORDID,CAST('" + FormatDate(toDate) + "' AS DATE) AS transaction_date,'0' AS TRANSACTION_NO,'Closing Balance' AS PARTICULARS,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)>0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END AS  DEBITAMOUNT,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)<0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END  AS CREDITAMOUNT,'' narration, COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) AS balance  FROM " + AddDoubleQuotes(AccountsSchema) + ".tbl_trans_total_transactions WHERE transaction_date <='" + FormatDate(toDate) + "'  AND  account_id=" + BankId + " order by recordid,transaction_date ) x) x order by recordid;";


                    using (NpgsqlDataReader dr = NPGSqlHelper.ExecuteReader(con, CommandType.Text, Query))
                    {
                        while (dr.Read())
                        {
                            AccountReportsDTO _ObjBank = new AccountReportsDTO();
                            _ObjBank.precordid = Convert.ToInt64(dr["recordid"]);
                            _ObjBank.ptransactiondate = dr["transaction_date"];
                            _ObjBank.pcreditamount = Convert.ToDouble(dr["CREDITAMOUNT"]);
                            _ObjBank.pdebitamount = Convert.ToDouble(dr["DEBITAMOUNT"]);
                            _ObjBank.pdescription = Convert.ToString(dr["narration"]);
                            _ObjBank.pparticulars = Convert.ToString(dr["PARTICULARS"]);
                            _ObjBank.ptransactionno = Convert.ToString(dr["TRANSACTION_NO"]);
                            _ObjBank.popeningbal = Convert.ToDouble(dr["balance"]);
                            _ObjBank.pBalanceType = Convert.ToString(dr["balancetype"]);
                            lstcashbook.Add(_ObjBank);
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            });
            return lstcashbook;
        }

}
