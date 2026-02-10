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


    public List<Bank> GetBankDetails1(string connectionString, string globalSchema, string accountsSchema, string BranchCode, string CompanyName)
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
                cmd.CommandText = $"select tbl_mst_bank_configuration_id as bankaccountid,case when (account_number is null or coalesce(account_number,'')='') then account_name else account_name||' - '|| account_number end as bank_name from " + AddDoubleQuotes(globalSchema) + ".tbl_mst_bank t1 join " + AddDoubleQuotes(accountsSchema) + ".tbl_mst_bank_configuration  ts on t1.tbl_mst_bank_id =ts.bank_id  where t1.company_code='" + CompanyName + "'and t1.branch_code='" + BranchCode + "'order by bank_name;";
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

    #region BankNames   
    public List<BankNamesDetails> GetBankNamesDetails(
    string connectionString,
    string? globalSchema,
    string? accountsSchema,
    string? companyCode,
    string? branchCode)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is required");

        if (string.IsNullOrWhiteSpace(globalSchema) ||
            string.IsNullOrWhiteSpace(accountsSchema) ||
            string.IsNullOrWhiteSpace(companyCode) ||
            string.IsNullOrWhiteSpace(branchCode))
            return new List<BankNamesDetails>();

        var bankList = new List<BankNamesDetails>();

        using var con = new NpgsqlConnection(connectionString);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandType = CommandType.Text;

        cmd.CommandText = $@"
        SELECT 
            ts.tbl_mst_bank_configuration_id AS bankaccountid,
            CASE 
                WHEN (account_number IS NULL OR COALESCE(account_number,'') = '') 
                THEN account_name 
                ELSE account_name || ' - ' || account_number 
            END AS bank_name
        FROM {AddDoubleQuotes(globalSchema)}.tbl_mst_bank t1
        JOIN {AddDoubleQuotes(accountsSchema)}.tbl_mst_bank_configuration ts
            ON t1.tbl_mst_bank_id = ts.bank_id
        WHERE t1.company_code = @CompanyCode
          AND t1.branch_code = @BranchCode
        ORDER BY bank_name;
    ";

        cmd.Parameters.AddWithValue("@CompanyCode", companyCode);
        cmd.Parameters.AddWithValue("@BranchCode", branchCode);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            bankList.Add(new BankNamesDetails
            {
                BankAccountId = reader.GetInt32(0),
                BankName = reader.GetString(1),
                BankBranch = string.Empty,
                IFSCCode = string.Empty,
                AccountType = string.Empty
            });
        }

        return bankList;
    }



    #endregion BankNames
    #region companyNameandaddressDetails
    public List<CompanyBranchDetails> GetCompanyNameAndAddressDetails(
        string connectionString,
        string? globalSchema,
        string? companyCode,
        string? branchCode)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is required");

        if (string.IsNullOrWhiteSpace(globalSchema) ||
            string.IsNullOrWhiteSpace(companyCode) ||
            string.IsNullOrWhiteSpace(branchCode))
            return new List<CompanyBranchDetails>();

        var branchList = new List<CompanyBranchDetails>();

        using var con = new NpgsqlConnection(connectionString);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandType = CommandType.Text;

        cmd.CommandText = $@"
        SELECT
            b.tbl_mst_branch_configuration_id,
            c.company_name,
            c.company_code,
            b.branch_code,
            b.unique_branch_name,
            COALESCE(b.gst_number, b.state_code::text) AS gst_number,
            c.cin_number,
            c.registration_address,
            b.branch_name,
            b.state_code,
            b.branch_address,
            b.chit_register_address,
            COALESCE(b.backdate_enable_enddate >= CURRENT_DATE, FALSE) AS transaction_lock_status,
            COALESCE(c.application_version_no, '') AS application_version_no,
            CASE WHEN b.uncommenced_sjv_allow_status = 'Y' THEN TRUE ELSE FALSE END AS uncommenced_sjv_allow_status,
            CASE WHEN b.incharge_validate_on_subscriber = 'Y' THEN TRUE ELSE FALSE END AS incharge_validate_on_subscriber,
            COALESCE(b.display_name, '') AS display_name,
            CASE WHEN b.fin_closing_jv_allow_status = 'Y' THEN TRUE ELSE FALSE END AS fin_closing_jv_allow_status,
            COALESCE(b.legalcell_name, '') AS legalcell_name,
            c.receipt_restriction_no_of_dues AS legalcell_dues,
            CASE WHEN b.islegalgeneral_receipt_allow_status = 'Y' THEN TRUE ELSE FALSE END AS islegalgeneral_receipt_allow_status,
            c.chitfund_act_number,
            c.legal_dept_address,
            CASE WHEN b.isonlyefeereceipt_allow_status = 'Y' THEN TRUE ELSE FALSE END AS isonlyefeereceipt_allow_status,
            b.kgms_start_time,
            b.kgms_end_time,
            c.nonpayment_subscribers_graceperiod,
            c.receipt_restriction_no_of_dues_daywise AS legalcell_dues_days,
            c.max_chits_per_contact,
            CASE WHEN b.daywise_auctions = 'Y' THEN TRUE ELSE FALSE END AS daywise_auctions,
            CASE WHEN b.lc_calculatedpenalty_editable = 'Y' THEN TRUE ELSE FALSE END AS lc_calculatedpenalty_editable,
            b.onlineprocess_backdate_days,
            CASE WHEN b.iscontact_editable_allow_status = 'Y' THEN TRUE ELSE FALSE END AS iscontact_editable_allow_status,
            CASE WHEN b.fixed_chit_status = 'Y' THEN TRUE ELSE FALSE END AS fixed_chit_status,
            CASE WHEN b.is_auto_brs_imps_applicable = 'Y' THEN TRUE ELSE FALSE END AS is_auto_brs_imps_applicable,
            CASE WHEN b.issubscriber_nominee_allocation = 'Y' THEN TRUE ELSE FALSE END AS issubscriber_nominee_allocation,
            COALESCE(b.biometric_enable_enddate >= CURRENT_DATE, FALSE) AS biometric_date_lock_status
        FROM {AddDoubleQuotes(globalSchema)}.tbl_mst_branch_configuration b
        JOIN {AddDoubleQuotes(globalSchema)}.tbl_mst_chit_company_configuration c
            ON c.tbl_mst_chit_company_configuration_id = b.company_configuration_id
        WHERE b.branch_code = @BranchCode
          AND c.company_code = @CompanyCode;
    ";

        cmd.Parameters.AddWithValue("@BranchCode", branchCode);
        cmd.Parameters.AddWithValue("@CompanyCode", companyCode);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            branchList.Add(new CompanyBranchDetails
            {
                BranchId = reader.GetInt32(0),
                CompanyName = reader.GetString(1),
                CompanyCode = reader.GetString(2),
                BranchCode = reader.GetString(3),
                UniqueBranchName = reader.GetString(4),
                GstNumber = reader.GetString(5),
                CinNumber = reader.GetString(6),
                RegistrationAddress = reader.GetString(7),
                BranchName = reader.GetString(8),
                StateCode = reader.GetInt32(9),
                BranchAddress = reader.GetString(10),
                ChitRegisterAddress = reader.IsDBNull(11) ? string.Empty : reader.GetString(11),
                TransactionLockStatus = reader.GetBoolean(12),
                ApplicationVersionNo = reader.GetString(13),
                UncommencedSJVAllowStatus = reader.GetBoolean(14),
                InchargeValidateOnSubscriber = reader.GetBoolean(15),
                DisplayName = reader.GetString(16),
                FinClosingJVAllowStatus = reader.GetBoolean(17),
                LegalCellName = reader.GetString(18),
                LegalCellDues = reader.GetInt32(19).ToString(),
                IsLegalGeneralReceiptAllowStatus = reader.GetBoolean(20),
                ChitFundActNumber = reader.GetString(21),
                LegalDeptAddress = reader.GetString(22),
                IsOnlyFeeReceiptAllowStatus = reader.GetBoolean(23),
                KgmsStartTime = reader.GetString(24),
                KgmsEndTime = reader.GetString(25),
                NonPaymentSubscribersGracePeriod = reader.GetInt32(26),
                LegalCellDuesDays = reader.GetInt32(27),
                MaxChitsPerContact = reader.GetInt32(28),
                DaywiseAuctions = reader.GetBoolean(29),
                IcCalculatedPenaltyEditable = reader.GetBoolean(30),
                OnlineProcessBackDateDays = reader.GetInt32(31),
                IsContactEditableAllowStatus = reader.GetBoolean(32),
                FixedChitStatus = reader.GetBoolean(33),
                IsAutoBrsImpsApplicable = reader.GetBoolean(34),
                IsSubscriberNomineeAllocation = reader.GetBoolean(35),
                BiometricDateLockStatus = reader.GetBoolean(36)
            });
        }

        return branchList;
    }


    #endregion companyNameandaddressDetails 
    #region BankConfigurationdetails
    public List<BankConfigurationDetails> GetBankConfigurationDetails(
    string connectionString,
    string? branchSchema,
    string? companyCode,
    string? branchCode)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is required");

        if (string.IsNullOrWhiteSpace(branchSchema) ||
            string.IsNullOrWhiteSpace(companyCode) ||
            string.IsNullOrWhiteSpace(branchCode))
            return new List<BankConfigurationDetails>();

        var bankList = new List<BankConfigurationDetails>();

        using var con = new NpgsqlConnection(connectionString);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandType = CommandType.Text;

        cmd.CommandText = $@"
        SELECT 
            tbl_mst_bank_configuration_id,
            isprimary,
            isformanbank,
            is_foreman_payment_bank,
            is_interest_payment_bank
        FROM {AddDoubleQuotes(branchSchema)}.tbl_mst_bank_configuration
        WHERE status = true
          AND company_code = @CompanyCode
          AND branch_code = @BranchCode
        ORDER BY 1;
    ";

        cmd.Parameters.AddWithValue("@CompanyCode", companyCode);
        cmd.Parameters.AddWithValue("@BranchCode", branchCode);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            bankList.Add(new BankConfigurationDetails
            {
                BankConfigurationId = reader.GetInt32(0),
                IsPrimary = reader.GetBoolean(1),
                IsFormanBank = reader.GetBoolean(2),
                IsForemanPaymentBank = reader.GetBoolean(3),
                IsInterestPaymentBank = reader.GetBoolean(4)
            });
        }

        return bankList;
    }


    #endregion BankConfigurationdetails
    #region ViewChequeManagementDetails
    public List<ViewChequeManagementDTO> ViewChequeManagementDetails(
    string connectionString,
    string branchSchema,
    string globalSchema,
    string companyCode,
    string branchCode,
    int pageSize,
    int pageNo)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is required");

        branchSchema = branchSchema.Trim();
        globalSchema = globalSchema.Trim();

        var chequeList = new List<ViewChequeManagementDTO>();

        using var conn = new Npgsql.NpgsqlConnection(connectionString);
        conn.Open();

        string query = $@"
        SELECT 
            t2.tbl_mst_bank_configuration_id,
            t1.cheque_book_id,
            t1.noofcheques,
            t1.cheque_from_number,
            t1.cheque_to_number,
            t1.cheque_generate_status,
            t3.bank_name,
            t2.account_number,
            t1.status,
            COALESCE(
                (
                    SELECT cheque_status
                    FROM {AddDoubleQuotes(branchSchema)}.tbl_mst_cheques
                    WHERE cheque_book_id = t1.cheque_book_id
                      AND status = TRUE
                    LIMIT 1
                ), ''
            ) AS cheque_status
        FROM {AddDoubleQuotes(branchSchema)}.tbl_mst_cheque_management t1
        JOIN {AddDoubleQuotes(branchSchema)}.tbl_mst_bank_configuration t2
            ON t1.bank_configuration_id = t2.tbl_mst_bank_configuration_id
        JOIN {AddDoubleQuotes(globalSchema)}.tbl_mst_bank t3
            ON t2.bank_id = t3.tbl_mst_bank_id
        WHERE t2.status = TRUE
          AND t1.company_code = @CompanyCode
          AND t1.branch_code = @BranchCode
        ORDER BY t1.tbl_mst_cheque_management_id DESC
        LIMIT @PageSize OFFSET @Offset;
    ";

        using var cmd = new Npgsql.NpgsqlCommand(query, conn);
        cmd.Parameters.AddWithValue("@CompanyCode", companyCode);
        cmd.Parameters.AddWithValue("@BranchCode", branchCode);
        cmd.Parameters.AddWithValue("@PageSize", pageSize);
        cmd.Parameters.AddWithValue("@Offset", pageNo * pageSize);

        using var dr = cmd.ExecuteReader();
        while (dr.Read())
        {
            chequeList.Add(new ViewChequeManagementDTO
            {
                BankConfigurationId = dr.GetInt32(0),
                ChequeBookId = dr.GetInt32(1),
                NoOfCheques = dr.GetInt32(2),
                ChequeFromNumber = dr.GetInt32(3),
                ChequeToNumber = dr.GetInt32(4),
                ChequeGenerateStatus = dr.GetBoolean(5),
                BankName = dr.GetString(6),
                AccountNumber = dr.GetString(7),
                Status = dr.GetBoolean(8),
                ChequeStatus = dr.GetString(9)
            });
        }

        return chequeList;
    }

   
    private string AddDoubleQuotes(string name)
    {
        return $"\"{name.Trim()}\"";
    }

    #endregion ViewChequeManagementDetails

    #region  ExistingChequeCount
    public List<ExistingChequeCountDTO> GetExistingChequeCount(
    string connectionString,
    int bankId,
    int chqFromNo,
    int chqToNo,
    string branchSchema,
    string companyCode,
    string branchCode)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is required");

        if (string.IsNullOrWhiteSpace(branchSchema) || string.IsNullOrWhiteSpace(companyCode) || string.IsNullOrWhiteSpace(branchCode))
            return new List<ExistingChequeCountDTO>();

        var result = new List<ExistingChequeCountDTO>();

        using var con = new NpgsqlConnection(connectionString);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandType = CommandType.Text;

        cmd.CommandText = $@"
        SELECT COUNT(*) AS count
        FROM {AddDoubleQuotes(branchSchema)}.tbl_mst_cheque_management
        WHERE bank_configuration_id = @BankId
          AND (
                cheque_from_number BETWEEN @ChqFromNo AND @ChqToNo
                OR cheque_to_number BETWEEN @ChqFromNo AND @ChqToNo
              )
          AND company_code = @CompanyCode
          AND branch_code = @BranchCode;
    ";

        cmd.Parameters.AddWithValue("@BankId", bankId);
        cmd.Parameters.AddWithValue("@ChqFromNo", chqFromNo);
        cmd.Parameters.AddWithValue("@ChqToNo", chqToNo);
        cmd.Parameters.AddWithValue("@CompanyCode", companyCode);
        cmd.Parameters.AddWithValue("@BranchCode", branchCode);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            result.Add(new ExistingChequeCountDTO
            {
                Count = reader.GetInt32(0)
            });
        } 

        return result;
    }

    #endregion ExistingChequeCount



}
