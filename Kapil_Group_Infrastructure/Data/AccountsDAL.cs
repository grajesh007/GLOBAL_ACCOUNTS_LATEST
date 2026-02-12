using System;
using System.Collections.Generic;
using System.Data;
using Npgsql;
using Kapil_Group_Repository.Interfaces;
using Kapil_Group_Repository.Entities;
using System.Globalization;
namespace Kapil_Group_Infrastructure.Data
{ 
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
SELECT b.tbl_mst_branch_configuration_id, c.company_name, c.company_code, b.branch_code, b.unique_branch_name,
       COALESCE(b.gst_number, b.state_code::text) AS gst_number, c.cin_number, c.registration_address, b.branch_name,
       b.state_code, b.branch_address, b.chit_register_address, COALESCE(b.backdate_enable_enddate >= CURRENT_DATE, FALSE) AS transaction_lock_status,
       COALESCE(c.application_version_no, '') AS application_version_no, CASE WHEN b.uncommenced_sjv_allow_status='Y' THEN TRUE ELSE FALSE END AS uncommenced_sjv_allow_status,
       CASE WHEN b.incharge_validate_on_subscriber='Y' THEN TRUE ELSE FALSE END AS incharge_validate_on_subscriber, COALESCE(b.display_name,'') AS display_name,
       CASE WHEN b.fin_closing_jv_allow_status='Y' THEN TRUE ELSE FALSE END AS fin_closing_jv_allow_status, COALESCE(b.legalcell_name,'') AS legalcell_name,
       c.receipt_restriction_no_of_dues AS legalcell_dues, CASE WHEN b.islegalgeneral_receipt_allow_status='Y' THEN TRUE ELSE FALSE END AS islegalgeneral_receipt_allow_status,
       c.chitfund_act_number, c.legal_dept_address, CASE WHEN b.isonlyefeereceipt_allow_status='Y' THEN TRUE ELSE FALSE END AS isonlyefeereceipt_allow_status,
       b.kgms_start_time, b.kgms_end_time, c.nonpayment_subscribers_graceperiod, c.receipt_restriction_no_of_dues_daywise AS legalcell_dues_days,
       c.max_chits_per_contact, CASE WHEN b.daywise_auctions='Y' THEN TRUE ELSE FALSE END AS daywise_auctions,
       CASE WHEN b.lc_calculatedpenalty_editable='Y' THEN TRUE ELSE FALSE END AS lc_calculatedpenalty_editable,
       b.onlineprocess_backdate_days, CASE WHEN b.iscontact_editable_allow_status='Y' THEN TRUE ELSE FALSE END AS iscontact_editable_allow_status,
       CASE WHEN b.fixed_chit_status='Y' THEN TRUE ELSE FALSE END AS fixed_chit_status,
       CASE WHEN b.is_auto_brs_imps_applicable='Y' THEN TRUE ELSE FALSE END AS is_auto_brs_imps_applicable,
       CASE WHEN b.issubscriber_nominee_allocation='Y' THEN TRUE ELSE FALSE END AS issubscriber_nominee_allocation,
       COALESCE(b.biometric_enable_enddate >= CURRENT_DATE, FALSE) AS biometric_date_lock_status
FROM {AddDoubleQuotes(globalSchema)}.tbl_mst_branch_configuration b
JOIN {AddDoubleQuotes(globalSchema)}.tbl_mst_chit_company_configuration c ON c.tbl_mst_chit_company_configuration_id = b.company_configuration_id
WHERE b.branch_code=@BranchCode AND c.company_code=@CompanyCode;
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

    public List<ChequeManagementDTO> ViewChequeManagementDetails(
        string con,
        string BranchSchema,
        string GlobalSchema,
        string CompanyCode,
        string BranchCode,
        int PageSize,
        int PageNo)
    {
        List<ChequeManagementDTO> lst = new List<ChequeManagementDTO>();

        try
        {
            using (var conn = new Npgsql.NpgsqlConnection(con))
            {
                conn.Open();

                int totalRecords = 0;


                string countQuery = "SELECT COUNT(*) AS total_records FROM "
                    + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheque_management t1 JOIN "
                    + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t2 ON t1.bank_configuration_id = t2.tbl_mst_bank_configuration_id "
                    + "JOIN " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank t3 ON t2.bank_id = t3.tbl_mst_bank_id "
                    + "WHERE t2.status = TRUE AND t1.company_code = '" + CompanyCode + "' "
                    + "AND t1.branch_code  = '" + BranchCode + "';";

                using (var cmd = new Npgsql.NpgsqlCommand(countQuery, conn))
                {
                    totalRecords = Convert.ToInt32(cmd.ExecuteScalar());
                }


                string dataQuery = "select t2.tbl_mst_bank_configuration_id, t1.cheque_book_id,t1.noofcheques,"
                    + "t1.cheque_from_number,t1.cheque_to_number,t1.cheque_generate_status,"
                    + "bank_name,account_number,t1.status,"
                    + "COALESCE((select cheque_status from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheques "
                    + "where cheque_book_id=t1.cheque_book_id and status=true limit 1),'')cheque_status "
                    + "from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheque_management t1 "
                    + "join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t2 "
                    + "on t1.bank_configuration_id = t2.tbl_mst_bank_configuration_id "
                    + "join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank t3 "
                    + "on t2.bank_id = t3.tbl_mst_bank_id where t2.status=true "
                    + "and t1.company_code='" + CompanyCode + "' "
                    + "and t1.branch_code='" + BranchCode + "' "
                    + "order by tbl_mst_cheque_management_id desc "
                    + "LIMIT " + PageSize + " OFFSET " + PageNo + ";";

                using (var cmd = new Npgsql.NpgsqlCommand(dataQuery, conn))
                using (var dr = cmd.ExecuteReader())
                {
                    bool isFirstRow = true;

                    while (dr.Read())
                    {
                        ChequeManagementDTO dto = new ChequeManagementDTO
                        {
                            ptotalrecords = isFirstRow ? totalRecords : 0,
                            pbankconfigurationid = Convert.ToInt64(dr["tbl_mst_bank_configuration_id"]),
                            pchequebookid = Convert.ToInt64(dr["cheque_book_id"]),
                            pnoofcheques = Convert.ToInt32(dr["noofcheques"]),
                            pchequefromnumber = Convert.ToInt64(dr["cheque_from_number"]),
                            pchequetonumber = Convert.ToInt64(dr["cheque_to_number"]),
                            pchequegeneratestatus = Convert.ToBoolean(dr["cheque_generate_status"]),
                            pbankname = Convert.ToString(dr["bank_name"]),
                            paccountnumber = Convert.ToString(dr["account_number"]),
                            pstatus = Convert.ToBoolean(dr["status"]),
                            pchequestatus = Convert.ToString(dr["cheque_status"])
                        };

                        lst.Add(dto);
                        isFirstRow = false;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            throw;
        }

        return lst;
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

    #region BankUPIDetails......
    public List<BankUPIDetails> GetBankUPIDetails(string connectionString, string GlobalSchema, string CompanyCode, string BranchCode)
    {
        List<BankUPIDetails> upiList = new List<BankUPIDetails>();

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is null or empty", nameof(connectionString));

        try
        {
            NpgsqlConnectionStringBuilder builder;
            try
            {
                builder = new NpgsqlConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Invalid connection string format", nameof(connectionString), ex);
            }

            using (NpgsqlConnection con = new NpgsqlConnection(builder.ConnectionString))
            {
                con.Open();

                using var cmd = con.CreateCommand();
                cmd.CommandText =
                    "select tbl_mst_bank_upi_names_id as recordid, upi_name " +
                    "from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank_upi_names " +
                    "where status = true " +
                    "and company_code = '" + CompanyCode + "' " +
                    "and branch_code = '" + BranchCode + "';";

                cmd.CommandType = CommandType.Text;

                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    BankUPIDetails obj = new BankUPIDetails();

                    obj.recordid =
                        reader.IsDBNull(0) ? 0 : (int)reader.GetInt64(0);

                    obj.upiname =
                        reader.IsDBNull(1) ? string.Empty : reader.GetString(1);

                    upiList.Add(obj);
                }
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to retrieve UPI names (schema={GlobalSchema}). See inner exception for details.",
                ex);
        }

        return upiList;
    }

    #endregion BankUPIDetails...


    #region  ViewBankInformationDetails...
    public List<ViewBankInformationDetails> GetViewBankInformationDetails(string connectionString, string GlobalSchema, string BranchSchema, string BranchCode, string CompanyCode)
    {
        List<ViewBankInformationDetails> bankList = new List<ViewBankInformationDetails>();

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is null or empty", nameof(connectionString));

        try
        {
            NpgsqlConnectionStringBuilder builder;
            try
            {
                builder = new NpgsqlConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Invalid connection string format", nameof(connectionString), ex);
            }

            using (NpgsqlConnection con = new NpgsqlConnection(builder.ConnectionString))
            {
                con.Open();

                using var cmd = con.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText =
     "SELECT tbl_mst_bank_configuration_id, bank_id, COALESCE((SELECT bank_name FROM " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank " +
     "WHERE tbl_mst_bank_id = t1.bank_id), '') AS bank_name, account_number, account_name, status, is_debitcard_applicable, " +
     "is_upi_applicable, isprimary, isformanbank, is_foreman_payment_bank, is_interest_payment_bank " +
     "FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t1 WHERE t1.status = 'true' " +
     "AND t1.company_code = '" + CompanyCode + "' AND t1.branch_code = '" + BranchCode + "' ORDER BY tbl_mst_bank_configuration_id DESC;";


                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    ViewBankInformationDetails obj = new ViewBankInformationDetails();

                    obj.tbl_mst_bank_configuration_id =
                        reader.IsDBNull(0) ? 0 : (int)reader.GetInt64(0);

                    obj.bank_id =
                        reader.IsDBNull(1) ? 0 : (int)reader.GetInt64(1);

                    obj.BankName =
                        reader.IsDBNull(2) ? string.Empty : reader.GetString(2);

                    obj.account_number =
                        reader.IsDBNull(3) ? string.Empty : reader.GetString(3);

                    obj.account_name =
                        reader.IsDBNull(4) ? string.Empty : reader.GetString(4);

                    obj.status =
                        !reader.IsDBNull(5) && reader.GetBoolean(5);

                    obj.is_debitcard_applicable =
                        !reader.IsDBNull(6) && reader.GetBoolean(6);

                    obj.is_upi_applicable =
                        !reader.IsDBNull(7) && reader.GetBoolean(7);

                    obj.isprimary =
                        !reader.IsDBNull(8) && reader.GetBoolean(8);

                    obj.isformanbank =
                        !reader.IsDBNull(9) && reader.GetBoolean(9);

                    obj.is_foreman_payment_bank =
                        !reader.IsDBNull(10) && reader.GetBoolean(10);

                    obj.is_interest_payment_bank =
                        !reader.IsDBNull(11) && reader.GetBoolean(11);

                    bankList.Add(obj);
                }
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to retrieve bank details (schema={BranchSchema}). See inner exception for details.", ex);
        }

        return bankList;
    }

    #endregion ViewBankInformationDetails...


    #region  GeneralReceiptsData....
    public List<GeneralReceiptsData> GetGeneralReceiptsData(string connectionString, string GlobalSchema, string BranchSchema, string TaxSchema, string CompanyCode, string BranchCode)
    {
        List<GeneralReceiptsData> receiptList = new List<GeneralReceiptsData>();

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is null or empty", nameof(connectionString));

        try
        {
            NpgsqlConnectionStringBuilder builder;
            try
            {
                builder = new NpgsqlConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Invalid connection string format", nameof(connectionString), ex);
            }

            using (NpgsqlConnection con = new NpgsqlConnection(builder.ConnectionString))
            {
                con.Open();

                using var cmd = con.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText =
     "SELECT COALESCE(a.receipt_date::text,'') receipt_date, a.receipt_number, a.modeof_receipt, bank_name, reference_number, " +
     "COALESCE(a.total_received_amount,0) totalreceivedamount, narration, UPPER(contact_mailing_name) contactname, is_tds_applicable, section_name AS tdssection, " +
     "'' AS pannumber, tds_calculation_type, 0 AS tdspercentage, b.modeof_receipt AS typeofreceipt, a.file_name, " +
     "COALESCE(b.clear_date::text,'') clear_date, COALESCE(b.cheque_date::text,'') cheque_date, COALESCE(b.deposited_date::text,'') deposited_date " +
     "FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_generalreceipt a " +
     "LEFT JOIN " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference b ON a.receipt_number = b.receipt_number " +
     "LEFT JOIN " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration c ON b.deposited_bank_id = c.tbl_mst_bank_configuration_id " +
     "LEFT JOIN " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank f ON b.receipt_bank_id = f.tbl_mst_bank_id " +
     "LEFT JOIN " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact d ON a.contact_id = d.tbl_mst_contact_id " +
     "LEFT JOIN " + AddDoubleQuotes(TaxSchema) + ".tbl_mst_tds e ON a.tds_section_id = e.tbl_mst_tds_id " +
     "WHERE a.receipt_date::date = CURRENT_DATE AND a.company_code = '" + CompanyCode + "' AND a.branch_code = '" + BranchCode + "';";


                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    GeneralReceiptsData obj = new GeneralReceiptsData();

                    obj.receipt_date =
                        reader.IsDBNull(0) ? string.Empty : reader.GetString(0);

                    obj.receipt_number =
                        reader.IsDBNull(1) ? string.Empty : reader.GetString(1);

                    obj.modeof_receipt =
                        reader.IsDBNull(2) ? string.Empty : reader.GetString(2);

                    obj.bank_name =
                        reader.IsDBNull(3) ? string.Empty : reader.GetString(3);

                    obj.reference_number =
                        reader.IsDBNull(4) ? string.Empty : reader.GetString(4);

                    obj.totalreceivedamount =
                        reader.IsDBNull(5) ? 0 : (int)reader.GetInt64(5);

                    obj.narration =
                        reader.IsDBNull(6) ? string.Empty : reader.GetString(6);

                    obj.contactname =
                        reader.IsDBNull(7) ? string.Empty : reader.GetString(7);

                    obj.is_tds_applicable =
                        !reader.IsDBNull(8) && reader.GetBoolean(8);

                    obj.tdssection =
                        reader.IsDBNull(9) ? string.Empty : reader.GetString(9);

                    obj.pannumber =
                        reader.IsDBNull(10) ? string.Empty : reader.GetString(10);

                    obj.tds_calculation_type =
                        reader.IsDBNull(11) ? string.Empty : reader.GetString(11);

                    obj.tdspercentage =
                        reader.IsDBNull(12) ? 0 : (int)reader.GetInt64(12);

                    obj.typeofreceipt =
                        reader.IsDBNull(13) ? string.Empty : reader.GetString(13);

                    obj.file_name =
                        reader.IsDBNull(14) ? string.Empty : reader.GetString(14);

                    obj.clear_date =
                        reader.IsDBNull(15) ? string.Empty : reader.GetString(15);

                    obj.cheque_date =
                        reader.IsDBNull(16) ? string.Empty : reader.GetString(16);

                    obj.deposited_date =
                        reader.IsDBNull(17) ? string.Empty : reader.GetString(17);

                    receiptList.Add(obj);
                }
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to retrieve receipt details (schema={BranchSchema}). See inner exception for details.", ex);
        }

        return receiptList;
    }

    #endregion GeneralReceiptsData....

    #region  ViewBankInformation...

    public List<ViewBankInformation> GetViewBankInformation(string connectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode, int precordid)
    {
        List<ViewBankInformation> list = new List<ViewBankInformation>();

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is null or empty", nameof(connectionString));

        try
        {
            NpgsqlConnectionStringBuilder builder;
            try
            {
                builder = new NpgsqlConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Invalid connection string format", nameof(connectionString), ex);
            }

            using (NpgsqlConnection con = new NpgsqlConnection(builder.ConnectionString))
            {
                con.Open();

                using var cmd = con.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText =
    "SELECT tbl_mst_bank_configuration_id AS recordid, COALESCE(bank_date::text,'') AS bank_date, account_number, bank_name, account_name, bank_branch, ifsccode, " +
    "COALESCE(overdraft,0) AS overdraft, COALESCE(opening_balance,0) AS openingbalance, is_debitcard_applicable, is_upi_applicable, t.status AS statusname, 'OLD' AS typeofoperation, " +
    "account_type, opening_jvno, (SELECT account_trans_type FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher_details b " +
    "JOIN " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher a ON a.tbl_trans_journal_voucher_id = b.journal_voucher_id " +
    "WHERE journal_voucher_no = t.opening_jvno AND jv_account_id = t.bank_account_id) AS OpeningBalanceType " +
    "FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t JOIN " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank t1 ON t.bank_id = t1.tbl_mst_bank_id " +
    "WHERE tbl_mst_bank_configuration_id = '" + precordid + "' AND t.status = TRUE AND t.company_code = '" + CompanyCode + "' AND t.branch_code = '" + BranchCode + "';";


                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    ViewBankInformation obj = new ViewBankInformation();

                    obj.recordid = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
                    obj.bank_date = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
                    obj.account_number = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
                    obj.bank_name = reader.IsDBNull(3) ? string.Empty : reader.GetString(3);
                    obj.account_name = reader.IsDBNull(4) ? string.Empty : reader.GetString(4);
                    obj.bank_branch = reader.IsDBNull(5) ? string.Empty : reader.GetString(5);
                    obj.ifsccode = reader.IsDBNull(6) ? string.Empty : reader.GetString(6);
                    obj.overdraft = reader.IsDBNull(7) ? 0 : reader.GetInt32(7);
                    obj.openingbalance = reader.IsDBNull(8) ? 0 : reader.GetInt32(8);
                    obj.is_debitcard_applicable = reader.IsDBNull(9) ? false : reader.GetBoolean(9);
                    obj.is_upi_applicable = reader.IsDBNull(10) ? false : reader.GetBoolean(10);
                    obj.statusname = reader.IsDBNull(11) ? false : reader.GetBoolean(11);
                    obj.typeofoperation = reader.IsDBNull(12) ? string.Empty : reader.GetString(12);
                    obj.account_type = reader.IsDBNull(13) ? string.Empty : reader.GetString(13);
                    obj.opening_jvno = reader.IsDBNull(14) ? string.Empty : reader.GetString(14);
                    obj.OpeningBalanceType = reader.IsDBNull(15) ? string.Empty : reader.GetString(15);

                    list.Add(obj);
                }
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to retrieve bank configuration details (schema={BranchSchema}). See inner exception for details.",
                ex);
        }

        return list;
    }


    #endregion ViewBankInformation...

    #region AvailableChequeCount...

    public List<AvailableChequeCount> GetAvailableChequeCount(string connectionString, int bankId, int chqFromNo, int chqToNo, string branchSchema, string companyCode, string branchCode)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is required");

        if (string.IsNullOrWhiteSpace(branchSchema) ||
            string.IsNullOrWhiteSpace(companyCode) ||
            string.IsNullOrWhiteSpace(branchCode))
            return new List<AvailableChequeCount>();

        var result = new List<AvailableChequeCount>();

        using var con = new NpgsqlConnection(connectionString);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandType = CommandType.Text;

        cmd.CommandText = $@" SELECT COUNT(1) AS count FROM {AddDoubleQuotes(branchSchema)}.tbl_mst_cheques a WHERE a.bank_configuration_id = @BankId AND a.cheque_number BETWEEN @ChqFromNo AND @ChqToNo AND a.cheque_status = 'Un Used' AND a.company_code = @CompanyCode AND a.branch_code = @BranchCode;";

        cmd.Parameters.AddWithValue("@BankId", bankId);
        cmd.Parameters.AddWithValue("@ChqFromNo", chqFromNo);
        cmd.Parameters.AddWithValue("@ChqToNo", chqToNo);
        cmd.Parameters.AddWithValue("@CompanyCode", companyCode);
        cmd.Parameters.AddWithValue("@BranchCode", branchCode);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            result.Add(new AvailableChequeCount
            {
                Count = reader.IsDBNull(0) ? 0 : reader.GetInt32(0)
            });
        }

        return result;
    }


    #endregion AvailableChequeCount...

    #region PettyCashExistingData...


    public List<PettyCashExistingData> GetPettyCashExistingData(
        string connectionString,
        string GlobalSchema,
        string BranchSchema,
        string CompanyCode,
        string Branchcode)
    {
        List<PettyCashExistingData> paymentList = new List<PettyCashExistingData>();

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is null or empty", nameof(connectionString));

        try
        {
            NpgsqlConnectionStringBuilder builder;

            try
            {
                builder = new NpgsqlConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Invalid connection string format", nameof(connectionString), ex);
            }

            using (NpgsqlConnection con = new NpgsqlConnection(builder.ConnectionString))
            {
                con.Open();
                Console.WriteLine(con.State);

                using var cmd = con.CreateCommand();
                cmd.CommandText =
    "SELECT COALESCE(t1.payment_date::text,'') AS paymentdate, t1.payment_number AS paymentid, t1.modeof_payment, t2.modeof_payment AS typeofpayment, " +
    "bank_name, reference_number, total_paid_amount " +
    "FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_pettycash_voucher t1 " +
    "LEFT JOIN " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference t2 ON t1.payment_number = t2.payment_number " +
    "LEFT JOIN " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t3 ON t3.tbl_mst_bank_configuration_id = t2.bank_configuration_id " +
    "LEFT JOIN " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank t4 ON t4.tbl_mst_bank_id = t3.bank_id " +
    "WHERE t1.payment_date = CURRENT_DATE AND t1.company_code = '" + CompanyCode + "' AND t1.branch_code = '" + Branchcode + "' " +
    "ORDER BY t1.payment_date DESC;";

                cmd.CommandType = CommandType.Text;

                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    PettyCashExistingData obj = new PettyCashExistingData();

                    obj.PaymentDate =
                        reader.IsDBNull(0) ? string.Empty : reader.GetString(0);

                    obj.PaymentId =
                        reader.IsDBNull(1) ? string.Empty : reader.GetString(1);

                    obj.ModeOfPayment =
                        reader.IsDBNull(2) ? string.Empty : reader.GetString(2);

                    obj.TypeOfPayment =
                        reader.IsDBNull(3) ? string.Empty : reader.GetString(3);

                    obj.BankName =
                        reader.IsDBNull(4) ? string.Empty : reader.GetString(4);

                    obj.ReferenceNumber =
                        reader.IsDBNull(5) ? string.Empty : reader.GetString(5);

                    obj.TotalPaidAmount =
                        reader.IsDBNull(6) ? 0 : (int)reader.GetInt64(6);

                    paymentList.Add(obj);
                }
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to retrieve petty cash payment details (schema={BranchSchema}). See inner exception for details.",
                ex);
        }

        return paymentList;
    }

    #endregion PettyCashExistingData...

    #region PaymentVoucherExistingData..
    public List<PaymentVoucherDetails> GetPaymentVoucherExistingData(
        string connectionString,
        string? globalSchema,
        string? branchSchema,
        string? companyCode,
        string? branchCode)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is required");

        if (string.IsNullOrWhiteSpace(globalSchema) ||
            string.IsNullOrWhiteSpace(branchSchema) ||
            string.IsNullOrWhiteSpace(companyCode) ||
            string.IsNullOrWhiteSpace(branchCode))
            return new List<PaymentVoucherDetails>();

        var paymentList = new List<PaymentVoucherDetails>();

        using var con = new NpgsqlConnection(connectionString);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandType = CommandType.Text;

        cmd.CommandText = $@"
SELECT COALESCE(t1.payment_date::text,'') AS paymentdate, t1.payment_number AS paymentid,
       t1.modeof_payment, t2.modeof_payment AS typeofpayment, t4.bank_name, t2.reference_number, t1.total_paid_amount
FROM {AddDoubleQuotes(branchSchema)}.tbl_trans_payment_voucher t1
LEFT JOIN {AddDoubleQuotes(branchSchema)}.tbl_trans_payment_reference t2 ON t1.payment_number = t2.payment_number
LEFT JOIN {AddDoubleQuotes(branchSchema)}.tbl_mst_bank_configuration t3 ON t3.tbl_mst_bank_configuration_id = t2.bank_configuration_id
LEFT JOIN {AddDoubleQuotes(globalSchema)}.tbl_mst_bank t4 ON t4.tbl_mst_bank_id = t3.bank_id
WHERE t1.payment_date = CURRENT_DATE AND t1.company_code = @CompanyCode AND t1.branch_code = @BranchCode
ORDER BY t1.payment_date DESC;
";


        cmd.Parameters.AddWithValue("@CompanyCode", companyCode);
        cmd.Parameters.AddWithValue("@BranchCode", branchCode);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            paymentList.Add(new PaymentVoucherDetails
            {
                PaymentDate = reader.GetString(0),
                PaymentId = reader.GetString(1),
                ModeOfPayment = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                TypeOfPayment = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                BankName = reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
                ReferenceNumber = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                TotalPaidAmount = reader.IsDBNull(6) ? 0 : reader.GetDecimal(6)
            });
        }

        return paymentList;
    }

    #endregion PaymentVoucherExistingData..

    #region  ProductnamesandHSNcodes..
    public List<ProductNamesAndHSNCodesDetails> GetProductNamesAndHSNCodes(
    string connectionString,
    string? globalSchema)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is required");

        if (string.IsNullOrWhiteSpace(globalSchema))
            return new List<ProductNamesAndHSNCodesDetails>();

        var productList = new List<ProductNamesAndHSNCodesDetails>();

        using var con = new NpgsqlConnection(connectionString);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandType = CommandType.Text;

        cmd.CommandText = $"SELECT product_name, hsn_code FROM {AddDoubleQuotes(globalSchema)}.tbl_mst_hsncode WHERE status = true ORDER BY product_name;";


        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            productList.Add(new ProductNamesAndHSNCodesDetails
            {
                ProductName = reader.GetString(0),
                HSNCode = reader.GetString(1)
            });
        }

        return productList;
    }

    #endregion ProductnamesandHSNcodes..

    #region  getReceiptNumber..



    public List<getReceiptNumber> getReceiptNumber(
        string connectionString,
        string? globalSchema,
        string? branchSchema,
        string? companyCode,
        string? branchCode)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is required");

        if (string.IsNullOrWhiteSpace(globalSchema))
            return new List<getReceiptNumber>();

        var productList = new List<getReceiptNumber>();

        using var con = new NpgsqlConnection(connectionString);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandType = CommandType.Text;

        cmd.CommandText =
         $"select tbl_trans_pettycash_voucher_id,payment_number from " + AddDoubleQuotes(branchSchema) + ".tbl_trans_pettycash_voucher where case when TO_CHAR(current_date,'YYYY')::int= (select cal_year from " + AddDoubleQuotes(globalSchema) + ".tbl_mst_calendar_period where cal_year = to_char(current_date, 'YYYY')::int) and to_char(current_date,'MM') in('01', '02', '03') then payment_date between(select fin_from_Date from " + AddDoubleQuotes(globalSchema) + ".tbl_mst_calendar_period where cal_year = to_char(current_date - interval '1 year', 'YYYY')::int) and (select fin_to_Date from " + AddDoubleQuotes(globalSchema) + ".tbl_mst_calendar_period where cal_year = to_char(current_date - interval '1 year', 'YYYY')::int) else payment_date between(select fin_from_Date from " + AddDoubleQuotes(globalSchema) + ".tbl_mst_calendar_period where cal_year = to_char(current_date, 'YYYY')::int) and (select fin_to_Date from " + AddDoubleQuotes(globalSchema) + ".tbl_mst_calendar_period where cal_year = to_char(current_date, 'YYYY')::int) end and coalesce(receipt_cancel_reference_number,'')='' and company_code='" + companyCode + "' and branch_code='" + branchCode + "' order by tbl_trans_pettycash_voucher_id;";


        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            productList.Add(new getReceiptNumber
            {


                tbl_trans_pettycash_voucher_id = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                payment_number = reader.GetString(1)

            });
        }

        return productList;
    }

    #endregion ProductnamesandHSNcodes..


    #region GetBankUPIList
    public List<BankUPIListDetails> GetBankUPIListDetails(
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
            return new List<BankUPIListDetails>();

        var upiList = new List<BankUPIListDetails>();

        using var con = new NpgsqlConnection(connectionString);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandType = CommandType.Text;

        cmd.CommandText = $@"
        SELECT 
            account_name,
            account_id,
            account_balance
        FROM {AddDoubleQuotes(branchSchema)}.tbl_mst_account
        WHERE parent_id IN (
            SELECT account_id 
            FROM {AddDoubleQuotes(branchSchema)}.tbl_mst_account 
            WHERE account_name = 'AMOUNT RECEIVABLE-PAYMENT GATEWAY' 
              AND chracc_type = '2'
        )
        AND status = true
        AND company_code = @CompanyCode
        AND branch_code = @BranchCode
        ORDER BY 1;";

        cmd.Parameters.AddWithValue("@CompanyCode", companyCode);
        cmd.Parameters.AddWithValue("@BranchCode", branchCode);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            upiList.Add(new BankUPIListDetails
            {
                AccountName = reader.GetString(0),
                AccountId = reader.GetInt32(1),
                AccountBalance = reader.GetDecimal(2)
            });
        }

        return upiList;
    }

    #endregion GetBankUPIList

    #region GetCAOBranchList
    //    public List<CAOBranchDetails> GetCAOBranchList(
    //     string connectionString,
    //     string? branchSchema,
    //     string? globalSchema,
    //     string? companyCode,
    //     string? branchCode)
    // {
    //     if (string.IsNullOrWhiteSpace(connectionString))
    //         throw new ArgumentException("Connection string is required");

    //     if (string.IsNullOrWhiteSpace(branchSchema) ||
    //         string.IsNullOrWhiteSpace(globalSchema) ||
    //         string.IsNullOrWhiteSpace(companyCode) ||
    //         string.IsNullOrWhiteSpace(branchCode))
    //         return new List<CAOBranchDetails>();

    //     var branchList = new List<CAOBranchDetails>();

    //     using var con = new NpgsqlConnection(connectionString);
    //     con.Open();

    //     using var cmd = con.CreateCommand();
    //     cmd.CommandType = CommandType.Text;

    //     cmd.CommandText = $@"
    //         SELECT DISTINCT 
    //             b.tbl_mst_branch_configuration_id,
    //             b.branch_code,
    //             b.branch_name
    //         FROM {AddDoubleQuotes(branchSchema)}.tbl_trans_interbranch_receipt a
    //         JOIN {AddDoubleQuotes(globalSchema)}.tbl_mst_branch_configuration b
    //             ON a.interbranch_id = b.tbl_mst_branch_configuration_id
    //         LEFT JOIN {AddDoubleQuotes(branchSchema)}.tbl_trans_generalreceipt c
    //             ON c.receipt_number = a.general_receipt_number
    //         WHERE a.modeof_receipt = 'C'
    //           AND a.deposited_status = 'N'
    //           AND a.receipt_cancel_reference_number IS NULL
    //           AND a.company_code = @CompanyCode
    //           AND b.branch_code = @BranchCode;
    //     ";

    //     cmd.Parameters.AddWithValue("@CompanyCode", companyCode);
    //     cmd.Parameters.AddWithValue("@BranchCode", branchCode);

    //     using var reader = cmd.ExecuteReader();
    //     while (reader.Read())
    //     {
    //         branchList.Add(new CAOBranchDetails
    //         {
    //             BranchConfigurationId = reader.GetInt32(0),
    //             BranchCode = reader.GetString(1),
    //             BranchName = reader.GetString(2)
    //         });
    //     }

    //     return branchList;
    // }

    public List<CAOBranchListDetails> GetCAOBranchListDetails(
        string connectionString,
        string? globalSchema,
        string? branchSchema,
        string? companyCode,
        string? branchCode)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is required");

        if (string.IsNullOrWhiteSpace(globalSchema) ||
            string.IsNullOrWhiteSpace(branchSchema) ||
            string.IsNullOrWhiteSpace(companyCode) ||
            string.IsNullOrWhiteSpace(branchCode))
            return new List<CAOBranchListDetails>();

        var branchList = new List<CAOBranchListDetails>();

        using var con = new NpgsqlConnection(connectionString);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandType = CommandType.Text;

        cmd.CommandText = $@"
        SELECT DISTINCT
            b.tbl_mst_branch_configuration_id,
            b.branch_code,
            b.branch_name
        FROM {AddDoubleQuotes(branchSchema)}.tbl_trans_interbranch_receipt a
        JOIN {AddDoubleQuotes(globalSchema)}.tbl_mst_branch_configuration b
            ON a.interbranch_id = b.tbl_mst_branch_configuration_id
        LEFT JOIN {AddDoubleQuotes(branchSchema)}.tbl_trans_generalreceipt c
            ON c.receipt_number = a.general_receipt_number
        WHERE a.modeof_receipt = 'C'
          AND a.deposited_status = 'N'
          AND c.receipt_cancel_reference_number IS NULL
          AND a.company_code = @CompanyCode
          AND b.branch_code = @BranchCode;
    ";

        cmd.Parameters.AddWithValue("@CompanyCode", companyCode);
        cmd.Parameters.AddWithValue("@BranchCode", branchCode);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            branchList.Add(new CAOBranchListDetails
            {
                BranchConfigurationId = reader.GetInt32(0),
                BranchCode = reader.GetString(1),
                BranchName = reader.GetString(2)
            });
        }

        return branchList;
    }

    #endregion GetCAOBranchList


    public static string FormatDate(object strFormateDate)
    {
        string strDate = Convert.ToString(strFormateDate);
        string Date = null;
        if (!string.IsNullOrEmpty(strDate))
        {
            strDate = strDate.Substring(0, 10);

            string[] dat = null;
            if (strDate != null)
            {
                if (strDate.Contains("/"))
                {
                    dat = strDate.Split('/');
                }
                else if (strDate.Contains("-"))
                {
                    dat = strDate.Split('-');
                }
                Date = dat[2] + "-" + dat[1] + "-" + dat[0];
            }
        }
        return Date;
    }


    //cpoe


    public List<AccountReportsDTO> GetBankBookDetails(
        string con,
        string fromDate,
        string toDate,
        long _pBankAccountId,
        string AccountsSchema,
        string GlobalSchema,
        string CompanyCode,
        string BranchCode)
    {
        List<AccountReportsDTO> lstcashbook = new List<AccountReportsDTO>();

        try
        {
            DateTime fromDateTime = DateTime.ParseExact(fromDate, "yyyy-MM-dd", CultureInfo.InvariantCulture);
            DateTime toDateTime = DateTime.ParseExact(toDate, "yyyy-MM-dd", CultureInfo.InvariantCulture);


            using (var conn = new Npgsql.NpgsqlConnection(con))
            {
                conn.Open();

                long BankId;
                string bankQuery = $"SELECT bank_account_id FROM {AddDoubleQuotes(AccountsSchema)}.tbl_mst_bank_configuration " +
                                   $"WHERE tbl_mst_bank_configuration_id={_pBankAccountId} AND company_code='{CompanyCode}' AND branch_code='{BranchCode}'";

                using (var cmd = new Npgsql.NpgsqlCommand(bankQuery, conn))
                {
                    BankId = Convert.ToInt64(cmd.ExecuteScalar());
                }

                string Query = "select row_number() over (order by transaction_date) as recordid,* from (select transaction_date::text,TRANSACTION_NO,PARTICULARS,narration,DEBITAMOUNT,abs(CREDITAMOUNT)CREDITAMOUNT,abs(balance)balance,case when balance>0 then 'Dr' else 'Cr' end as balancetype from (select *,sum(DEBITAMOUNT+CREDITAMOUNT) OVER(ORDER BY transaction_date,RECORDID)as BALANCE from(SELECT 0 AS RECORDID,CAST('" + FormatDate(fromDate) + "' AS DATE) AS transaction_date,'0' AS TRANSACTION_NO,'Opening Balance' AS PARTICULARS,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)>0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END AS  DEBITAMOUNT,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)<0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END  AS CREDITAMOUNT,'' narration  FROM " + AddDoubleQuotes(AccountsSchema) + ".tbl_trans_total_transactions WHERE transaction_date < '" + FormatDate(fromDate) + "'  AND  account_id=" + BankId + " UNION ALL SELECT tbl_trans_total_transactions_id as RECORDID, transaction_date,TRANSACTION_NO,PARTICULARS,COALESCE(DEBITAMOUNT,0.00) as  DEBITAMOUNT,-COALESCE(CREDITAMOUNT,0.00) as CREDITAMOUNT,narration FROM  " + AddDoubleQuotes(AccountsSchema) + ".tbl_trans_total_transactions WHERE transaction_date BETWEEN  '" + FormatDate(fromDate) + "' AND '" + FormatDate(toDate) + "' AND account_id=" + BankId + " AND ( debitamount<>0 or creditamount<>0) order by transaction_date,RECORDID) as D) x union all select transaction_date::text,TRANSACTION_NO,PARTICULARS,narration,DEBITAMOUNT,abs(CREDITAMOUNT) CREDITAMOUNT,abs(balance) balance,case when balance>0 then 'Dr' else 'Cr' end as balancetype from (SELECT 0 AS RECORDID,CAST('" + FormatDate(toDate) + "' AS DATE) AS transaction_date,'0' AS TRANSACTION_NO,'Closing Balance' AS PARTICULARS,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)>0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END AS  DEBITAMOUNT,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)<0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END  AS CREDITAMOUNT,'' narration, COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) AS balance  FROM " + AddDoubleQuotes(AccountsSchema) + ".tbl_trans_total_transactions WHERE transaction_date <='" + FormatDate(toDate) + "'  AND  account_id=" + BankId + " order by recordid,transaction_date ) x) x order by recordid;";

                using (var cmd = new Npgsql.NpgsqlCommand(Query, conn))
                using (var dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        AccountReportsDTO _ObjBank = new AccountReportsDTO
                        {
                            precordid = Convert.ToInt64(dr["recordid"]),
                            ptransactiondate = dr["transaction_date"].ToString(),
                            pcreditamount = Convert.ToDouble(dr["CREDITAMOUNT"]),
                            pdebitamount = Convert.ToDouble(dr["DEBITAMOUNT"]),
                            pdescription = Convert.ToString(dr["narration"]),
                            pparticulars = Convert.ToString(dr["PARTICULARS"]),
                            ptransactionno = Convert.ToString(dr["TRANSACTION_NO"]),
                            popeningbal = Convert.ToDouble(dr["balance"]),
                            pBalanceType = Convert.ToString(dr["balancetype"])
                        };
                        lstcashbook.Add(_ObjBank);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            throw;
        }

        return lstcashbook;
    }





    public int GetRePrintInterBranchGeneralReceiptCount(
        string Connectionstring,
        string ReceiptId,
        string BranchSchema,
        string CompanyCode,
        string BranchCode)
    {
        int totalcount;

        string query = "select count(*) from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_interbranch_receipt where general_receipt_number='" + ReceiptId + "' and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'";

        using (NpgsqlConnection conn = new NpgsqlConnection(Connectionstring))
        {
            conn.Open();
            using (NpgsqlCommand cmd = new NpgsqlCommand(query, conn))

            {
                totalcount = Convert.ToInt32(cmd.ExecuteScalar());
            }
        }

        return totalcount;
    }



    public List<GstDTo> getStatesbyPartyid(
        long ppartyid,
        string Connectionstring,
        int id,
        string GlobalSchema,
        string BranchSchema,
        string CompanyCode,
        string BranchCode)
    {
        List<GstDTo> statelist = new List<GstDTo>();
        string query = "";

        try
        {
            bool isSupplierApplicable = false;

            using (NpgsqlConnection conn = new NpgsqlConnection(Connectionstring))
            {
                conn.Open();

                string checkQuery =
                    "select count(*) from " + AddDoubleQuotes(GlobalSchema) +
                    ".tbl_mst_contact t1 where is_supplier_applicable = true " +
                    "and tbl_mst_contact_id=" + ppartyid +
                    " and company_code='" + CompanyCode +
                    "' and branch_code='" + BranchCode + "'";

                using (NpgsqlCommand cmd = new NpgsqlCommand(checkQuery, conn))
                {
                    isSupplierApplicable = Convert.ToInt32(cmd.ExecuteScalar()) > 0;
                }
            }

            if (isSupplierApplicable)
            {
                query =
                    "select state_id,state,case when state_code<>branchstatcode and sgsttype='SGST' " +
                    "then 'IGST' else 'CGST,'||sgsttype end as gsttype,gst_number from " +
                    "(select t1.state_id,t1.state,t1.state_code," +
                    "case when union_territory=false then 'SGST' else 'UTGST' end as sgsttype," +
                    "t2.state_code as branchstatcode,coalesce(t1.gst_number,'')gst_number " +
                    "from (select distinct a.status, d.tbl_mst_state_id as state_id," +
                    "d.state_name as state,state_code,union_territory,document_reference_no as gst_number " +
                    "from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact a " +
                    "join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact_address_details b " +
                    "on a.tbl_mst_contact_id=b.contact_id " +
                    "join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_district c " +
                    "on b.district_id=c.tbl_mst_district_id " +
                    "join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_state d " +
                    "on c.state_id=d.tbl_mst_state_id " +
                    "left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact_documents f " +
                    "on f.contact_id=tbl_mst_contact_id and document_proofs_id IN " +
                    "(select tbl_mst_document_proofs_id from " +
                    AddDoubleQuotes(GlobalSchema) +
                    ".tbl_mst_document_proofs where upper(document_name)='GST NUMBER') " +
                    "where tbl_mst_contact_id=" + ppartyid +
                    " and isprimary = true) t1," +
                    AddDoubleQuotes(GlobalSchema) +
                    ".tbl_mst_branch_configuration t2 where t1.status = true " +
                    "and branch_code='" + BranchCode + "')x order by state;";
            }
            else
            {
                query =
                    "select state_id,state,case when state_code<>branchstatcode and sgsttype='SGST' " +
                    "then 'IGST' else 'CGST,'||sgsttype end as gsttype,'' gst_number from " +
                    "(select t1.tbl_mst_state_id as state_id,t1.state_name as state," +
                    "t1.state_code,case when union_territory=false then 'SGST' else 'UTGST' end as sgsttype," +
                    "t2.state_code as branchstatcode from " +
                    AddDoubleQuotes(GlobalSchema) +
                    ".tbl_mst_state t1," +
                    AddDoubleQuotes(GlobalSchema) +
                    ".tbl_mst_branch_configuration t2 where t1.status = true " +
                    "and branch_code='" + BranchCode + "')x order by state;";
            }

            using (NpgsqlConnection conn = new NpgsqlConnection(Connectionstring))
            {
                conn.Open();

                using (NpgsqlCommand cmd = new NpgsqlCommand(query, conn))
                using (NpgsqlDataReader dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        GstDTo obGstDTo = new GstDTo();
                        obGstDTo.pState = dr["state"].ToString();
                        obGstDTo.pStateId = Convert.ToInt32(dr["state_id"]);
                        obGstDTo.pgsttype = Convert.ToString(dr["gsttype"]);
                        obGstDTo.gstnumber = dr["gst_number"];
                        statelist.Add(obGstDTo);
                    }
                }
            }
        }
        catch
        {
            throw;
        }

        return statelist;
    }


    public int checkAccountnameDuplicates(
        string Accountname,
        string AccountType,
        int Parentid,
        string GlobalSchema,
        string connectionstring,
        string CompanyCode,
        string BranchCode)
    {
        int count = 0;

        try
        {
            if (!string.IsNullOrEmpty(Accountname))
            {
                string query = string.Empty;

                if (AccountType != "3")
                {
                    query = "select count(*) from " + AddDoubleQuotes(GlobalSchema) +
                            ".tbl_mst_account where upper(account_name)='" + Accountname.ToUpper() +
                            "' and company_code='" + CompanyCode +
                            "' and branch_code='" + BranchCode + "'";
                }

                if (AccountType == "3")
                {
                    query = "select count(*) from " + AddDoubleQuotes(GlobalSchema) +
                            ".tbl_mst_account where upper(account_name)='" + Accountname.ToUpper() +
                            "' and parent_id=" + Parentid +
                            " and company_code='" + CompanyCode +
                            "' and branch_code='" + BranchCode + "'";
                }

                using (var con = new NpgsqlConnection(connectionstring))
                {
                    con.Open();

                    using (var cmd = new NpgsqlCommand(query, con))
                    {
                        count = Convert.ToInt32(cmd.ExecuteScalar());
                    }
                }
            }
        }
        catch (Exception)
        {
            throw;
        }

        return count;
    }

    public decimal GetCashRestrictAmountpercontact(
        string type,
        string branchtype,
        string con,
        string GlobalSchema,
        string BranchSchema,
        long contactid,
        string checkdate,
        string CompanyCode,
        string BranchCode)
    {
        decimal result = 0;
        string branch_type = string.Empty;

        try
        {
            using (var connection = new Npgsql.NpgsqlConnection(con))
            {
                connection.Open();

                string branchQuery = "select branch_type from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where branch_code='" + BranchCode + "'";

                using (var cmd = new Npgsql.NpgsqlCommand(branchQuery, connection))
                {
                    var scalar = cmd.ExecuteScalar();
                    branch_type = scalar?.ToString();
                }

                if (type == "PAYMENT VOUCHER" && branch_type == "KGMS")
                {
                    string query = "select coalesce(sum(b.ledger_amount),0) as amt from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher a," + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher_details b where a.tbl_trans_payment_voucher_id = b.payment_voucher_id and a.modeof_payment = 'C' and a.payment_date = '" + FormatDate(checkdate) + "' and contact_id = " + contactid + " and a.company_code='" + CompanyCode + "' and a.branch_code='" + BranchCode + "'";

                    using (var cmd = new Npgsql.NpgsqlCommand(query, connection))
                    {
                        var scalar = cmd.ExecuteScalar();
                        result = scalar != null ? Convert.ToDecimal(scalar) : 0;
                    }
                }
                else if (type == "GENERAL RECEIPT" && branch_type == "KGMS")
                {
                    string query = "select coalesce(sum(total_received_Amount),0) from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_generalreceipt a where Receipt_date = '" + FormatDate(checkdate) + "' and modeof_receipt = 'C' and contact_id = " + contactid + " and receipt_cancel_reference_number is null and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'";

                    using (var cmd = new Npgsql.NpgsqlCommand(query, connection))
                    {
                        var scalar = cmd.ExecuteScalar();
                        result = scalar != null ? Convert.ToDecimal(scalar) : 0;
                    }
                }
                else
                {
                    result = 0;
                }
            }
        }
        catch (Exception)
        {
            throw;
        }

        return result;
    }


public List<AccountsDTO> GetGstLedgerAccountList(
    string ConnectionString,
    string formname,
    string BranchSchema,
    string CompanyCode,
    string BranchCode)
{
    List<AccountsDTO> accountslist = new List<AccountsDTO>();
    string query = string.Empty;

    try
    {
        if (formname == "GST REPORT")
        {
            query = "select t1.account_id,t1.account_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 where chracc_type='2' and t1.status=true and is_gst_applicable=true and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' order by account_name";
        }

        using (var connection = new Npgsql.NpgsqlConnection(ConnectionString))
        {
            connection.Open();

            using (var cmd = new Npgsql.NpgsqlCommand(query, connection))
            {
                using (var dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        AccountsDTO _AccountsDTO = new AccountsDTO();       
                        _AccountsDTO.pledgerid = Convert.ToInt64(dr["account_id"]);
                        _AccountsDTO.pledgername = Convert.ToString(dr["account_name"]);

                        accountslist.Add(_AccountsDTO);
                    }
                }
            }
        }
    }
    catch (Exception)
    {
        throw;
    }

    return accountslist;
}

public List<AccountsDTO> GetLedgerAccountList(
    string ConnectionString,
    string formname,
    string GlobalSchema,
    string BranchSchema,
    string CompanyCode,
    string BranchCode)
{
    List<AccountsDTO> accountslist = new List<AccountsDTO>();
    string query = string.Empty;

    try
    {
        if (formname == "DISBURSEMENT")
                    {
                        query = "";
                       // select t1.accountid,t1.accountname,sum( coalesce(debitamount,0)-coalesce(creditamount,0)) as balance," +" (case when acctype='L' then 'EQUITY AND LIABILITIES' when acctype='A' then 'ASSETS' when acctype='I' then 'INCOME' when acctype='E' then 'EXPENSES' end)acctype from tblmstaccounts t1 left join tbltranstotaltransactions t2 on t1.accountid=t2.parentid  where chracctype ='2' and t1.accountid not in (select accountid from tblmstuntransactionaccounts  where formname ='PAYMENT VOUCHER') and acctype  ='A' and t1.accountname='TRADE ADVANCE TO SHOWROOMS' and t1.statusid= true group by t1.accountid,t1.accountname,acctype order by accountname;

                    }
                    else if (formname == "PAYMENT VOUCHER")
                    {

                        query = "select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 WHERE t1.chracc_type = '2'  AND t1.status = TRUE and  t1.debit_restriction_status = false and COALESCE(t1.account_name,'')<>'' and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' ORDER BY t1.account_name;";
                       // select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 WHERE t1.chracc_type = '2'  AND t1.status = TRUE and  t1.debit_restriction_status = false and COALESCE(t1.account_name,'')<>'' ORDER BY t1.account_name;

                    }
                    else if (formname == "GENERAL RECEIPT")
                    {

                        query = "select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 WHERE t1.chracc_type = '2'  AND t1.status = TRUE and  t1.credit_restriction_status = false and COALESCE(t1.account_name,'')<>'' and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' ORDER BY t1.account_name;";
                        //select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 WHERE t1.chracc_type = '2'  AND t1.status = TRUE and  t1.credit_restriction_status = false and COALESCE(t1.account_name,'')<>'' ORDER BY t1.account_name;
                    }
                    else if (formname == "INITIALPAYMENT VOUCHER")
                    {
                        query = "select t1.account_id,t1.account_name,sum( coalesce(debitamount,0)-coalesce(creditamount,0)) as balance, (case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.account_id=t2.parent_id  where t1.account_name='ZONAL BID PAYABLE DEPARTMENT' and chracc_type ='2'  and t1.status= true and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' group by t1.account_id,t1.account_name,account_type order by account_name;";
                       // select t1.account_id,t1.account_name,sum( coalesce(debitamount,0)-coalesce(creditamount,0)) as balance, (case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.account_id=t2.parent_id  where t1.account_name='ZONAL BID PAYABLE DEPARTMENT' and chracc_type ='2'  and t1.status= true group by t1.account_id,t1.account_name,account_type order by account_name;
                    }
                    else if (formname == "JOURNAL VOUCHER")
                    {

                        query = "select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 WHERE t1.chracc_type = '2'  AND t1.status = TRUE and credit_restriction_status=false and t1.debit_restriction_status = FALSE and COALESCE(t1.account_name,'')<>'' and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'ORDER BY t1.account_name;";
                      // select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 WHERE t1.chracc_type = '2'  AND t1.status = TRUE and credit_restriction_status=false and t1.debit_restriction_status = FALSE and COALESCE(t1.account_name,'')<>'' ORDER BY t1.account_name;

                    }
                    else if (formname == "LEGAL EXPENSES JV")
                    {

                        long count = 0;

                        string _Query = "select count(1) from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where branch_code='" + BranchCode + "' and status=true and branch_type='KGMS'";
                       // select count(1) from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where branch_code='" + BranchSchema + "' and status=true and branch_type='KGMS'

                     using (var con = new Npgsql.NpgsqlConnection(ConnectionString))
{
    con.Open();

    using (var cmd = new Npgsql.NpgsqlCommand(_Query, con))
    {
        object result = cmd.ExecuteScalar();
        count = result != null ? Convert.ToInt64(result) : 0;
    }
}


                        if (count == 1)

                        {
                            query = "select (select account_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_name='CHIT SUBSCRIPTION' and chracc_type='1' and status=true and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + ") as account_id,branch_name as account_name,0 balance,'' acctype,branch_code,tbl_mst_branch_configuration_id as branch_id,'ONLINE' as branch_type from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where status=true and branch_type='CAO'";
                            //select (select account_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_name='CHIT SUBSCRIPTION' and chracc_type='1' and status=true) as account_id,branch_name as account_name,0 balance,'' acctype,branch_code,tbl_mst_branch_configuration_id as branch_id,'ONLINE' as branch_type from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where status=true and branch_type='CAO'
                        }
                        else
                        {
                            string OFFLINE = "OFFLINE";

                            query = "";
                          //  select coalesce((select tbl_mst_branch_configuration_id from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where branch_name=account_name and status=true and branch_type='CAO'),y.branch_id) as account_id,account_name,balance,acctype,y.branch_code,y.branch_id,case when (select count(1) from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where branch_code= y.branch_code)>0 then 'ONLINE' ELSE 'OFFLINE' END AS branch_type from (select t1.account_id,t1.account_name,sum( coalesce(debitamount,0)-coalesce(creditamount,0)) as balance, (case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.account_id=t2.parent_id  where chracc_type ='2'  and t1.status= true and credit_restriction_status=false AND debit_restriction_status=false and t1.account_id in(select account_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account a," + AddDoubleQuotes(OFFLINE) + ".tbl_mst_collectionappbranches b where a.account_name = b.branch_name and a.parent_id = (select account_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_name = 'INTER BRANCH BALANCES' and chracc_type = '1')) group by t1.account_id,t1.account_name,account_type order by account_name)x join " + AddDoubleQuotes(OFFLINE) + ".tbl_mst_collectionappbranches y on x.account_name = y.branch_name; 
                        }

                    }
                    else if (formname == "LEGAL EXPENSES ACCOUNTS")//no data
                    {
                        query = "select t1.account_id,t1.account_name,sum( coalesce(debitamount,0)-coalesce(creditamount,0)) as balance, (case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.account_id=t2.parent_id  where chracc_type ='2'  and t1.status= true and credit_restriction_status=false AND debit_restriction_status=false and t1.company_code='" + CompanyCode + "' and t1.branch_code='" + BranchCode + "' and t1.parent_id = (select account_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_name = 'OTHER EXPENSES' and chracc_type = '1') and exists (select 1 from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_account_legal_expenses where account_id=t1.account_id) group by t1.account_id,t1.account_name,account_type order by account_name;";
                       // select t1.account_id,t1.account_name,sum( coalesce(debitamount,0)-coalesce(creditamount,0)) as balance, (case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.account_id=t2.parent_id  where chracc_type ='2'  and t1.status= true and credit_restriction_status=false AND debit_restriction_status=false and t1.parent_id = (select account_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_name = 'OTHER EXPENSES' and chracc_type = '1') and exists (select 1 from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_account_legal_expenses where account_id=t1.account_id) group by t1.account_id,t1.account_name,account_type order by account_name;
                    }
                    else if (formname == "TDS JV")
                    {

                        query = "select distinct * from (select t1.account_id,t1.account_name,coalesce(t1.account_balance,0) as balance,(case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1  where chracc_type ='2' and t1.status= true and is_tds_applicable=true and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' UNION ALL select t1.account_id,t1.account_name,coalesce(t1.account_balance,0) as balance,(case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 where chracc_type ='2' and t1.status= true and  (account_name like 'T%D%S%19%P%E' or account_name like 'VERIFICATION AND PROCESSING CHARGES' or account_name like 'C%GST')) x order by account_name;";
                        //select distinct * from (select t1.account_id,t1.account_name,coalesce(t1.account_balance,0) as balance,(case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1  where chracc_type ='2' and t1.status= true and is_tds_applicable=true  UNION ALL select t1.account_id,t1.account_name,coalesce(t1.account_balance,0) as balance,(case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 where chracc_type ='2' and t1.status= true and  (account_name like 'T%D%S%19%P%E' or account_name like 'VERIFICATION AND PROCESSING CHARGES' or account_name like 'C%GST')) x order by account_name;

                    }
                    else if (formname == "GST REPORT")
                    {
                        query = "select t1.account_id,t1.account_name,sum( coalesce(debitamount,0)-coalesce(creditamount,0)) as balance,(case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.account_id=t2.parent_id  where chracc_type ='2' and t1.status= true and is_gst_applicable=true and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' group by t1.account_id,t1.account_name,account_type order by account_name;";
                        //select t1.account_id,t1.account_name,sum( coalesce(debitamount,0)-coalesce(creditamount,0)) as balance,(case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.account_id=t2.parent_id  where chracc_type ='2' and t1.status= true and is_gst_applicable=true group by t1.account_id,t1.account_name,account_type order by account_name;
                    }
                    else if (formname == "INTERBRANCH JV")
                    {

                        query = "select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance,(case when account_type = 'L' then 'EQUITY AND LIABILITIES' when account_type = 'A' then 'ASSETS' when account_type = 'I' then 'INCOME' when account_type = 'E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1  where chracc_type = '2' and t1.status = true and is_interbranch_jv_applicable = true  and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'order by t1.account_name,account_type;";
                       // select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance,(case when account_type = 'L' then 'EQUITY AND LIABILITIES' when account_type = 'A' then 'ASSETS' when account_type = 'I' then 'INCOME' when account_type = 'E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1  where chracc_type = '2' and t1.status = true and is_interbranch_jv_applicable = true order by t1.account_name,account_type;

                    }
                    else if (formname == "INTERBRANCH MV")
                    {

                        query = "select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 WHERE t1.chracc_type = '2'  AND t1.status = TRUE and  t1.debit_restriction_status = false and t1.is_interbranch_mv_applicable=true and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'ORDER BY t1.account_name;";
                       // select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 WHERE t1.chracc_type = '2'  AND t1.status = TRUE and  t1.debit_restriction_status = false and t1.is_interbranch_mv_applicable=true ORDER BY t1.account_name;
                    }

                    else if (formname == "PAROLL MV")
                    {
                        query = "select t1.account_id,t1.account_name,COALESCE(account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1  WHERE t1.chracc_type = '2'  AND t1.status = TRUE and  t1.debit_restriction_status = false and t1.is_payroll_mv_applicable=true and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'ORDER BY t1.account_name;";
                       // select t1.account_id,t1.account_name,COALESCE(account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1  WHERE t1.chracc_type = '2'  AND t1.status = TRUE and  t1.debit_restriction_status = false and t1.is_payroll_mv_applicable=true ORDER BY t1.account_name;
                    }
                    else
                    {
                        query = "select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1  WHERE t1.chracc_type = '2'  AND t1.status = TRUE and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'ORDER BY t1.account_name;";
                       // select t1.account_id,t1.account_name,COALESCE(t1.account_balance, 0) AS balance, CASE WHEN t1.account_type = 'L' THEN 'EQUITY AND LIABILITIES' WHEN t1.account_type = 'A' THEN 'ASSETS' WHEN t1.account_type = 'I' THEN 'INCOME' WHEN t1.account_type = 'E' THEN 'EXPENSES' END AS acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1  WHERE t1.chracc_type = '2'  AND t1.status = TRUE ORDER BY t1.account_name;
                    }

        using (var connection = new Npgsql.NpgsqlConnection(ConnectionString))
        {
            connection.Open();

            using (var cmd = new Npgsql.NpgsqlCommand(query, connection))
            {
                using (var dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        AccountsDTO obj = new AccountsDTO
                        {
                            pledgerid = Convert.ToInt64(dr["account_id"]),
                            pledgername = Convert.ToString(dr["account_name"]),
                            accountbalance = dr["balance"] != DBNull.Value ? Convert.ToDecimal(dr["balance"]) : 0,
                            id = Convert.ToInt64(dr["account_id"]),
                            text = Convert.ToString(dr["account_name"]),
                            pAccounttype = Convert.ToString(dr["acctype"])
                        };

                        if (formname == "LEGAL EXPENSES JV")
                        {
                            obj.pbranchcode = Convert.ToString(dr["branch_code"]);
                            obj.pbranchtype = Convert.ToString(dr["branch_type"]);
                        }

                        accountslist.Add(obj);
                    }
                }
            }
        }
    }
    catch (Exception)
    {
        throw;
    }

    return accountslist;
}




public List<AccountsDTO> GetLedgerSummaryAccountList(string ConnectionString, string formname, string GlobalSchema, string BranchSchema,string CompanyCode,
          string BranchCode)
{
    string query = string.Empty;
   List<AccountsDTO>  accountslist = new List<AccountsDTO>();

    try
    {
        if (formname == "DISBURSEMENT")
        {
            query = "";
        }
        else
        {
            query = "select t1.account_id,t1.account_name,sum(coalesce(account_balance,0)) as balance,(case when account_type='L' then 'EQUITY AND LIABILITIES' when account_type='A' then 'ASSETS' when account_type='I' then 'INCOME' when account_type='E' then 'EXPENSES' end)acctype from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 where chracc_type='2' and t1.status=true and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' group by t1.account_id,t1.account_name,account_type order by account_name;";
        }

        using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
        {
            con.Open();

            using (NpgsqlCommand cmd = new NpgsqlCommand(query, con))
            {
                using (NpgsqlDataReader dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        AccountsDTO _AccountsDTO = new AccountsDTO();
                        _AccountsDTO.pledgerid = Convert.ToInt64(dr["account_id"]);
                        _AccountsDTO.pledgername = Convert.ToString(dr["account_name"]);
                        _AccountsDTO.accountbalance = Convert.ToDecimal(dr["balance"]);
                        _AccountsDTO.id = Convert.ToInt64(dr["account_id"]);
                        _AccountsDTO.text = Convert.ToString(dr["account_name"]);
                        _AccountsDTO.pAccounttype = Convert.ToString(dr["acctype"]);

                        accountslist.Add(_AccountsDTO);
                    }
                }
            }
        }
    }
    catch (Exception ex)
    {
        throw ex;
    }

    return accountslist;
}



}
}
