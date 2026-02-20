using System;
using System.Collections.Generic;
using System.Data;
using Npgsql;
using System.Text;
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
                    cmd.CommandText = $"select tbl_mst_bank_configuration_id as bankaccountid,case when (account_number is null or coalesce(account_number,'')='') then account_name else account_name||' - '|| account_number end as bank_name from " + AddDoubleQuotes(globalSchema) + ".tbl_mst_bank t1 join " + AddDoubleQuotes(accountsSchema) + ".tbl_mst_bank_configuration  ts on t1.tbl_mst_bank_id =ts.bank_id  where t1.company_code='" + CompanyName + "' and t1.branch_code='" + BranchCode + "' order by bank_name;";
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
                            reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetInt64(0));


                        obj.BankName =
                            reader.IsDBNull(1) ? string.Empty : reader.GetString(1);

                        //obj.bankbranch =
                        //    reader.IsDBNull(2) ? string.Empty : reader.GetString(2);

                        //obj.ifsccode =
                        //    reader.IsDBNull(3) ? string.Empty : reader.GetString(3);

                        //obj.accounttype =
                        //    reader.IsDBNull(4) ? string.Empty : reader.GetString(4);

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
            if (strFormateDate == null)
                return null;

            DateTime parsedDate;

            if (DateTime.TryParseExact(
                    Convert.ToString(strFormateDate),
                    new string[] { "dd-MMM-yyyy", "dd-MM-yyyy", "dd/MM/yyyy" },
                    System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.None,
                    out parsedDate))
            {
                return parsedDate.ToString("yyyy-MM-dd");
            }

            return null;
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



                using (var conn = new Npgsql.NpgsqlConnection(con))
                {
                    conn.Open();

                    long BankId;
                    string bankQuery = "select bank_account_id from   " + AddDoubleQuotes(AccountsSchema) + ".tbl_mst_bank_configuration  where tbl_mst_bank_configuration_id=" + _pBankAccountId + " and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'";


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




        public List<AccountsDTO> GetLedgerSummaryAccountList(string ConnectionString, string formname, string GlobalSchema, string BranchSchema, string CompanyCode,
                  string BranchCode)
        {
            string query = string.Empty;
            List<AccountsDTO> accountslist = new List<AccountsDTO>();

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





        public List<subAccountLedgerDTO> GetSubAccountLedgerDetails(string con, string BranchSchema, string CompanyCode,
              string BranchCode)
        {
            List<subAccountLedgerDTO> lstSubAccountLedger = new List<subAccountLedgerDTO>();
            string strQuery = string.Empty;
            try
            {
                string Query = "SELECT distinct account_name FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account  WHERE chracc_type = '3' and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' ORDER BY account_name";

                using (var connection = new Npgsql.NpgsqlConnection(con))
                {
                    connection.Open();
                    using (var cmd = new Npgsql.NpgsqlCommand(Query, connection))
                    {
                        using (var dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                subAccountLedgerDTO _Obj = new subAccountLedgerDTO();
                                _Obj.paccountname = Convert.ToString(dr["account_name"]);

                                lstSubAccountLedger.Add(_Obj);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return lstSubAccountLedger;
        }



        public List<subAccountLedgerDTO> GetAccountLedgerData(
            string con,
            string SubLedgerName,
            string BranchSchema, string CompanyCode,
                  string BranchCode)
        {
            List<subAccountLedgerDTO> lstSubAccountLedger = new List<subAccountLedgerDTO>();

            try
            {
                string Query = "select t1.parent_id,(select account_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_id=t1.parent_id) as parentaccountname,account_id,account_name,account_balance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 where chracc_type = '3' and status = true and account_name = '" + SubLedgerName + "' and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' order by parentaccountname";
                //select t1.parent_id,(select account_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_id=t1.parent_id) as parentaccountname,account_id,account_name,account_balance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 where chracc_type = '3' and status = true and account_name = '" + SubLedgerName + "' order by parentaccountname

                using (NpgsqlConnection conn = new NpgsqlConnection(con))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            subAccountLedgerDTO _Obj = new subAccountLedgerDTO();
                            _Obj.pparentname = Convert.ToString(dr["parentaccountname"]);
                            _Obj.pparentid = Convert.ToInt64(dr["parent_id"]);

                            lstSubAccountLedger.Add(_Obj);
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }

            return lstSubAccountLedger;
        }




        public List<subAccountLedgerDTO> GetSubLedgerReportData(string con, string SubLedgerName, long parentid, string fromDate, string toDate, string BranchSchema, string CompanyCode,
                  string BranchCode)
        {
            List<subAccountLedgerDTO> lstSubAccountLedger = new List<subAccountLedgerDTO>();
            string strSubQuery = string.Empty;

            try
            {
                if (!string.IsNullOrEmpty(fromDate) && !string.IsNullOrEmpty(toDate))
                {
                    if (parentid > 0)
                    {
                        strSubQuery = "  AND  t1.accountname ='" + SubLedgerName + "' and t1.parent_id=" + parentid;
                    }
                    else
                    {
                        strSubQuery = "  AND  t1.accountname ='" + SubLedgerName + "'";
                    }

                    string Query = "select parentaccname,accountname,RECORDID,transaction_date,TRANSACTION_NO,PARTICULARS,DEBITAMOUNT, ABS(CREDITAMOUNT) as creditamount,narration,abs(balance)as balance,case when balance>0 then 'Dr' else 'Cr' end as balancetype from(select parentaccname,accountname,RECORDID,transaction_date,TRANSACTION_NO, PARTICULARS,DEBITAMOUNT, CREDITAMOUNT,narration,sum(DEBITAMOUNT+CREDITAMOUNT) OVER(PARTITION BY parentaccname ORDER BY parentaccname,transaction_date,RECORDID)as BALANCE from(SELECT t2.account_name as parentaccname,t1.accountname,0 AS RECORDID,CAST('" + FormatDate(fromDate) + "' AS DATE)-1 AS transaction_date,'0' AS TRANSACTION_NO,'Opening Balance' AS PARTICULARS,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)>0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END AS  DEBITAMOUNT,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)<0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END  AS CREDITAMOUNT,'' narration,COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) AS BALANCE  FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t2 on t1.parent_id =t2.account_id  WHERE transaction_date < '" + FormatDate(fromDate) + "' " + strSubQuery + " group by t2.account_name,t1.accountname UNION ALL SELECT t2.account_name,t1.accountname, t1.tbl_trans_total_transactions_id RECORDID, transaction_date,TRANSACTION_NO,PARTICULARS,COALESCE(DEBITAMOUNT,0.00) as  DEBITAMOUNT,-COALESCE(CREDITAMOUNT,0.00) as CREDITAMOUNT,narration ,0 BALANCE FROM  " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t1 join  " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t2 on t1.parent_id =t2.account_id WHERE  transaction_date BETWEEN  '" + FormatDate(fromDate) + "' AND '" + FormatDate(toDate) + "'" + strSubQuery + " and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' AND ( debitamount<>0 or creditamount<>0))as D order by parentaccname,accountname, transaction_date, RECORDID, TRANSACTION_NO)E";
                    // select parentaccname,accountname,RECORDID,transaction_date,TRANSACTION_NO,PARTICULARS,DEBITAMOUNT, ABS(CREDITAMOUNT) as creditamount,narration,abs(balance)as balance,case when balance>0 then 'Dr' else 'Cr' end as balancetype from(select parentaccname,accountname,RECORDID,transaction_date,TRANSACTION_NO, PARTICULARS,DEBITAMOUNT, CREDITAMOUNT,narration,sum(DEBITAMOUNT+CREDITAMOUNT) OVER(PARTITION BY parentaccname ORDER BY parentaccname,transaction_date,RECORDID)as BALANCE from(SELECT t2.account_name as parentaccname,t1.accountname,0 AS RECORDID,CAST('" + FormatDate(fromDate) + "' AS DATE)-1 AS transaction_date,'0' AS TRANSACTION_NO,'Opening Balance' AS PARTICULARS,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)>0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END AS  DEBITAMOUNT,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)<0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END  AS CREDITAMOUNT,'' narration,COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) AS BALANCE  FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t2 on t1.parent_id =t2.account_id  WHERE transaction_date < '" + FormatDate(fromDate) + "' " + strSubQuery + " group by t2.account_name,t1.accountname UNION ALL SELECT t2.account_name,t1.accountname, t1.tbl_trans_total_transactions_id RECORDID, transaction_date,TRANSACTION_NO,PARTICULARS,COALESCE(DEBITAMOUNT,0.00) as  DEBITAMOUNT,-COALESCE(CREDITAMOUNT,0.00) as CREDITAMOUNT,narration ,0 BALANCE FROM  " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t1 join  " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t2 on t1.parent_id =t2.account_id WHERE  transaction_date BETWEEN  '" + FormatDate(fromDate) + "' AND '" + FormatDate(toDate) + "'" + strSubQuery + "  AND ( debitamount<>0 or creditamount<>0))as D  order by parentaccname,accountname, transaction_date, RECORDID, TRANSACTION_NO)E;

                    using (NpgsqlConnection conn = new NpgsqlConnection(con))
                    {
                        conn.Open();

                        using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                        {
                            using (NpgsqlDataReader dr = cmd.ExecuteReader())
                            {
                                while (dr.Read())
                                {
                                    subAccountLedgerDTO _Obj = new subAccountLedgerDTO();
                                    _Obj.pparentname = Convert.ToString(dr["parentaccname"]);
                                    _Obj.paccountname = Convert.ToString(dr["accountname"]);
                                    _Obj.ptransactiondate = Convert.ToString(dr["transaction_date"]);
                                    _Obj.ptransactionno = Convert.ToString(dr["TRANSACTION_NO"]);
                                    _Obj.pparticulars = Convert.ToString(dr["PARTICULARS"]);
                                    _Obj.pdescription = Convert.ToString(dr["narration"]);
                                    _Obj.pdebitamount = Convert.ToDouble(dr["DEBITAMOUNT"]);
                                    _Obj.pcreditamount = Convert.ToDouble(dr["creditamount"]);
                                    _Obj.pclosingbal = Convert.ToDouble(dr["balance"]);
                                    _Obj.pBalanceType = Convert.ToString(dr["balancetype"]);

                                    lstSubAccountLedger.Add(_Obj);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }

            return lstSubAccountLedger;
        }





        public List<AccountsDTO> GetSubLedgerAccountList(long ledgerid, string ConnectionString, string GlobalSchema, string BranchSchema, string LocalSchema, string CompanyCode,
                  string BranchCode)
        {
            List<AccountsDTO> accountslist = new List<AccountsDTO>();
            string _query = string.Empty;

            try
            {
                int checkaccount = 0;

                using (NpgsqlConnection conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();

                    using (NpgsqlCommand cmdCheck = new NpgsqlCommand("select account_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_name = 'DEFAULT SUBSCRIBERS' and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "';", conn))
                    //select account_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_name = 'DEFAULT SUBSCRIBERS'
                    {
                        object result = cmdCheck.ExecuteScalar();
                        if (result != null)
                            checkaccount = Convert.ToInt32(result);
                    }

                    if (checkaccount == ledgerid)
                    {
                        string Query = "select distinct tr.payment_number,tr.payment_date::text,narration,coalesce(contact_id,0) contact_id,coalesce((select account_name from " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account where account_id=t1.parent_id)||'-'||(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=contact_id)) contactname,(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=contact_id) contact_mailing_name,tr.modeof_payment,reference_number,trr.trans_type,(select contact_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=employee_id) employeename from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_pettycash_voucher tr join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_pettycash_voucher_details trd on tr.tbl_trans_pettycash_voucher_id=trd.payment_voucher_id left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_reference trr on trr.payment_number=tr.payment_number join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account t1 on trd.debit_account_id=t1.account_id where tr.payment_number='" + ledgerid + "' and t1.company_code='" + CompanyCode + "' and t1.branch_code='" + BranchCode + "';";

                        /*query = "select account_id,account_name||'('||branch_name||')' as account_name,balance from (select t1.account_id,t1.account_name,t1.parent_id,sum(coalesce(account_balance, 0)) as balance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 where t1.parent_id = " + ledgerid + " and t1.chracc_type = '3' and t1.status = true and t1.company_code='" + CompanyCode + "' and t1.branch_code='" + BranchCode + "' group by t1.account_id,t1.account_name,t1.parent_id order by t1.account_name) x left join(select distinct groupcode || '-' || ticketno || '(' || upper(subscriber_name) || ')' as subname, branch_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_removed_subscriber a join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_chitgroup b on a.chitgroup_id = b.tbl_mst_chitgroup_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration c on a.subscriber_joined_branch_id = c.tbl_mst_branch_configuration_id union all select distinct 'BIJAY'||groupcode || '-' || ticketno || '(' || upper(subscriber_name) || ')' as subname, branch_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_removed_subscriber a join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_chitgroup b on a.chitgroup_id = b.tbl_mst_chitgroup_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration c on a.subscriber_joined_branch_id = c.tbl_mst_branch_configuration_id and exists(select 1 from " + AddDoubleQuotes(GlobalSchema) + ".tbl_trans_nps_security_lien where nps_groupcode = b.groupcode and nps_ticketno = a.ticketno and lien_contact_id=a.contact_id and release_status is null)) y on replace(replace(x.account_name,'LIEN-BIJAY-',''),'LIEN-','')= y.subname";*/
                        //select account_id,account_name||'('||branch_name||')' as account_name,balance from (select t1.account_id,t1.account_name,t1.parent_id,sum(coalesce(account_balance,0)) as balance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 where t1.parent_id=" + ledgerid + " and t1.chracc_type='3' and t1.status=true group by t1.account_id,t1.account_name,t1.parent_id order by t1.account_name) x left join(select distinct groupcode||'-'||ticketno||'('||upper(subscriber_name)||')' as subname,branch_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_removed_subscriber a join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_chitgroup b on a.chitgroup_id=b.tbl_mst_chitgroup_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration c on a.subscriber_joined_branch_id=c.tbl_mst_branch_configuration_id union all select distinct 'BIJAY'||groupcode||'-'||ticketno||'('||upper(subscriber_name)||')' as subname,branch_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_removed_subscriber a join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_chitgroup b on a.chitgroup_id=b.tbl_mst_chitgroup_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration c on a.subscriber_joined_branch_id=c.tbl_mst_branch_configuration_id and exists(select 1 from " + AddDoubleQuotes(GlobalSchema) + ".tbl_trans_nps_security_lien where nps_groupcode=b.groupcode and nps_ticketno=a.ticketno and lien_contact_id=a.contact_id and release_status is null)) y on replace(replace(x.account_name,'LIEN-BIJAY-',''),'LIEN-','')=y.subname
                    }
                    else
                    {
                        _query = "select t1.account_id,t1.account_name,sum(coalesce(account_balance, 0)) as balance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 where t1.parent_id =" + ledgerid + " and t1.chracc_type = '3' and t1.status = true and t1.company_code='" + CompanyCode + "' and t1.branch_code='" + BranchCode + "'group by t1.account_id,t1.account_name order by account_name;";
                        //select t1.account_id,t1.account_name,sum(coalesce(account_balance,0)) as balance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account t1 where t1.parent_id=" + ledgerid + " and t1.chracc_type='3' and t1.status=true group by t1.account_id,t1.account_name order by account_name;
                    }

                    using (NpgsqlCommand cmd = new NpgsqlCommand(_query, conn))
                    {
                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                AccountsDTO _AccountsDTO = new AccountsDTO();
                                _AccountsDTO.psubledgerid = Convert.ToInt64(dr["account_id"]);
                                _AccountsDTO.psubledgername = Convert.ToString(dr["account_name"]);
                                _AccountsDTO.accountbalance = Convert.ToDecimal(dr["balance"]);

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







        public List<AccountReportsDTO> GetTrialBalance(string con, string LocalSchema, string fromDate, string todate, string groupType, string CompanyCode,
                  string BranchCode, string GlobalSchema)
        {
            string Query = string.Empty;

            List<AccountReportsDTO> lstcashbook = new List<AccountReportsDTO>();

            try
            {
                Query = "select account_name,account_id,coalesce(groupcode,'')groupcode,case when parentname='E' then 'EXPENSES' when parentname='A' then 'ASSETS' when parentname='L' then 'EQUITY AND LIABILITIES' when parentname='I' then 'INCOME' end as parentname,DEBITAMOUNT,CREDITAMOUNT,case when parentname='E' then 2 when parentname='A' then 3 when parentname='L' then 4 when parentname='I' then 1 end as sortorder from (select account_name,account_id,groupcode,parentname,case when balamt>=0 then abs(balamt) else 0 end as debitamount, case when balamt<0 then abs(balamt) else 0 end as creditamount from (select parentname,parentaccountname as account_name,account_id,groupcode,coalesce(sum(DEBITAMOUNT),0) as  DEBITAMOUNT,-coalesce(sum(CREDITAMOUNT)) as CREDITAMOUNT,coalesce(sum(DEBITAMOUNT-CREDITAMOUNT),0) as balamt from (SELECT  case when accounttype='5' then split_part(parentaccountname,'-',1) else parentaccountname end as parentaccountname,case when accounttype='5' then 0 else parent_id end as account_id,case when accounttype='5' then split_part(parentaccountname,'-',1) end groupcode,(select account_type from " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account where account_id = t1.parent_id) as parentname, debitamount, creditamount FROM " + AddDoubleQuotes(LocalSchema) + " .tbl_trans_total_transactions t1 WHERE company_code = '" + CompanyCode + "' and branch_code = '" + BranchCode
                 + "' and case when upper('" + groupType + "')= 'ASON' then transaction_date<= '" + FormatDate(todate) + "' else transaction_date between '" + FormatDate(fromDate) + "' and '" + FormatDate(todate) + "' end)t group by parentname, parentaccountname, account_id, groupcode)t2)x where DEBITAMOUNT <> 0 or creditamount<>0 order by account_name";


                using (NpgsqlConnection conn = new NpgsqlConnection(con))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            AccountReportsDTO _ObjTrialBalance = new AccountReportsDTO();
                            _ObjTrialBalance.precordid = Convert.ToInt64(dr["sortorder"]);
                            _ObjTrialBalance.paccountid = Convert.ToInt64(dr["account_id"]);
                            _ObjTrialBalance.paccountname = Convert.ToString(dr["account_name"]);
                            _ObjTrialBalance.pdebitamount = Convert.ToDouble(dr["DEBITAMOUNT"]);
                            _ObjTrialBalance.pcreditamount = Convert.ToDouble(dr["CREDITAMOUNT"]);
                            _ObjTrialBalance.pparentname = Convert.ToString(dr["parentname"]);
                            _ObjTrialBalance.groupcode = dr["groupcode"];

                            lstcashbook.Add(_ObjTrialBalance);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return lstcashbook;
        }



        public List<IssuedChequeDTO> GetIssuedChequeNumbers(string con, long bankId, string BranchSchema, string CompanyCode,
                          string BranchCode)
        {
            List<IssuedChequeDTO> lstIssuedCheque = new List<IssuedChequeDTO>();

            try
            {
                string Query = "select distinct cheque_book_id,cheque_from_number,cheque_to_number,(cheque_from_number ||'-'|| cheque_to_number) as chqfromto from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheque_management tc join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration ts on tc.bank_configuration_id=ts.tbl_mst_bank_configuration_id where ts.tbl_mst_bank_configuration_id=" + bankId + " and ts.company_code='" + CompanyCode + "' and ts.branch_code='" + BranchCode + "' order by cheque_from_number;";

                //select distinct cheque_book_id,cheque_from_number,cheque_to_number,(cheque_from_number||'-'||cheque_to_number) as chqfromto from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheque_management tc join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration ts on tc.bank_configuration_id=ts.tbl_mst_bank_configuration_id where ts.tbl_mst_bank_configuration_id="+ bankId + " order by cheque_from_number;

                using (NpgsqlConnection conn = new NpgsqlConnection(con))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            IssuedChequeDTO _Obj = new IssuedChequeDTO();
                            _Obj.pchkBookId = Convert.ToInt64(dr["cheque_book_id"]);
                            _Obj.pchequeNoFrom = Convert.ToInt64(dr["cheque_from_number"]);
                            _Obj.pchequeNoTo = Convert.ToInt64(dr["cheque_to_number"]);
                            _Obj.pchqfromto = Convert.ToString(dr["chqfromto"]);

                            lstIssuedCheque.Add(_Obj);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }

            return lstIssuedCheque;
        }




        public List<subAccountLedgerDTO> GetMainAccounthead(string con, string BranchSchema, string CompanyCode,
                          string BranchCode)
        {
            List<subAccountLedgerDTO> subAccledger = new List<subAccountLedgerDTO>();

            try
            {
                string Query = "select account_name,account_id,account_type from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where parent_id is null and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' order by tbl_mst_account_id;";
                //select account_name,account_id,account_type from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where parent_id is null order by tbl_mst_account_id

                using (NpgsqlConnection conn = new NpgsqlConnection(con))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            subAccountLedgerDTO _Obj = new subAccountLedgerDTO();

                            _Obj.Acc_Id = Convert.ToString(dr["account_id"]);
                            _Obj.Account_name = Convert.ToString(dr["account_name"]);
                            _Obj.Account_type = Convert.ToString(dr["account_type"]);

                            subAccledger.Add(_Obj);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }

            return subAccledger;
        }



        public List<cashBookDto> getCashbookData(string ConnectionString, string fromdate, string todate, string BranchSchema, string CompanyCode,
                          string BranchCode)
        {
            string SreQry = string.Empty;
            List<cashBookDto> lstcashbook = new List<cashBookDto>();

            try
            {

                using (NpgsqlConnection conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();

                    string accountid = "select account_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where parent_id is null and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' order by tbl_mst_account_id;";
                    //113
                    //  ,account_name,account_type
                    //select string_agg(account_id::varchar,',') from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where case when '" + transType.ToUpper() + "'='CASH' THEN account_name='CASH ON HAND' WHEN '" + transType.ToUpper() + "'='CHEQUE' THEN account_name='CHEQUE ON HAND' ELSE account_name in ('CASH ON HAND','CHEQUE ON HAND') END and chracc_type='2'

                    using (NpgsqlCommand cmdAcc = new NpgsqlCommand(accountid, conn))
                    {
                        accountid = Convert.ToString(cmdAcc.ExecuteScalar());
                    }

                    if (!string.IsNullOrEmpty(accountid))
                    {
                        string qry = "select transaction_date::text,transaction_no,particulars,narration,debitamount,creditamount,ABS(bal) as balance,case when bal>=0 then 'Dr' else 'Cr' END AS balancetype from(select t.*,sum(debitamount-creditamount) over(order by transaction_date,recordid) as bal from (SELECT 0 as recordid, '" + FormatDate(fromdate) + "' as transaction_date,null TRANSACTION_NO,'OPENING BALANCE' particulars,''narration,case when  COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)>=0 then COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) else 0 end debitamount,case when  COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)<0 then ABS(COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)) else 0 end creditamount   FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions WHERE  transaction_date < '" + FormatDate(fromdate) + "' and PARENT_ID in (" + accountid + ") AND company_code = '" + CompanyCode + "' and branch_code = '" + BranchCode + "' union all SELECT tbl_trans_total_transactions_id as recordid,transaction_date::text,TRANSACTION_NO,PARTICULARS,narration,COALESCE(DEBITAMOUNT,0.00) AS DEBITAMOUNT,COALESCE(CREDITAMOUNT,0.00) AS CREDITAMOUNT FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions  WHERE PARENT_ID in (" + accountid + ") AND transaction_date BETWEEN '" + FormatDate(fromdate) + "' AND '" + FormatDate(todate) + "' order by transaction_date,recordid)t)t1 where (debitamount<>0 or creditamount<>0) ;";
                        //select transaction_date::text,transaction_no,particulars,narration,debitamount,creditamount,ABS(bal) as balance,case when bal>=0 then 'Dr' else 'Cr' END AS balancetype from(select t.*,sum(debitamount-creditamount) over(order by transaction_date,recordid) as bal from(select 0 as recordid,'" + FormatDate(fromdate) + "' as transaction_date,null transaction_no,'OPENING BALANCE' particulars,'' narration,case when COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)>=0 then COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) else 0 end debitamount,case when COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)<0 then ABS(COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)) else 0 end creditamount from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions where transaction_date < '" + FormatDate(fromdate) + "' and PARENT_ID in (" + accountid + ") union all select tbl_trans_total_transactions_id as recordid,transaction_date::text,transaction_no,particulars,narration,COALESCE(debitamount,0.00) AS debitamount,COALESCE(creditamount,0.00) AS creditamount from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions where PARENT_ID in (" + accountid + ") AND transaction_date between '" + FormatDate(fromdate) + "' AND '" + FormatDate(todate) + "' order by transaction_date,recordid)t)t1 where (debitamount<>0 or creditamount<>0);

                        using (NpgsqlCommand cmd = new NpgsqlCommand(qry, conn))
                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                cashBookDto _cashbookDTO = new cashBookDto();

                                _cashbookDTO.ptransactiondate = dr["transaction_date"];
                                _cashbookDTO.ptransactionno = Convert.ToString(dr["transaction_no"]);
                                _cashbookDTO.pdescription = Convert.ToString(dr["narration"]);
                                _cashbookDTO.pparticulars = Convert.ToString(dr["particulars"]);
                                _cashbookDTO.pcreditamount = Convert.ToDouble(dr["creditamount"]);
                                _cashbookDTO.pdebitamount = Convert.ToDouble(dr["debitamount"]);
                                _cashbookDTO.pclosingbal = Convert.ToDouble(dr["balance"]);
                                _cashbookDTO.pBalanceType = Convert.ToString(dr["balancetype"]);

                                lstcashbook.Add(_cashbookDTO);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return lstcashbook;
        }



        ////returncode
        public List<AccountReportsDTO> GetBalances(string con, string LocalSchema, string fromDate, string todate, string groupType, string formname, string CompanyCode,
                          string BranchCode)
        {
            string Query = string.Empty;
            string condition = string.Empty;

            List<AccountReportsDTO> lstcashbook = new List<AccountReportsDTO>();

            try
            {
                if (formname == "PL")
                {
                    Query = "select account_name,account_id,groupcode,parentname,case when parentname='INCOME' then creditamount-debitamount else debitamount-creditamount end as balance,debitamount,creditamount,sortorder from (select account_name,account_id,coalesce(groupcode,'') groupcode,case when parentname='E' then 'EXPENSES' when parentname='A' then 'ASSETS' when parentname='L' then 'EQUITY AND LIABILITIES' when parentname='I' then 'INCOME' end as parentname,DEBITAMOUNT,CREDITAMOUNT,case when parentname='E' then 2 when parentname='A' then 3 when parentname='L' then 4 when parentname='I' then 1 end as sortorder from (select account_name,account_id,groupcode,parentname,case when balamt>=0 then abs(balamt) else 0 end as debitamount,case when balamt<0 then abs(balamt) else 0 end as creditamount from (select parentname,parentaccountname as account_name,account_id,groupcode,coalesce(sum(DEBITAMOUNT),0) as DEBITAMOUNT,-coalesce(sum(CREDITAMOUNT),0) as CREDITAMOUNT,coalesce(sum(DEBITAMOUNT-CREDITAMOUNT),0) as balamt from (SELECT case when accounttype='5' then split_part(parentaccountname,'-',1) else parentaccountname end as parentaccountname,case when accounttype='5' then 0 else parent_id end as account_id,case when accounttype='5' then split_part(parentaccountname,'-',1) end groupcode,(select account_type from " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account where account_id=t1.parent_id) as parentname,debitamount,creditamount FROM " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_total_transactions t1 WHERE t1.company_code='" + CompanyCode + "' AND t1.branch_code='" + BranchCode + "' AND case when upper('" + groupType + "')='ASON' then transaction_date<='" + FormatDate(todate) + "' else transaction_date between '" + FormatDate(fromDate) + "' and '" + FormatDate(todate) + "' end)t group by parentname,parentaccountname,account_id,groupcode)t2)x where DEBITAMOUNT<>0 or creditamount<>0 order by account_name)x where parentname in ('INCOME','EXPENSES') order by sortorder";
                    // select account_name,account_id,groupcode,parentname,case when parentname='INCOME' then creditamount-debitamount else debitamount-creditamount end as balance,debitamount,creditamount,sortorder from (select account_name,account_id,coalesce(groupcode,'') groupcode,case when parentname='E' then 'EXPENSES' when parentname='A' then 'ASSETS' when parentname='L' then 'EQUITY AND LIABILITIES' when parentname='I' then 'INCOME' end as parentname,DEBITAMOUNT,CREDITAMOUNT,case when parentname='E' then 2 when parentname='A' then 3 when parentname='L' then 4 when parentname='I' then 1 end as sortorder from (select account_name,account_id,groupcode,parentname,case when balamt>=0 then abs(balamt) else 0 end as debitamount,case when balamt<0 then abs(balamt) else 0 end as creditamount from (select parentname,parentaccountname as account_name,account_id,groupcode,coalesce(sum(DEBITAMOUNT),0) as DEBITAMOUNT,-coalesce(sum(CREDITAMOUNT)) as CREDITAMOUNT,coalesce(sum(DEBITAMOUNT-CREDITAMOUNT),0) as balamt from (SELECT case when accounttype='5' then split_part(parentaccountname,'-',1) else parentaccountname end as parentaccountname,case when accounttype='5' then 0 else parent_id end as account_id,case when accounttype='5' then split_part(parentaccountname,'-',1) end groupcode,(select account_type from " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account where account_id=t1.parent_id) as parentname,debitamount,creditamount FROM " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_total_transactions t1 WHERE case when upper('" + groupType + "')='ASON' then transaction_date<='" + FormatDate(todate) + "' else transaction_date between '" + FormatDate(fromDate) + "' and '" + FormatDate(todate) + "' end)t group by parentname,parentaccountname,account_id,groupcode)t2)x where DEBITAMOUNT<>0 or creditamount<>0 order by account_name)x where parentname in ('INCOME','EXPENSES') order by sortorder
                }
                else
                {
                    Query = "select account_name,account_id,groupcode,parentname,case when parentname='ASSETS' then debitamount-creditamount else creditamount-debitamount end as balance,debitamount,creditamount,sortorder from (select account_name,account_id,coalesce(groupcode,'')groupcode,case when parentname='E' then 'EXPENSES' when parentname='A' then 'ASSETS' when parentname='L' then 'EQUITY AND LIABILITIES' when parentname='I' then 'INCOME' end as parentname,DEBITAMOUNT,CREDITAMOUNT,case when parentname='E' then 2 when parentname='A' then 3 when parentname='L' then 4 when parentname='I' then 1 end as sortorder from (select account_name,account_id,groupcode,parentname,case when balamt>=0 then abs(balamt) else 0 end as debitamount, case when balamt<0 then abs(balamt) else 0 end as creditamount from (select parentname,parentaccountname as account_name,account_id,groupcode,coalesce(sum(DEBITAMOUNT),0) as DEBITAMOUNT,-coalesce(sum(CREDITAMOUNT)) as CREDITAMOUNT,coalesce(sum(DEBITAMOUNT-CREDITAMOUNT),0) as balamt from (SELECT case when accounttype='5' then split_part(parentaccountname,'-',1) else parentaccountname end as parentaccountname, case when accounttype='5' then 0 else parent_id end as account_id, case when accounttype='5' then split_part(parentaccountname,'-',1) end groupcode, (select account_type from " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account where account_id = t1.parent_id) as parentname, debitamount, creditamount FROM " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_total_transactions t1 WHERE (case when upper('" + groupType + "')='ASON' then transaction_date<= '" + FormatDate(todate) + "' else transaction_date between '" + FormatDate(fromDate) + "' and '" + FormatDate(todate) + "' end) AND company_code ='" + CompanyCode + "' AND branch_code ='" + BranchCode + "') t group by parentname,parentaccountname, account_id, groupcode) t2)x where DEBITAMOUNT<>0 or creditamount<>0 order by account_name )x where parentname in ('ASSETS', 'EQUITY AND LIABILITIES') and groupcode='' union all select 'CHIT SUBSCRIPTION' as account_name, account_id,'chit' as groupcode,'ASSETS' as parentname,case when parentname='ASSETS' then sum(debitamount)-sum(creditamount) else sum(debitamount)-sum(creditamount) end as balance, sum(debitamount)debitamount,sum(creditamount)creditamount,sortorder from (select account_name,account_id,coalesce(groupcode,'')groupcode,case when parentname='E' then 'EXPENSES' when parentname='A' then 'ASSETS' when parentname='L' then 'EQUITY AND LIABILITIES' when parentname='I' then 'INCOME' end as parentname,DEBITAMOUNT,CREDITAMOUNT,case when parentname='E' then 2 when parentname='A' then 3 when parentname='L' then 4 when parentname='I' then 1 end as sortorder from (select account_name,account_id,groupcode,parentname,case when balamt>=0 then abs(balamt) else 0 end as debitamount, case when balamt<0 then abs(balamt) else 0 end as creditamount from (select parentname,parentaccountname as account_name,account_id,groupcode,coalesce(sum(DEBITAMOUNT),0) as DEBITAMOUNT,-coalesce(sum(CREDITAMOUNT)) as CREDITAMOUNT,coalesce(sum(DEBITAMOUNT-CREDITAMOUNT),0) as balamt from (SELECT case when accounttype='5' then split_part(parentaccountname,'-',1) else parentaccountname end as parentaccountname, case when accounttype='5' then 0 else parent_id end as account_id, case when accounttype='5' then split_part(parentaccountname,'-',1) end groupcode, (select account_type from " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account where account_id = t1.parent_id) as parentname, debitamount, creditamount FROM " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_total_transactions t1 WHERE (case when upper('" + groupType + "')='ASON' then transaction_date<= '" + FormatDate(todate) + "' else transaction_date between '" + FormatDate(fromDate) + "' and '" + FormatDate(todate) + "' end) AND company_code ='" + CompanyCode + "' AND branch_code ='" + BranchCode + "') t group by parentname,parentaccountname, account_id, groupcode) t2)x where DEBITAMOUNT<>0 or creditamount<>0 order by account_name )x where parentname in ('ASSETS', 'EQUITY AND LIABILITIES') and groupcode!='' group by account_id,parentname,sortorder order by parentname desc, sortorder";
                    // select account_name,account_id,groupcode,parentname,case when parentname='ASSETS' then debitamount-creditamount else creditamount-debitamount end as balance,debitamount,creditamount,sortorder from (select account_name,account_id,coalesce(groupcode,'') groupcode,case when parentname='E' then 'EXPENSES' when parentname='A' then 'ASSETS' when parentname='L' then 'EQUITY AND LIABILITIES' when parentname='I' then 'INCOME' end as parentname,DEBITAMOUNT,CREDITAMOUNT,case when parentname='E' then 2 when parentname='A' then 3 when parentname='L' then 4 when parentname='I' then 1 end as sortorder from (select account_name,account_id,groupcode,parentname,case when balamt>=0 then abs(balamt) else 0 end as debitamount,case when balamt<0 then abs(balamt) else 0 end as creditamount from (select parentname,parentaccountname as account_name,account_id,groupcode,coalesce(sum(DEBITAMOUNT),0) as DEBITAMOUNT,-coalesce(sum(CREDITAMOUNT)) as CREDITAMOUNT,coalesce(sum(DEBITAMOUNT-CREDITAMOUNT),0) as balamt from (SELECT case when accounttype='5' then split_part(parentaccountname,'-',1) else parentaccountname end as parentaccountname,case when accounttype='5' then 0 else parent_id end as account_id,case when accounttype='5' then split_part(parentaccountname,'-',1) end groupcode,(select account_type from " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account where account_id=t1.parent_id) as parentname,debitamount,creditamount FROM " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_total_transactions t1 WHERE case when upper('" + groupType + "')='ASON' then transaction_date<='" + FormatDate(todate) + "' else transaction_date between '" + FormatDate(fromDate) + "' and '" + FormatDate(todate) + "' end)t group by parentname,parentaccountname,account_id,groupcode)t2)x where DEBITAMOUNT<>0 or creditamount<>0 order by account_name)x where parentname in ('ASSETS','EQUITY AND LIABILITIES') and groupcode='' order by sortorder
                }

                double profitorloss = 0;

                using (NpgsqlConnection conn = new NpgsqlConnection(con))
                {
                    conn.Open();

                    // ---------- PROFIT LOSS SCALAR ----------
                    string profitQry = "select coalesce(sum(creditamount)-sum(debitamount),0.00) as balance from (" + Query + ") y";

                    using (NpgsqlCommand cmdScalar = new NpgsqlCommand(profitQry, conn))
                    {
                        object result = cmdScalar.ExecuteScalar();
                        profitorloss = result == DBNull.Value ? 0 : Convert.ToDouble(result);
                    }

                    // ---------- MAIN DATA ----------
                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            AccountReportsDTO _ObjTrialBalance = new AccountReportsDTO();

                            _ObjTrialBalance.precordid = Convert.ToInt64(dr["sortorder"]);
                            _ObjTrialBalance.paccountid = Convert.ToInt64(dr["account_id"]);
                            _ObjTrialBalance.paccountname = Convert.ToString(dr["account_name"]);
                            _ObjTrialBalance.pdebitamount = Convert.ToDouble(dr["balance"]);
                            _ObjTrialBalance.pcashtotal = Convert.ToDouble(dr["DEBITAMOUNT"]) + Convert.ToDouble(dr["CREDITAMOUNT"]);
                            _ObjTrialBalance.pcreditamount = profitorloss;
                            _ObjTrialBalance.pparentname = Convert.ToString(dr["parentname"]);
                            _ObjTrialBalance.groupcode = dr["groupcode"];

                            lstcashbook.Add(_ObjTrialBalance);
                        }
                    }
                }

                if (lstcashbook.Count > 0 && formname == "BS")
                {
                    AccountReportsDTO _ObjTrialBalance = new AccountReportsDTO();
                    _ObjTrialBalance.precordid = 2;
                    _ObjTrialBalance.paccountname = "PROFIT/(LOSS)";
                    _ObjTrialBalance.pdebitamount = profitorloss;
                    _ObjTrialBalance.pparentname = "EQUITY AND LIABILITIES";

                    lstcashbook.Add(_ObjTrialBalance);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return lstcashbook;
        }


        public List<BankTransferTypesDTO> GetBankTransferTypes(string ConnectionString, string branchSchema, string CompanyCode,
                          string BranchCode)
        {
            List<BankTransferTypesDTO> _BankTransferTypesDTO = new List<BankTransferTypesDTO>();

            string strQuery = "select tbl_mst_bank_configuration_id,account_name,tbl_mst_bank_transfer_types_id,bank_transfer_type,from_bank_account_id,to_bank_account_id from " + AddDoubleQuotes(branchSchema) + ".tbl_mst_bank_transfer_types t1 left join " + AddDoubleQuotes(branchSchema) + ".tbl_mst_bank_configuration t2 on t1.from_bank_account_id=t2.bank_account_id::varchar where t1.status=true and t1.company_code='" + CompanyCode + "' and t1.branch_code='" + BranchCode + "' order by bank_transfer_type";
            // select tbl_mst_bank_configuration_id,account_name,tbl_mst_bank_transfer_types_id,bank_transfer_type,from_bank_account_id,to_bank_account_id from "  + AddDoubleQuotes(branchSchema)  + ".tbl_mst_bank_transfer_types t1 left join " + AddDoubleQuotes(branchSchema)  + ".tbl_mst_bank_configuration t2 on t1.from_bank_account_id=t2.bank_account_id::varchar where t1.status=true order by bank_transfer_type

            try
            {
                using (NpgsqlConnection conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(strQuery, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            BankTransferTypesDTO bankTransferTypesDTO = new BankTransferTypesDTO();

                            bankTransferTypesDTO.bankttransferid = dr["tbl_mst_bank_transfer_types_id"];
                            bankTransferTypesDTO.banktransfername = dr["bank_transfer_type"];
                            bankTransferTypesDTO.frombankaccountid = dr["from_bank_account_id"];
                            bankTransferTypesDTO.tobankaccountid = dr["to_bank_account_id"];
                            bankTransferTypesDTO.bankconfigurationid = dr["tbl_mst_bank_configuration_id"];
                            bankTransferTypesDTO.accountname = dr["account_name"];

                            _BankTransferTypesDTO.Add(bankTransferTypesDTO);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }

            return _BankTransferTypesDTO;
        }


        public List<ChequeEnquiryDTO> GetChequeReturnDetails(string con, string fromdate, string todate, string BranchSchema, string GlobalSchema, string CompanyCode,
                          string BranchCode)
        {
            List<ChequeEnquiryDTO> lstChequeEnquiry = new List<ChequeEnquiryDTO>();
            try
            {
                string Query = "select * from(select clear_date::text,reference_number,tcdr.total_received_amount,(select bank_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank  where tbl_mst_bank_id = receipt_bank_id AND company_code='" + CompanyCode + "' and branch_code = '" + BranchCode + "')bank_name,tcdr.receipt_number,tcdr.received_from as particulars,coalesce(gnrc.receipt_date, chrc.chit_receipt_date) as receipt_date,clear_status,coalesce((select branch_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration c where chrc.subscriber_joined_branch_id = c.tbl_mst_branch_configuration_id),'') as referred_branch from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference tcdr left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_generalreceipt gnrc on tcdr.receipt_number = gnrc.receipt_number left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_chit_receipt chrc on tcdr.receipt_number = chrc.comman_receipt_number::varchar) x where clear_status = 'R' and clear_date between '" + FormatDate(fromdate) + "' and '" + FormatDate(todate) + "'";
                //  select * from(select clear_date::text,reference_number,tcdr.total_received_amount,(select bank_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank where tbl_mst_bank_id = receipt_bank_id)bank_name,tcdr.receipt_number,tcdr.received_from as particulars,coalesce(gnrc.receipt_date, chrc.chit_receipt_date) as receipt_date,clear_status,coalesce((select branch_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration c where chrc.subscriber_joined_branch_id = c.tbl_mst_branch_configuration_id),'') as referred_branch from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference tcdr left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_generalreceipt gnrc on tcdr.receipt_number = gnrc.receipt_number left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_chit_receipt chrc on tcdr.receipt_number = chrc.comman_receipt_number::varchar) x where clear_status = 'R' and clear_date between '" + FormatDate(fromdate) + "' and '" + FormatDate(todate) + "';

                using (NpgsqlConnection conn = new NpgsqlConnection(con))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            ChequeEnquiryDTO _Obj = new ChequeEnquiryDTO();

                            _Obj.preferencenumber = Convert.ToString(dr["reference_number"]);
                            _Obj.pparticulars = Convert.ToString(dr["particulars"]);
                            _Obj.preceiptid = Convert.ToString(dr["receipt_number"]);

                            if (!string.IsNullOrEmpty(dr["clear_date"].ToString()))
                            {
                                _Obj.pcleardate = dr["clear_date"];
                            }

                            if (!string.IsNullOrEmpty(dr["receipt_date"].ToString()))
                            {
                                _Obj.pchequedate = dr["receipt_date"];
                            }

                            _Obj.pbankname = Convert.ToString(dr["bank_name"]);
                            _Obj.ptotalreceivedamount = Convert.ToInt64(dr["total_received_amount"]);
                            _Obj.pbranchname = (dr["referred_branch"]);

                            lstChequeEnquiry.Add(_Obj);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return lstChequeEnquiry;
        }



        public List<IssuedChequeDTO> GetIssuedChequeDetails(string con, long _BankId, long _ChqBookId, long _ChqFromNo, long _ChqToNo, string BranchSchema, string GlobalSchema, string CompanyCode,
                          string BranchCode)
        {
            List<IssuedChequeDTO> lstIssuedCheque = new List<IssuedChequeDTO>();
            try
            {
                string Query = "SELECT CD.reference_number,CD.payment_number,CD.paid_to AS particulars,CD.payment_date::text,CD.clear_date::text,TB.cheque_book_id,CASE WHEN CD.clear_status='P' THEN 'Cleared' WHEN CD.clear_status='N' THEN 'Not Cleared' WHEN CD.clear_status='C' THEN 'Cancelled' WHEN CD.clear_status='R' THEN 'Returned' END AS Status,COALESCE(CD.paid_amount,0) AS paid_amount,(SELECT bank_name FROM " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank WHERE tbl_mst_bank_id IN (SELECT DISTINCT bank_id FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration WHERE tbl_mst_bank_configuration_id = CD.bank_configuration_id)) AS bank_name,'Used-Cheques' AS Cheque_Status FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference CD JOIN " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheque_management TB ON CD.bank_configuration_id = TB.bank_configuration_id AND TB.bank_configuration_id = " + _BankId + " AND TB.cheque_book_id = " + _ChqBookId + " AND TB.company_code = '" + CompanyCode + "' AND TB.branch_code = '" + BranchCode + "' WHERE CAST(CD.reference_number AS numeric) >= " + _ChqFromNo + " AND CAST(CD.reference_number AS numeric) <= " + _ChqToNo + " AND CD.company_code = '" + CompanyCode + "' AND CD.branch_code = '" + BranchCode + "' UNION ALL SELECT tc.cheque_number::text AS reference_number,'' AS payment_number,'' AS particulars,NULL AS payment_date,NULL AS clear_date,tc.cheque_book_id,'' AS Status,0 AS paid_amount,(SELECT bank_name FROM " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank WHERE tbl_mst_bank_id IN (SELECT DISTINCT bank_id FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration WHERE tbl_mst_bank_configuration_id = tc.bank_configuration_id)) AS bank_name,tc.cheque_status FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheques tc JOIN " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration tb ON tb.tbl_mst_bank_configuration_id = tc.bank_configuration_id AND tb.tbl_mst_bank_configuration_id = " + _BankId + " WHERE tc.cheque_status IN ('Un Used','Cancelled') AND tc.cheque_book_id = " + _ChqBookId + " AND tc.cheque_number >= " + _ChqFromNo + " AND tc.cheque_number <= " + _ChqToNo + " AND tc.company_code = '" + CompanyCode + "' AND tc.branch_code = '" + BranchCode + "' ORDER BY reference_number;";
                // SELECT CD.reference_number,CD.payment_number,CD.paid_to particulars,CD.payment_date::text,CD.clear_date::text,tb.cheque_book_id,(CASE WHEN CD.clear_status='P' THEN 'Cleared' WHEN CD.clear_status='N' THEN 'Not Cleared' WHEN CD.clear_status='C' THEN 'Cancelled' WHEN CD.clear_status='R' THEN 'Returned' END) as Status,coalesce(cd.paid_amount,0) as paid_amount,(select bank_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank where tbl_mst_bank_id in(select distinct bank_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where tbl_mst_bank_configuration_id=CD.bank_configuration_id)) bank_name,'Used-Cheques' as Cheque_Status FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference CD JOIN " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheque_management TB ON CD.bank_configuration_id=TB.bank_configuration_id AND TB.bank_configuration_id=" + _BankId + " AND tb.cheque_book_id=" + _ChqBookId + " AND CAST(CD.reference_number AS numeric)>=" + _ChqFromNo + " AND CAST(CD.reference_number AS numeric)<=" + _ChqToNo + " union all select tc.cheque_number::text,''payment_number,'' particulars,null as payment_date,null as clear_date,tc.cheque_book_id,'' Status,0 as paid_amount,(select bank_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank where tbl_mst_bank_id in(select distinct bank_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where tbl_mst_bank_configuration_id=tc.bank_configuration_id)) bank_name,tc.cheque_status from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheques tc join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration tb on tb.tbl_mst_bank_configuration_id=tc.bank_configuration_id and tc.cheque_status in('Un Used','Cancelled') and TB.tbl_mst_bank_configuration_id=" + _BankId + " and tc.cheque_book_id=" + _ChqBookId + " and tc.cheque_number>=" + _ChqFromNo + " AND tc.cheque_number<=" + _ChqToNo + " order by reference_number;

                using (NpgsqlConnection conn = new NpgsqlConnection(con))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    {
                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                IssuedChequeDTO _Obj = new IssuedChequeDTO();

                                _Obj.pchequenumber = Convert.ToString(dr["reference_number"]);
                                _Obj.ppaymentid = Convert.ToString(dr["payment_number"]);
                                _Obj.pparticulars = Convert.ToString(dr["particulars"]);

                                if (!string.IsNullOrEmpty(dr["payment_date"].ToString()))
                                    _Obj.ppaymentdate = dr["payment_date"];

                                if (!string.IsNullOrEmpty(dr["clear_date"].ToString()))
                                    _Obj.pcleardate = dr["clear_date"];

                                _Obj.pchkBookId = Convert.ToInt64(dr["cheque_book_id"]);
                                _Obj.pstatus = Convert.ToString(dr["Status"]);
                                _Obj.ppaidamount = Convert.ToInt64(dr["paid_amount"]);
                                _Obj.pbankname = Convert.ToString(dr["bank_name"]);
                                _Obj.pchequestatus = Convert.ToString(dr["Cheque_Status"]);

                                lstIssuedCheque.Add(_Obj);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return lstIssuedCheque;
        }



        public List<IssuedChequeDTO> GetUnUsedCheques(string con, long _BankId, long _ChqBookId, long _ChqFromNo, long _ChqToNo, string BranchSchema, string GlobalSchema, string CompanyCode,
                          string BranchCode)
        {
            List<IssuedChequeDTO> lstIssuedCheque = new List<IssuedChequeDTO>();
            try
            {
                string Query = "select tc.cheque_number::text, '' as payment_number, '' as particulars, null as payment_date, null as clear_date, tc.cheque_book_id, '' as Status, 0 as paid_amount, (select bank_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank where tbl_mst_bank_id in (select distinct bank_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where tbl_mst_bank_configuration_id = tc.bank_configuration_id)) as bank_name, tc.cheque_status from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheques tc join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration tb on tb.tbl_mst_bank_configuration_id = tc.bank_configuration_id and tc.cheque_status in ('Un Used') and tb.tbl_mst_bank_configuration_id = " + _BankId + " and tc.cheque_book_id = " + _ChqBookId + " and tc.cheque_number >= " + _ChqFromNo + " and tc.cheque_number <= " + _ChqToNo + " and tc.company_code = '" + CompanyCode + "' and tc.branch_code = '" + BranchCode + "' order by tc.cheque_number;";
                // select tc.cheque_number::text,''payment_number,'' particulars,null as payment_date,null as clear_date,tc.cheque_book_id,'' Status,0 as paid_amount,(select bank_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank where tbl_mst_bank_id in(select distinct bank_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where tbl_mst_bank_configuration_id=tc.bank_configuration_id)) bank_name,tc.cheque_status from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheques tc join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration tb on tb.tbl_mst_bank_configuration_id=tc.bank_configuration_id and tc.cheque_status in('Un Used') and TB.tbl_mst_bank_configuration_id=" + _BankId + " and tc.cheque_book_id=" + _ChqBookId + " and tc.cheque_number>=" + _ChqFromNo + " AND tc.cheque_number<=" + _ChqToNo + " order by tc.cheque_number;

                using (NpgsqlConnection conn = new NpgsqlConnection(con))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    {
                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                IssuedChequeDTO _Obj = new IssuedChequeDTO();

                                _Obj.pchequenumber = Convert.ToString(dr["cheque_number"]);
                                _Obj.ppaymentid = Convert.ToString(dr["payment_number"]);
                                _Obj.pparticulars = Convert.ToString(dr["particulars"]);

                                if (!string.IsNullOrEmpty(dr["payment_date"].ToString()))
                                    _Obj.ppaymentdate = dr["payment_date"];

                                if (!string.IsNullOrEmpty(dr["clear_date"].ToString()))
                                    _Obj.pcleardate = dr["clear_date"];

                                _Obj.pchkBookId = Convert.ToInt64(dr["cheque_book_id"]);
                                _Obj.pstatus = Convert.ToString(dr["Status"]);
                                _Obj.ppaidamount = Convert.ToInt64(dr["paid_amount"]);
                                _Obj.pbankname = Convert.ToString(dr["bank_name"]);
                                _Obj.pchequestatus = Convert.ToString(dr["Cheque_Status"]);

                                lstIssuedCheque.Add(_Obj);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return lstIssuedCheque;
        }



        public List<CountryDTO> getCountry(string ConnectionString, string GlobalSchema)
        {
            List<CountryDTO> _lstCountryDTO = new List<CountryDTO>();

            try
            {
                string Query = "SELECT tbl_mst_country_id,country_name FROM " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_country where status=true order by country_name";

                using (NpgsqlConnection conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            CountryDTO CountryDTO = new CountryDTO
                            {
                                tbl_mst_country_id = dr["tbl_mst_country_id"],
                                country_name = dr["country_name"]
                            };

                            _lstCountryDTO.Add(CountryDTO);
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }

            return _lstCountryDTO;
        }




        public List<StateDTO> getState(string ConnectionString, string GlobalSchema, long id)
        {
            List<StateDTO> _lstStateDTO = new List<StateDTO>();

            try
            {
                string Query = "SELECT tbl_mst_state_id,state_name FROM " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_state where country_id=" + id + " order by state_name";

                using (NpgsqlConnection conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            StateDTO StateDTO = new StateDTO
                            {
                                tbl_mst_state_id = dr["tbl_mst_state_id"],
                                state_name = dr["state_name"]
                            };

                            _lstStateDTO.Add(StateDTO);
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
            return _lstStateDTO;
        }


        public List<District> getDistrict(string ConnectionString, string GlobalSchema, long id)
        {
            List<District> _lstDistrict = new List<District>();

            try
            {
                string Query = "SELECT tbl_mst_district_id,district_name FROM " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_district where state_id=" + id + " order by district_name";

                using (NpgsqlConnection conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            District districtDTO = new District
                            {
                                tbl_mst_district_id = dr["tbl_mst_district_id"],
                                district_name = dr["district_name"]
                            };

                            _lstDistrict.Add(districtDTO);
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
            return _lstDistrict;
        }

        #region Getformnamedetails...

        public List<Formnamedetails> Getformnamedetails(
            string connectionString,
            string globalSchema,
            string companyCode,
            string branchCode)
        {
            List<Formnamedetails> formList = new List<Formnamedetails>();

            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string is null or empty", nameof(connectionString));

            try
            {
                // Validate connection string
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
                    cmd.CommandType = CommandType.Text;

                    cmd.CommandText = $@"
                SELECT form_name
                FROM
                (
                    SELECT DISTINCT 
                        REPLACE(form_name,'GENERAL VOUCHER','GENERAL Receipt') AS form_name
                    FROM {AddDoubleQuotes(globalSchema)}.tbl_mst_generate_id
                    WHERE con_column_name='ACCOUNTING'
                      AND company_code=@CompanyCode
                      AND branch_code=@BranchCode

                    UNION ALL

                    SELECT DISTINCT 'SUBSCRIBER JV CANCEL'
                    FROM {AddDoubleQuotes(globalSchema)}.tbl_mst_generate_id
                    WHERE con_column_name='ACCOUNTING'
                      AND company_code=@CompanyCode
                      AND branch_code=@BranchCode

                    UNION ALL

                    SELECT DISTINCT 'GENERAL RECEIPT CANCEL'
                    FROM {AddDoubleQuotes(globalSchema)}.tbl_mst_generate_id
                    WHERE con_column_name='ACCOUNTING'
                      AND company_code=@CompanyCode
                      AND branch_code=@BranchCode

                    UNION ALL

                    SELECT DISTINCT 'CHEQUES ON HAND CANCEL'
                    FROM {AddDoubleQuotes(globalSchema)}.tbl_mst_generate_id
                    WHERE con_column_name='ACCOUNTING'
                      AND company_code=@CompanyCode
                      AND branch_code=@BranchCode

                    UNION ALL

                    SELECT DISTINCT 'SUBSCRIBER JV GROUP-GROUP'
                    FROM {AddDoubleQuotes(globalSchema)}.tbl_mst_generate_id
                    WHERE con_column_name='ACCOUNTING'
                      AND company_code=@CompanyCode
                      AND branch_code=@BranchCode
                ) t
                ORDER BY 1;
            ";

                    cmd.Parameters.AddWithValue("@CompanyCode", companyCode);
                    cmd.Parameters.AddWithValue("@BranchCode", branchCode);

                    using var reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        Formnamedetails obj = new Formnamedetails();


                        obj.formNames = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);


                        formList.Add(obj);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to retrieve form name details (schema={globalSchema}). See inner exception for details.",
                    ex);
            }

            return formList;
        }


        #endregion Getformnamedetails...


        public List<PaymentVoucherReportDTO> GetChitPaymentReportData(
            string paymentId,
            string LocalSchema,
            string GlobalSchema,
            string Connectionstring,
            string CompanyCode,
            string BranchCode)
        {
            List<PaymentVoucherReportDTO> PaymentVoucherReportlist = new List<PaymentVoucherReportDTO>();

            try
            {
                string Query = "select distinct transaction_amount,accountname,parentaccountname,credit_account_id as debit_account_id,tr.payment_number,coalesce(tr.transaction_date::text,'') payment_date,tr.narration,coalesce(tr.contact_id,0) contact_id,coalesce((select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=tr.contact_id)) contactname,tr.modeof_payment,reference_number,trr.trans_type,(select contact_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=coalesce(employee_id,0)) employeename,bc.account_name||'-'||account_number as bank_account,0 as interbranch_id from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_chit_payment tr left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_reference trr on trr.payment_number=tr.payment_number join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_total_transactions ttt on tr.payment_number=ttt.transaction_no left join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_bank_configuration bc on bc.tbl_mst_bank_configuration_id=trr.bank_configuration_id join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account t1 on tr.credit_account_id=t1.account_id where tr.payment_number='" + paymentId + "' and debitamount>0 AND t1.company_code='" + CompanyCode + "' and t1.branch_code='" + BranchCode + "';";

                using (NpgsqlConnection conn = new NpgsqlConnection(Connectionstring))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            PaymentVoucherReportDTO pPaymentVoucherReportDTO = new PaymentVoucherReportDTO
                            {
                                ppaymentdate = dr["payment_date"],
                                ppaymentid = Convert.ToString(dr["payment_number"]),
                                accountname = dr["accountname"],
                                parentaccountname = dr["parentaccountname"],
                                pcontactid = Convert.ToString(dr["contact_id"]),
                                pcontactname = Convert.ToString(dr["contactname"]),
                                transaction_amount = dr["transaction_amount"],
                                pnarration = Convert.ToString(dr["narration"]),
                                pmodofPayment = PayModes(Convert.ToString(dr["modeof_payment"]), "D"),
                                pChequenumber = Convert.ToString(dr["reference_number"]),
                                ptypeofpayment = PayModes(Convert.ToString(dr["trans_type"]), "D"),
                                pemployeename = Convert.ToString(dr["employeename"]),
                                pbankaccount = dr["bank_account"],
                                ppaymentslist = GetPaymentVoucherDetailsReportData(
                                    paymentId,
                                    dr["contact_id"],
                                    Connectionstring,
                                    LocalSchema,
                                    GlobalSchema,
                                    dr["debit_account_id"],
                                    dr["interbranch_id"],
                                    BranchCode,
                                    CompanyCode)
                            };

                            PaymentVoucherReportlist.Add(pPaymentVoucherReportDTO);
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }

            return PaymentVoucherReportlist;
        }

        public string PayModes(string paymode, string param)
        {
            string p = string.Empty;
            paymode = paymode.ToUpper();
            if (param == "V")//return values
            {
                switch (paymode)
                {
                    case "CASH":
                        p = "C";
                        break;
                    case "BANK":
                        p = "B";
                        break;
                    case "CHEQUE":
                        p = "CH";
                        break;
                    case "ONLINE":
                        p = "O";
                        break;
                    case "NEFT":
                        p = "N";
                        break;
                    case "RTGS":
                        p = "R";
                        break;
                    case "UPI":
                        p = "U";
                        break;
                    case "IMPS":
                        p = "I";
                        break;
                    case "DEBIT CARD":
                        p = "DC";
                        break;
                    case "CREDIT CARD":
                        p = "CC";
                        break;
                    case "ADJUSTMENT":
                        p = "A";
                        break;
                    case "QR":
                        p = "QR";
                        break;
                    default:
                        break;
                }
            }
            if (param == "D") //return description
            {
                switch (paymode)
                {
                    case "C":
                        p = "CASH";
                        break;
                    case "B":
                        p = "BANK";
                        break;
                    case "CH":
                        p = "CHEQUE";
                        break;
                    case "O":
                        p = "ONLINE";
                        break;
                    case "N":
                        p = "NEFT";
                        break;
                    case "R":
                        p = "RTGS";
                        break;
                    case "U":
                        p = "UPI";
                        break;
                    case "I":
                        p = "IMPS";
                        break;
                    case "DC":
                        p = "DEBIT CARD";
                        break;
                    case "CC":
                        p = "CREDIT CARD";
                        break;
                    case "A":
                        p = "ADJUSTMENT";
                        break;
                    case "QR":
                        p = "QR";
                        break;
                    default:
                        break;
                }

            }
            return p;
        }


        public List<GeneralReceiptSubDetails> GetPaymentVoucherDetailsReportData(string paymentId, object contact_id, string Connectionstring, string LocalSchema, string GlobalSchema, object accountid, object interbranch_id, string branchcode, string companycode)
        {
            List<GeneralReceiptSubDetails> GeneralReceiptlist = new List<GeneralReceiptSubDetails>();
            string strQuery = string.Empty;
            object state_code = "";

            try
            {
                using (NpgsqlConnection con = new NpgsqlConnection(Connectionstring))
                {
                    con.Open();

                    if (Convert.ToInt64(contact_id) > 0)
                    {
                        string branch_code = string.Empty;

                        if (!string.IsNullOrEmpty(Convert.ToString(interbranch_id)))
                        {
                            if (Convert.ToInt64(interbranch_id) > 0)
                            {
                                using (var cmdScalar = new NpgsqlCommand("select branch_code from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where tbl_mst_branch_configuration_id=" + interbranch_id + " AND branch_code = '" + branch_code + "'", con))
                                //select branch_code from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where tbl_mst_branch_configuration_id=" + interbranch_id
                                {
                                    var result = cmdScalar.ExecuteScalar();
                                    branch_code = Convert.ToString(result);
                                }
                            }
                        }

                        if (!string.IsNullOrEmpty(branch_code))
                        {
                            strQuery = "SELECT contactname, accountname, SUM(ledgeramount)ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage FROM (select coalesce(contactname,newcontactname) contactname,accountname,coalesce(ledgeramount,0)+coalesce(tds_amount,0) as ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage from (select contact_mailing_name as contactname,case when tac.chracc_type in ( '2','5') then tac.account_name else  t1.account_name||'('||tac.account_name||')'  end accountname,(coalesce(t4.ledger_amount, 0)) ledgeramount,tac.gst_calculation_type,t4.gst_amount,tac.tds_calculation_type,t4.tds_amount,tac.account_name newcontactname,coalesce(Round(t4.tds_percentage,0),0)tds_percentage, coalesce((Round(t4.gst_percentage/2,0)),0)gst_percentage  from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher_details tr left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher t3 on t3.tbl_trans_payment_voucher_id=tr.payment_voucher_id left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_interbranch_payment_voucher t4 on t4.interbranch_payment_number=t3.payment_number and t4.interbranch_id=tr.branch_id AND company_code = '" + companycode + "' and branch_code = '" + branchcode + "' join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account tac on tac.account_id = t4.debit_account_id join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account t1 on tac.parent_id = t1.account_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact c on c.tbl_mst_contact_id = tr.contact_id where payment_number = '" + paymentId + "' and tr.debit_account_id = " + accountid + " and tr.contact_id = " + contact_id + ")x)X group by contactname, accountname, gst_calculation_type, gst_amount, tds_calculation_type, tds_amount, tds_percentage, gst_percentage;";
                            // SELECT contactname,accountname,SUM(ledgeramount)ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage FROM (select coalesce(contactname,newcontactname) contactname,accountname,coalesce(ledgeramount,0)+coalesce(tds_amount,0) as ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage from (select contact_mailing_name as contactname,case when tac.chracc_type in('2','5') then tac.account_name else t1.account_name||'('||tac.account_name||')' end accountname,(coalesce(t4.ledger_amount,0)) ledgeramount,tac.gst_calculation_type,t4.gst_amount,tac.tds_calculation_type,t4.tds_amount,tac.account_name newcontactname,coalesce(Round(t4.tds_percentage,0),0)tds_percentage,coalesce((Round(t4.gst_percentage/2,0)),0)gst_percentage from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher_details tr left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher t3 on t3.tbl_trans_payment_voucher_id=tr.payment_voucher_id left join " + AddDoubleQuotes(branch_code) + ".tbl_trans_interbranch_payment_voucher t4 on t4.interbranch_payment_number=t3.payment_number and t4.interbranch_id=tr.branch_id join " + AddDoubleQuotes(branch_code) + ".tbl_mst_account tac on tac.account_id=t4.debit_account_id join " + AddDoubleQuotes(branch_code) + ".tbl_mst_account t1 on tac.parent_id=t1.account_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact c on c.tbl_mst_contact_id=tr.contact_id where payment_number='" + paymentId + "' and tr.debit_account_id=" + accountid + " and tr.contact_id=" + contact_id + ")x)X group by contactname,accountname,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage;
                        }
                        else
                        {
                            strQuery = "SELECT contactname, accountname, SUM(ledgeramount)ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage FROM (select coalesce(contactname,newcontactname) contactname,accountname,coalesce(ledgeramount,0)+coalesce(tds_amount,0) as ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage from (select contact_mailing_name as contactname,case when tac.chracc_type in ( '2','5') then tac.account_name else  t1.account_name||'('||tac.account_name||')'  end accountname,(coalesce(ledger_amount, 0)) ledgeramount,tac.gst_calculation_type,gst_amount,tac.tds_calculation_type,tds_amount,tac.account_name newcontactname,coalesce(Round(tr.tds_percentage,0),0)tds_percentage, coalesce((Round(tr.gst_percentage/2,0)),0)gst_percentage  from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher_details tr join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account tac on tac.account_id = tr.debit_account_id join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account t1 on tac.parent_id = t1.account_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact c on c.tbl_mst_contact_id = tr.contact_id where payment_voucher_id in(select tbl_trans_payment_voucher_id from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher where payment_number = '" + paymentId + "') and debit_account_id=" + accountid + " and tr.contact_id=" + contact_id + " AND t1.company_code = '" + companycode + "' and t1.branch_code = '" + branchcode + "')x)X group by contactname, accountname, gst_calculation_type, gst_amount, tds_calculation_type, tds_amount,tds_percentage,gst_percentage";
                            //SELECT contactname,accountname,SUM(ledgeramount)ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage FROM (select coalesce(contactname,newcontactname) contactname,accountname,coalesce(ledgeramount,0)+coalesce(tds_amount,0) as ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage from (select contact_mailing_name as contactname,case when tac.chracc_type in('2','5') then tac.account_name else t1.account_name||'('||tac.account_name||')' end accountname,(coalesce(ledger_amount,0)) ledgeramount,tac.gst_calculation_type,gst_amount,tac.tds_calculation_type,tds_amount,tac.account_name newcontactname,coalesce(Round(tr.tds_percentage,0),0)tds_percentage,coalesce((Round(tr.gst_percentage/2,0)),0)gst_percentage from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher_details tr join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account tac on tac.account_id=tr.debit_account_id join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account t1 on tac.parent_id=t1.account_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact c on c.tbl_mst_contact_id=tr.contact_id where payment_voucher_id in(select tbl_trans_payment_voucher_id from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher where payment_number='" + paymentId + "') and debit_account_id=" + accountid + " and tr.contact_id=" + contact_id + ")x)X group by contactname,accountname,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage
                        }

                        using (var cmdState = new NpgsqlCommand(" select t3.state_code from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact_address_details t1 join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_district t2 on t1.district_id = t2.tbl_mst_district_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_state t3 on t2.state_id = t3.tbl_mst_state_id where contact_id =" + contact_id + " and t1.status=true and t1.isprimary=true and company_code = '" + companycode + "' and branch_code = '" + branchcode + "'", con))
                        // select t3.state_code from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact_address_details t1 join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_district t2 on t1.district_id=t2.tbl_mst_district_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_state t3 on t2.state_id=t3.tbl_mst_state_id where contact_id=" + contact_id + " and t1.status=true and t1.isprimary=true
                        {
                            state_code = cmdState.ExecuteScalar();
                        }
                    }
                    else
                    {
                        strQuery = "SELECT contactname, accountname, SUM(ledgeramount)ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage FROM (select coalesce(contactname,newcontactname) contactname,accountname,coalesce(ledgeramount,0)+coalesce(tds_amount,0) as ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage from (select contact_mailing_name as contactname,case when tac.chracc_type in ( '2','5') then tac.account_name else  t1.account_name||'('||tac.account_name||')'  end accountname,(coalesce(ledger_amount, 0)) ledgeramount,tac.gst_calculation_type,gst_amount,tac.tds_calculation_type,tds_amount,tac.account_name newcontactname,coalesce(Round(tr.tds_percentage,0),0)tds_percentage, coalesce((Round(tr.gst_percentage/2,0)),0)gst_percentage  from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher_details tr join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account tac on tac.account_id = tr.debit_account_id join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account t1 on tac.parent_id = t1.account_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact c on c.tbl_mst_contact_id = tr.contact_id where payment_voucher_id in(select tbl_trans_payment_voucher_id from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher where payment_number = '" + paymentId + "') and tr.debit_account_id=" + accountid + " and c.company_code = '" + companycode + "' and c.branch_code = '" + branchcode + "')x)X group by contactname, accountname, gst_calculation_type, gst_amount, tds_calculation_type, tds_amount,tds_percentage,gst_percentage";
                        // SELECT contactname,accountname,SUM(ledgeramount)ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage FROM (select coalesce(contactname,newcontactname) contactname,accountname,coalesce(ledgeramount,0)+coalesce(tds_amount,0) as ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage from (select contact_mailing_name as contactname,case when tac.chracc_type in('2','5') then tac.account_name else t1.account_name||'('||tac.account_name||')' end accountname,(coalesce(ledger_amount,0)) ledgeramount,tac.gst_calculation_type,gst_amount,tac.tds_calculation_type,tds_amount,tac.account_name newcontactname,coalesce(Round(tr.tds_percentage,0),0)tds_percentage,coalesce((Round(tr.gst_percentage/2,0)),0)gst_percentage from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher_details tr join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account tac on tac.account_id=tr.debit_account_id join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account t1 on tac.parent_id=t1.account_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact c on c.tbl_mst_contact_id=tr.contact_id where payment_voucher_id in(select tbl_trans_payment_voucher_id from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher where payment_number='" + paymentId + "') and tr.debit_account_id=" + accountid + ")x)X group by contactname,accountname,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount,tds_percentage,gst_percentage
                    }

                    using (NpgsqlCommand cmd = new NpgsqlCommand(strQuery, con))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            GeneralReceiptSubDetails _GeneralReceipt = new GeneralReceiptSubDetails();
                            _GeneralReceipt.pLedgeramount = Convert.ToDecimal(dr["ledgeramount"]);
                            _GeneralReceipt.pAccountname = Convert.ToString(dr["accountname"]);
                            _GeneralReceipt.pgstcalculationtype = Convert.ToString(dr["gst_calculation_type"]);

                            if (!string.IsNullOrEmpty(dr["gst_amount"].ToString()))
                                _GeneralReceipt.pcgstamount = Convert.ToDecimal(dr["gst_amount"]);

                            _GeneralReceipt.ptdscalculationtype = Convert.ToString(dr["tds_calculation_type"]);

                            if (!string.IsNullOrEmpty(dr["tds_amount"].ToString()))
                                _GeneralReceipt.ptdsamount = Convert.ToDecimal(dr["tds_amount"]);

                            _GeneralReceipt.state_code = state_code;

                            GeneralReceiptlist.Add(_GeneralReceipt);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }

            return GeneralReceiptlist;
        }



        public List<BankUPI> GetUpiNames(long bankid, string ConnectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode)
        {
            List<BankUPI> bankupilist = new List<BankUPI>();

            try
            {
                string Query = "select  bank_upi_address as upiid,upi_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_upi_details a join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank_upi_names b on a.bank_upi_id=b.tbl_mst_bank_upi_names_id where bank_configuration_id=" + bankid + " and a.status=true and b.company_code='" + CompanyCode + "' and b.branch_code='" + BranchCode + "' order by upi_name;";
                // select bank_upi_address as upiid,upi_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_upi_details a join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank_upi_names b on a.bank_upi_id=b.tbl_mst_bank_upi_names_id where bank_configuration_id=" + bankid + " and a.status=true order by upi_name;

                using (NpgsqlConnection conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            BankUPI _BankUPI = new BankUPI
                            {
                                pUpiid = Convert.ToString(dr["upiid"]),
                                pUpiname = Convert.ToString(dr["upi_name"])
                            };

                            bankupilist.Add(_BankUPI);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return bankupilist;
        }

        public List<ChequesDTO> GetChequeNumbers(long bankid, string ConnectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode)
        {
            List<ChequesDTO> chequeslist = new List<ChequesDTO>();

            try
            {
                string Query = "select  cheque_book_id , cheque_number  from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheques  where  bank_configuration_id =" + bankid + " and (cheque_status )='Un Used'  and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'order by cheque_number;";
                // select cheque_book_id, cheque_number from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_cheques where bank_configuration_id=" + bankid + " and (cheque_status)='Un Used' order by cheque_number;

                using (NpgsqlConnection conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            ChequesDTO _ChequesDTO = new ChequesDTO
                            {
                                pChequenumber = Convert.ToInt64(dr["cheque_number"]),
                                pChqbookid = Convert.ToInt64(dr["cheque_book_id"])
                            };

                            chequeslist.Add(_ChequesDTO);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return chequeslist;
        }



        public List<JvListDTO> GetJvListDetails(string con, string fromDate, string toDate, string modeOfTransaction, string BranchSchema, string GlobalSchema, string companyCode, string branchCode)
        {
            List<JvListDTO> lstJvList = new List<JvListDTO>();
            string Query = string.Empty;

            try
            {
                if (!string.IsNullOrEmpty(fromDate) && !string.IsNullOrEmpty(toDate))
                {
                    if (modeOfTransaction.ToUpper() == "MANUAL")
                    {
                        Query = "SELECT JV.journal_voucher_no,JV.journal_voucher_date::text,JV.narration,CASE WHEN JVD.account_trans_type='D' THEN JVD.ledger_amount END AS DEBIT,CASE WHEN JVD.account_trans_type='C' THEN JVD.ledger_amount END AS CREDIT,JVD.account_trans_type," + AddDoubleQuotes(GlobalSchema) + ".fn_getparticulars('" + BranchSchema + "',jv_account_id) AS PARTICULARS,transaction_type FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher_details JVD join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher JV on jvd.journal_voucher_id=jv.tbl_trans_journal_voucher_id WHERE journal_voucher_date BETWEEN '" + FormatDate(fromDate) + "' and '" + FormatDate(toDate) + "' and transaction_type='M' and JV.company_code ='" + companyCode + "' and JV.branch_code ='" + branchCode + "' ORDER BY journal_voucher_date,journal_voucher_no ; ";

                    }
                    else if (modeOfTransaction.ToUpper() == "AUTO")
                    {
                        Query = "SELECT JV.journal_voucher_no,JV.journal_voucher_date::text,JV.narration,CASE WHEN JVD.account_trans_type='D' THEN JVD.ledger_amount  END AS DEBIT,CASE WHEN JVD.account_trans_type='C' THEN JVD.ledger_amount  END AS CREDIT,JVD.account_trans_type ," + AddDoubleQuotes(GlobalSchema) + ".fn_getparticulars('" + BranchSchema + "',jv_account_id)AS PARTICULARS,transaction_type FROM   " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher_details  JVD ," + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher  JV WHERE JVD.journal_voucher_id=JV.tbl_trans_journal_voucher_id  and journal_voucher_date BETWEEN '" + FormatDate(fromDate) + "' and '" + FormatDate(toDate) + "' and transaction_type='A' and JV.company_code ='" + companyCode + "' and JV.branch_code ='" + branchCode + "' ORDER BY journal_voucher_date,journal_voucher_no;";

                    }
                    else
                    {
                        Query = "SELECT JV.journal_voucher_no,JV.journal_voucher_date::text,JV.narration,CASE WHEN JVD.account_trans_type='D' THEN JVD.ledger_amount  END AS DEBIT,CASE WHEN JVD.account_trans_type='C' THEN JVD.ledger_amount  END AS CREDIT,JVD.account_trans_type ," + AddDoubleQuotes(GlobalSchema) + ".fn_getparticulars('" + BranchSchema + "',jv_account_id)AS PARTICULARS,transaction_type FROM   " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher_details  JVD ," + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher  JV WHERE JVD.journal_voucher_id=JV.tbl_trans_journal_voucher_id  and JV.company_code ='" + companyCode + "' and JV.branch_code ='" + branchCode + "' and journal_voucher_date BETWEEN '" + FormatDate(fromDate) + "' and '" + FormatDate(toDate) + "'  ORDER BY journal_voucher_date,journal_voucher_no;";

                    }

                    using (NpgsqlConnection conn = new NpgsqlConnection(con))
                    {
                        conn.Open();

                        using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                JvListDTO _Obj = new JvListDTO();

                                _Obj.ptransactiondate = dr["journal_voucher_date"];
                                _Obj.ptransactionno = Convert.ToString(dr["journal_voucher_no"]);
                                _Obj.pparticulars = Convert.ToString(dr["PARTICULARS"]);
                                _Obj.pcreditamount = dr["CREDIT"] == DBNull.Value ? 0 : Convert.ToDouble(dr["CREDIT"]);
                                _Obj.pdebitamount = dr["DEBIT"] == DBNull.Value ? 0 : Convert.ToDouble(dr["DEBIT"]);
                                _Obj.pdescription = Convert.ToString(dr["narration"]);

                                lstJvList.Add(_Obj);
                            }
                        }
                    }
                }
            }
            catch
            {
                throw;
            }

            return lstJvList;
        }




        public List<AccountReportsDTO> GetAgentPaymentDetails(string con, string GlobalSchema, string BranchSchema, string parentaccountname, string accountname, string companyCode, string branchCode)
        {
            string Query = string.Empty;
            string pQuery = string.Empty;

            List<AccountReportsDTO> lstAgentMarket = new List<AccountReportsDTO>();

            try
            {
                Query = "select rownumber as recordid,transaction_date,parent_id,account_id,null as formname,transaction_no,PARTICULARS,narration,DEBITAMOUNT,abs(CREDITAMOUNT)as CREDITAMOUNT,abs(balance) as balance,case when balance>0 then 'Dr' else 'Cr' end as balancetype from (select row_number() over (order by RECORDID) as rownumber,*,sum(DEBITAMOUNT+CREDITAMOUNT) OVER(ORDER BY RECORDID)as BALANCE from(select distinct x.RECORDID,transaction_date,null as formname,parent_id,account_id,TRANSACTION_NO,PARTICULARS,COALESCE(DEBITAMOUNT,0.00) as DEBITAMOUNT,COALESCE(CREDITAMOUNT,0.00) as CREDITAMOUNT,narration from (SELECT tbl_trans_total_transactions_id as Recordid,transaction_date,parent_id,account_id,TRANSACTION_NO,PARTICULARS,COALESCE(DEBITAMOUNT,0.00) as DEBITAMOUNT,-COALESCE(CREDITAMOUNT,0.00) as CREDITAMOUNT,narration,split_part(REGEXP_REPLACE(TRANSACTION_NO,'[[:digit:]]','','g'),'/',1) as code FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions WHERE parentaccountname = '" + parentaccountname + "' and accountname='" + accountname + "' and company_code = '" + companyCode + "' and branch_code = '" + branchCode + "' AND (debitamount<>0 or creditamount<>0)) x join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_generate_id y on x.code=y.code) as D order by RECORDID )x order by RECORDID";
                // select rownumber as recordid,transaction_date,parent_id,account_id,null as formname,transaction_no,PARTICULARS,narration,DEBITAMOUNT,abs(CREDITAMOUNT)as CREDITAMOUNT,abs(balance) as balance,case when balance>0 then 'Dr' else 'Cr' end as balancetype from (select row_number() over (order by RECORDID) as rownumber,*,sum(DEBITAMOUNT+CREDITAMOUNT) OVER(ORDER BY RECORDID)as BALANCE from(select distinct x.RECORDID,transaction_date,null as formname,parent_id,account_id, TRANSACTION_NO,PARTICULARS,COALESCE(DEBITAMOUNT,0.00) as DEBITAMOUNT,COALESCE(CREDITAMOUNT,0.00) as CREDITAMOUNT,narration from (SELECT tbl_trans_total_transactions_id as Recordid,transaction_date,parent_id,account_id, TRANSACTION_NO,PARTICULARS,COALESCE(DEBITAMOUNT,0.00) as DEBITAMOUNT,-COALESCE(CREDITAMOUNT,0.00) as CREDITAMOUNT,narration,split_part(REGEXP_REPLACE(TRANSACTION_NO,'[[:digit:]]','','g'),'/',1) as code FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions WHERE parentaccountname='" + parentaccountname + "' and accountname='" + accountname + "' AND (debitamount<>0 or creditamount<>0)) x join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_generate_id y on x.code=y.code) as D order by RECORDID )x order by RECORDID

                using (NpgsqlConnection conn = new NpgsqlConnection(con))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            AccountReportsDTO _ObjBank = new AccountReportsDTO();

                            _ObjBank.precordid = Convert.ToInt64(dr["RECORDID"]);
                            _ObjBank.pparentid = Convert.ToInt64(dr["parent_id"]);
                            _ObjBank.paccountid = Convert.ToInt64(dr["account_id"]);
                            _ObjBank.pFormName = Convert.ToString(dr["formname"]);
                            _ObjBank.pdebitamount = Convert.ToDouble(dr["DEBITAMOUNT"]);
                            _ObjBank.pcreditamount = Convert.ToDouble(dr["CREDITAMOUNT"]);
                            _ObjBank.pparticulars = Convert.ToString(dr["PARTICULARS"]);
                            _ObjBank.pdescription = Convert.ToString(dr["narration"]);
                            _ObjBank.ptransactionno = Convert.ToString(dr["TRANSACTION_NO"]);
                            _ObjBank.popeningbal = Convert.ToDouble(dr["BALANCE"]);
                            _ObjBank.pBalanceType = Convert.ToString(dr["balancetype"]);
                            _ObjBank.ptransactiondate = dr["transaction_date"];

                            lstAgentMarket.Add(_ObjBank);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return lstAgentMarket;
        }



        public List<JournalVoucherReportDTO> GetJournalVoucherReportData(
            string ConnectionString,
            string GlobalSchema,
            string BranchSchema,
            string Jvnumber, string companyCode, string branchCode)
        {
            string strQuery1 = string.Empty;
            string Branchtype = string.Empty;
            string Legalcelltype = string.Empty;

            List<JournalVoucherReportDTO> lstJournalVoucherReport = new List<JournalVoucherReportDTO>();

            try
            {
                using (NpgsqlConnection conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();

                    strQuery1 = "select legalcell_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration  where branch_code='" + branchCode + "'";
                    //select legalcell_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where branch_code='" + BranchSchema + "'

                    using (NpgsqlCommand cmd1 = new NpgsqlCommand(strQuery1, conn))
                    {
                        object result = cmd1.ExecuteScalar();
                        Legalcelltype = result == null ? "" : Convert.ToString(result);
                    }

                    string strQuery = "select c.journal_voucher_no as  jvnumber,coalesce(c.journal_voucher_date::text,'') as jvdate,jv_account_id as jvaccountid,account_name as accountname,user_id,coalesce(contact_id::text,'') as contactid,coalesce((select coalesce(contact_mailing_name,'') from  " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=user_id),'')contactname,case when chracc_type='3' then " + AddDoubleQuotes(GlobalSchema) + ".fn_getparticulars('" + BranchSchema + "',jv_account_id) else account_name end as particulars, c.narration,(select account_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_id=b.parent_id) parentaccountname,case when account_trans_type='D' then ledger_amount else 0 end as debitamount,case when account_trans_type='C' then ledger_amount else 0 end as creditamount  from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher_details a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher c on c.tbl_trans_journal_voucher_id=a.journal_voucher_id join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account b on a.jv_account_id=b.account_id  left join  " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher_log   l on l.journal_voucher_no=c.journal_voucher_no AND l.company_code = '" + companyCode + "' and l.branch_code = '" + branchCode + "' where UPPER(c.journal_voucher_no) ='" + Jvnumber + "' Order by debitamount desc;";
                    //select c.journal_voucher_no as jvnumber,coalesce(c.journal_voucher_date::text,'') as jvdate,jv_account_id as jvaccountid,account_name as accountname,user_id,coalesce(contact_id::text,'') as contactid,coalesce((select coalesce(contact_mailing_name,'') from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=user_id),'') contactname,case when chracc_type='3' then " + AddDoubleQuotes(GlobalSchema) + ".fn_getparticulars('" + BranchSchema + "',jv_account_id) else account_name end as particulars,c.narration,(select account_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_id=b.parent_id) parentaccountname,case when account_trans_type='D' then ledger_amount else 0 end as debitamount,case when account_trans_type='C' then ledger_amount else 0 end as creditamount from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher_details a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher c on c.tbl_trans_journal_voucher_id=a.journal_voucher_id join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account b on a.jv_account_id=b.account_id left join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher_log l on l.journal_voucher_no=c.journal_voucher_no where UPPER(c.journal_voucher_no)='" + Jvnumber + "' order by debitamount desc;

                    using (NpgsqlCommand cmd = new NpgsqlCommand(strQuery, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            JournalVoucherReportDTO _JVReportDTO = new JournalVoucherReportDTO();

                            _JVReportDTO.pJvDate = dr["jvdate"];
                            _JVReportDTO.pJvnumber = Convert.ToString(dr["jvnumber"]);
                            _JVReportDTO.pCreditAmount = dr["creditamount"] == DBNull.Value ? 0 : Convert.ToDouble(dr["creditamount"]);
                            _JVReportDTO.pDebitamount = dr["debitamount"] == DBNull.Value ? 0 : Convert.ToDouble(dr["debitamount"]);

                            if (!string.IsNullOrEmpty(Convert.ToString(Legalcelltype)))
                            {
                                if (!string.IsNullOrEmpty(Convert.ToString(dr["contactid"])))
                                {
                                    string lcQuery = "select coalesce(split_part(groupcode,'(',1),'') from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher_lc_expenses where contact_id='" + (dr["contactid"]) + "' and transaction_type = 'D' and jv_account_id =" + dr["jvaccountid"] + " and journal_voucher_no ='" + Convert.ToString(dr["jvnumber"]) + "';";
                                    //select coalesce(split_part(groupcode,'(',1),'') from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_journal_voucher_lc_expenses where contact_id='" + dr["contactid"] + "' and transaction_type='D' and jv_account_id=" + dr["jvaccountid"] + " and journal_voucher_no='" + Convert.ToString(dr["jvnumber"]) + "';

                                    string group = "";


                                    using (NpgsqlConnection conn2 = new NpgsqlConnection(ConnectionString))
                                    {
                                        conn2.Open();

                                        using (NpgsqlCommand cmd2 = new NpgsqlCommand(lcQuery, conn2))
                                        {
                                            object grp = cmd2.ExecuteScalar();
                                            group = grp == null ? "" : Convert.ToString(grp);
                                        }
                                    }

                                    _JVReportDTO.pParticulars = Convert.ToString(dr["particulars"]) + " " + group;
                                }
                                else
                                {
                                    _JVReportDTO.pParticulars = Convert.ToString(dr["particulars"]);
                                }
                            }
                            else
                            {
                                _JVReportDTO.pParticulars = Convert.ToString(dr["particulars"]);
                            }

                            _JVReportDTO.pNarration = Convert.ToString(dr["narration"]);
                            _JVReportDTO.pContactName = dr["contactname"];

                            lstJournalVoucherReport.Add(_JVReportDTO);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return lstJournalVoucherReport;
        }



        public List<SVOnameDTO> Getmvonames(string connectionstring, string GlobalSchema, string localSchema, string branchcode)
        {
            List<SVOnameDTO> lstSVOnameDTO = new List<SVOnameDTO>();
            string _query;

            try
            {
                _query = "select distinct tbl_mst_branch_configuration_id, branch_code, branch_name,branch_type from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where branch_type='KGMS' and branch_name  like '%MVO%' and branch_code='" + branchcode + "'order by branch_name;";
                // select distinct tbl_mst_branch_configuration_id, branch_code, branch_name,branch_type from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where branch_type='KGMS' and branch_name like '%MVO%' order by branch_name;

                using (NpgsqlConnection con = new NpgsqlConnection(connectionstring))
                {
                    con.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(_query, con))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            SVOnameDTO svonameDTO = new SVOnameDTO();
                            {
                                svonameDTO.mvoname = dr["branch_name"];
                                svonameDTO.mvoid = dr["tbl_mst_branch_configuration_id"];
                            };

                            lstSVOnameDTO.Add(svonameDTO);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return lstSVOnameDTO;
        }




        public List<ModulesDTO> GetallRolesModules(string modulename, string globalSchema, string connectionString, string companyCode, string branchCode)
        {
            List<ModulesDTO> _ModulesDTODTOList = new List<ModulesDTO>();
            try
            {
                string Query = "select tbl_mst_modules_id,module_name from " + AddDoubleQuotes(globalSchema) + ".tbl_mst_modules where upper(parent_module_name)='" + modulename.ToUpper() + "' and company_code='" + companyCode + "' and branch_code='" + branchCode + "' order by tbl_mst_modules_id;";
                // select tbl_mst_modules_id,module_name from " + AddDoubleQuotes(globalSchema) + ".tbl_mst_modules where upper(parent_module_name)='" + modulename.ToUpper() + "' order by tbl_mst_modules_id;

                using (NpgsqlConnection con = new NpgsqlConnection(connectionString))
                {
                    con.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, con))
                    using (NpgsqlDataReader dataReader = cmd.ExecuteReader())
                    {
                        while (dataReader.Read())
                        {
                            ModulesDTO _RolemodulesDTO = new ModulesDTO
                            {
                                pModulename = Convert.ToString(dataReader["module_name"]),
                                pModuleId = Convert.ToInt64(dataReader["tbl_mst_modules_id"])
                            };

                            _ModulesDTODTOList.Add(_RolemodulesDTO);
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }

            return _ModulesDTODTOList;
        }


        public List<PartyDTO> GetPartyListbygroup(string ConnectionString, string GlobalSchema, string BranchSchema, string subledger, string CompanyCode, string BranchCode)
        {
            string strQuery = string.Empty;
            List<PartyDTO> partylist = new List<PartyDTO>();
            try
            {
                strQuery = "select distinct contact_id,contact_mailing_name from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_subscriber t1 join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact t2 on t1.contact_id=t2.tbl_mst_contact_id join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_chitgroup t3 on t1.chitgroup_id=t3.tbl_mst_chitgroup_id where companychit_ticketno!=ticketno and groupcode='" + subledger + "' and chit_status!='V' and t1.company_code='" + CompanyCode + "' and t1.branch_code='" + BranchCode + "'";

                using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
                {
                    con.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(strQuery, con))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            PartyDTO _PartyDTO = new PartyDTO();
                            _PartyDTO.ppartyid = Convert.ToInt64(dr["contact_id"]);
                            _PartyDTO.ppartyname = Convert.ToString(dr["contact_mailing_name"]);
                            partylist.Add(_PartyDTO);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return partylist;
        }

        #region GetBankNameDetails...

        public List<BankName> GetBankNameDetails(
            string connectionString,
            string globalSchema,
            string branchSchema,
            string BranchCode,
            string CompanyName)
        {
            List<BankName> bankList = new List<BankName>();

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

                    cmd.CommandText = $@"
                SELECT 
                    t1.tbl_mst_bank_configuration_id,

                    CASE 
                        WHEN (t1.account_number IS NULL OR t1.account_number = '') 
                        THEN COALESCE(t1.account_name,'') 
                        ELSE COALESCE(t1.account_name,'') || ' - ' || COALESCE(t1.account_number,'') 
                    END AS bank_name,

                    t1.bank_branch,
                    t1.ifsccode,
                    t1.account_type

                FROM {AddDoubleQuotes(branchSchema)}.tbl_mst_bank_configuration t1
                JOIN {AddDoubleQuotes(globalSchema)}.tbl_mst_bank t2
                    ON t1.bank_id = t2.tbl_mst_bank_id

                WHERE t1.status = true
                  AND t1.company_code = @CompanyName
                  AND t1.branch_code = @BranchCode

                ORDER BY bank_name;";

                    cmd.Parameters.AddWithValue("@CompanyName", CompanyName);
                    cmd.Parameters.AddWithValue("@BranchCode", BranchCode);

                    cmd.CommandType = CommandType.Text;

                    using var reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        BankName obj = new BankName();

                        obj.tbl_mst_bank_configuration_id =
                            reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetInt32(0));

                        obj.BankNames =
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
                throw new InvalidOperationException(
                    $"Failed to retrieve bank details (schema={globalSchema}). See inner exception for details.",
                    ex);
            }

            return bankList;
        }


        #endregion GetBankNameDetails...



        public List<BankDTO> GetBankntList(string ConnectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode)
        {
            List<BankDTO> banklist = new List<BankDTO>();
            try
            {
                using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
                {
                    con.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand("select t1.tbl_mst_bank_configuration_id,bank_account_id, case when (account_number is null or coalesce(account_number,'')='') then account_name else account_name||' - '|| account_number end as bankname,bank_branch,sum(coalesce(bankbookbalance,0)) as bankbookbalance,sum(coalesce(bankbookbalance,0))+sum(coalesce(passbookbalance,0)) passbookbalance,isprimary,isformanbank,is_foreman_payment_bank,is_interest_payment_bank from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t1 left join (select t1.tbl_mst_bank_configuration_id as recordid,coalesce(sum( coalesce(debitamount,0)-coalesce(creditamount,0)),0) as bankbookbalance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.bank_account_id=t2.parent_id group by t1.tbl_mst_bank_configuration_id)t2 on t1.tbl_mst_bank_configuration_id=t2.recordid left join (select deposited_bank_id,sum(passbookbalance)passbookbalance from (select bank_configuration_id as deposited_bank_id,paid_amount as passbookbalance  from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference  where clear_status ='N' union all select deposited_bank_id,-total_received_amount as passbookbalance from  " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference  where deposit_status  ='P' and clear_status='N' )x group by deposited_bank_id)t3 on t1.tbl_mst_bank_configuration_id=t3.deposited_bank_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank t4 on t1.bank_id=t4.tbl_mst_bank_id where t1.status=true and t1.company_code='" + CompanyCode + "' and t1.branch_code='" + BranchCode + "' group by t1.tbl_mst_bank_configuration_id,bank_name,case when (account_number is null or coalesce(account_number,'')='') then account_name else account_name||' - '|| account_number end,bank_account_id ,bank_branch,isprimary,isformanbank,is_foreman_payment_bank,is_interest_payment_bank order by bankname;", con))
                    // select t1.tbl_mst_bank_configuration_id,bank_account_id,case when (account_number is null or coalesce(account_number,'')='') then account_name else account_name||' - '|| account_number end as bankname,bank_branch,sum(coalesce(bankbookbalance,0)) as bankbookbalance,sum(coalesce(bankbookbalance,0))+sum(coalesce(passbookbalance,0)) passbookbalance,isprimary,isformanbank,is_foreman_payment_bank,is_interest_payment_bank from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t1 left join (select t1.tbl_mst_bank_configuration_id as recordid,coalesce(sum(coalesce(debitamount,0)-coalesce(creditamount,0)),0) as bankbookbalance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.bank_account_id=t2.parent_id group by t1.tbl_mst_bank_configuration_id)t2 on t1.tbl_mst_bank_configuration_id=t2.recordid left join (select deposited_bank_id,sum(passbookbalance)passbookbalance from (select bank_configuration_id as deposited_bank_id,paid_amount as passbookbalance from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference where clear_status='N' union all select deposited_bank_id,-total_received_amount as passbookbalance from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference where deposit_status='P' and clear_status='N')x group by deposited_bank_id)t3 on t1.tbl_mst_bank_configuration_id=t3.deposited_bank_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank t4 on t1.bank_id=t4.tbl_mst_bank_id where t1.status=true group by t1.tbl_mst_bank_configuration_id,bank_name,case when (account_number is null or coalesce(account_number,'')='') then account_name else account_name||' - '|| account_number end,bank_account_id,bank_branch,isprimary,isformanbank,is_foreman_payment_bank,is_interest_payment_bank order by bankname;
                    {
                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                BankDTO _BankDTO = new BankDTO();
                                _BankDTO.pdepositbankname = _BankDTO.pbankname = Convert.ToString(dr["bankname"]);
                                _BankDTO.pdepositbankid = _BankDTO.pbankid = Convert.ToInt64(dr["tbl_mst_bank_configuration_id"]);
                                _BankDTO.pbranchname = Convert.ToString(dr["bank_branch"]);
                                _BankDTO.pbankbalance = Convert.ToDecimal(dr["bankbookbalance"]);
                                _BankDTO.pbankpassbookbalance = Convert.ToDecimal(dr["passbookbalance"]);
                                _BankDTO.paccountid = Convert.ToString(dr["bank_account_id"]);
                                _BankDTO.pisprimary = Convert.ToBoolean(dr["isprimary"]);
                                _BankDTO.pisformanbank = Convert.ToBoolean(dr["isformanbank"]);
                                _BankDTO.isForemanPaymentBank = Convert.ToBoolean(dr["is_foreman_payment_bank"]);
                                _BankDTO.pisInterestPaymentBank = Convert.ToBoolean(dr["is_interest_payment_bank"]);
                                banklist.Add(_BankDTO);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return banklist;
        }


        public List<Modeoftransaction> GetModeoftransactions(string ConnectionString, string GlobalSchema, string CompanyCode, string BranchCode)
        {
            List<Modeoftransaction> Modeoftransactionlist = new List<Modeoftransaction>();
            try
            {
                using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
                {
                    con.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand("select tbl_mst_type_of_receiptspayments_id,mode_receiptspayments,type_of_receiptspayments,sub_type_of_receiptspayments,cheques_onhand_status,cheques_inbank_status from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_type_of_receiptspayments where status=true and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' order by mode_receiptspayments;", con))

                    //select tbl_mst_type_of_receiptspayments_id,mode_receiptspayments,type_of_receiptspayments,sub_type_of_receiptspayments,cheques_onhand_status,cheques_inbank_status from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_type_of_receiptspayments where status=true order by mode_receiptspayments;
                    {
                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                Modeoftransaction ModeoftransactionDTO = new Modeoftransaction
                                {
                                    pRecordid = Convert.ToInt64(dr["tbl_mst_type_of_receiptspayments_id"]),
                                    pmodofPayment = Convert.ToString(dr["mode_receiptspayments"]),
                                    pmodofreceipt = Convert.ToString(dr["mode_receiptspayments"]),
                                    ptranstype = Convert.ToString(dr["type_of_receiptspayments"]),
                                    ptypeofpayment = Convert.ToString(dr["sub_type_of_receiptspayments"]),
                                    pchqonhandstatus = Convert.ToString(dr["cheques_onhand_status"]),
                                    pchqinbankstatus = Convert.ToString(dr["cheques_inbank_status"])
                                };

                                Modeoftransactionlist.Add(ModeoftransactionDTO);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return Modeoftransactionlist;
        }

        public List<PartyDTO> GetPartyListGST(string ConnectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode)
        {
            string strQuery = string.Empty;
            List<PartyDTO> partylist = new List<PartyDTO>();
            try
            {
                strQuery = "select contact_id, name, contact_title_name, contact_reference_id, business_entity_contactno, business_entity_emailid, partyreftype, address1, area, city_name, district_name, state_name, state_code from((select x.contact_id as contact_id, name || '-' || state_short_code as name, contact_title_name, contact_reference_id,business_entity_contactno, business_entity_emailid, partyreftype, address1, area, city_name, district_name, state_name, state_code from(select tbl_mst_contact_id as contact_id, contact_mailing_name as name, contact_title_name, contact_reference_id, business_entity_contactno, business_entity_emailid, 'PARTY' as partyreftype  from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where is_supplier_applicable = true) x left join (select state_short_code, contact_id, address1, area, city_name, district_name, state_name, state_code from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact_address_details a, " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_district b, " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_state c where a.district_id = b.tbl_mst_district_id and b.state_id = c.tbl_mst_state_id and a.status = true and a.isprimary = true) y on x.contact_id = y.contact_id) )m where exists(select 1 from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_gstvoucher where vendor_id = m.contact_id and due_amount > 0 and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "')";
                //select contact_id,name,contact_title_name,contact_reference_id,business_entity_contactno,business_entity_emailid,partyreftype,address1,area,city_name,district_name,state_name,state_code from((select x.contact_id as contact_id,name||'-'||state_short_code as name,contact_title_name,contact_reference_id,business_entity_contactno,business_entity_emailid,partyreftype,address1,area,city_name,district_name,state_name,state_code from(select tbl_mst_contact_id as contact_id,contact_mailing_name as name,contact_title_name,contact_reference_id,business_entity_contactno,business_entity_emailid,'PARTY' as partyreftype from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where is_supplier_applicable=true) x left join(select state_short_code,contact_id,address1,area,city_name,district_name,state_name,state_code from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact_address_details a," + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_district b," + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_state c where a.district_id=b.tbl_mst_district_id and b.state_id=c.tbl_mst_state_id and a.status=true and a.isprimary=true) y on x.contact_id=y.contact_id)) m where exists(select 1 from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_gstvoucher where vendor_id=m.contact_id and due_amount>0)

                using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
                {
                    con.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(strQuery, con))
                    {
                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                PartyDTO _PartyDTO = new PartyDTO();
                                _PartyDTO.ppartyid = Convert.ToInt64(dr["contact_id"]);
                                _PartyDTO.ppartyname = Convert.ToString(dr["name"]);
                                _PartyDTO.ppartyreferenceid = Convert.ToString(dr["contact_reference_id"]);
                                _PartyDTO.ppartyreftype = Convert.ToString(dr["partyreftype"]);
                                _PartyDTO.ppartycontactno = Convert.ToString(dr["business_entity_contactno"]);
                                _PartyDTO.ppartyemailid = Convert.ToString(dr["business_entity_emailid"]);
                                _PartyDTO.ppartypannumber = Convert.ToString("");

                                // added below on 18.12.2023
                                _PartyDTO.address1 = Convert.ToString(dr["address1"]);
                                _PartyDTO.area = Convert.ToString(dr["area"]);
                                _PartyDTO.city_name = Convert.ToString(dr["city_name"]);
                                _PartyDTO.district_name = Convert.ToString(dr["district_name"]);
                                _PartyDTO.state_name = Convert.ToString(dr["state_name"]);
                                _PartyDTO.state_code = Convert.ToString(dr["state_code"]);

                                partylist.Add(_PartyDTO);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return partylist;
        }

        public List<PartyDTO> GetPartyList(string ConnectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode)
        {
            string strQuery = string.Empty;
            List<PartyDTO> partylist = new List<PartyDTO>();
            try
            {
                strQuery = "select contact_id,case when coalesce(panno,'')='' then  name else name||'-'||coalesce(panno) end as name,contact_title_name,contact_reference_id,business_entity_contactno,business_entity_emailid,partyreftype,address1,area,city_name,district_name,state_name,state_code,panno,aadharno,gstno from (select contact_id, name, contact_title_name, contact_reference_id, business_entity_contactno, business_entity_emailid, partyreftype, address1, area, city_name, district_name, state_name, state_code, panno, aadharno from(select contact_id, name, contact_title_name, contact_reference_id, business_entity_contactno, business_entity_emailid, partyreftype, address1, area, city_name, district_name, state_name, state_code, panno from(select x.contact_id as contact_id, name || '-' || state_short_code as name, contact_title_name, contact_reference_id, business_entity_contactno, business_entity_emailid, partyreftype, address1, area, city_name, district_name, state_name, state_code from(select tbl_mst_contact_id as contact_id, contact_mailing_name as name, contact_title_name, contact_reference_id, business_entity_contactno, business_entity_emailid, 'PARTY' as partyreftype  from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where is_supplier_applicable = true) x left join(select state_short_code, contact_id, address1, area, city_name, district_name, state_name, state_code from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact_address_details a, " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_district b, " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_state c where a.district_id = b.tbl_mst_district_id and b.state_id = c.tbl_mst_state_id and a.status = true and company_code ='" + CompanyCode + "' and branch_code='" + BranchCode + "' and a.isprimary = true) y on x.contact_id = y.contact_id) z left join (select coalesce(kycno, '') as PANNO, contact_id as contactid from " + AddDoubleQuotes(GlobalSchema) + ".vw_contact_kyc where document_name = 'Pan Card') t on z.contact_id = t.contactid) t1 left join (select coalesce(kycno, '') as AADHARNO, contact_id as contactid from " + AddDoubleQuotes(GlobalSchema) + ".vw_contact_kyc where document_name = 'Aadhar Card') t2 on t1.contact_id = t2.contactid ) t3 left join (select coalesce(kycno, '') as GSTNO, contact_id as contactid from " + AddDoubleQuotes(GlobalSchema) + ".vw_contact_kyc where document_name = 'Gst Number') t4 on t3.contact_id = t4.contactid order by name";
                // select contact_id,case when coalesce(panno,'')='' then name else name||'-'||coalesce(panno) end as name,contact_title_name,contact_reference_id,business_entity_contactno,business_entity_emailid,partyreftype,address1,area,city_name,district_name,state_name,state_code,panno,aadharno,gstno from (select contact_id,name,contact_title_name,contact_reference_id,business_entity_contactno,business_entity_emailid,partyreftype,address1,area,city_name,district_name,state_name,state_code,panno,aadharno from(select contact_id,name,contact_title_name,contact_reference_id,business_entity_contactno,business_entity_emailid,partyreftype,address1,area,city_name,district_name,state_name,state_code,panno from(select x.contact_id as contact_id,name||'-'||state_short_code as name,contact_title_name,contact_reference_id,business_entity_contactno,business_entity_emailid,partyreftype,address1,area,city_name,district_name,state_name,state_code from(select tbl_mst_contact_id as contact_id,contact_mailing_name as name,contact_title_name,contact_reference_id,business_entity_contactno,business_entity_emailid,'PARTY' as partyreftype from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where is_supplier_applicable=true) x left join(select state_short_code,contact_id,address1,area,city_name,district_name,state_name,state_code from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact_address_details a," + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_district b," + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_state c where a.district_id=b.tbl_mst_district_id and b.state_id=c.tbl_mst_state_id and a.status=true and a.isprimary=true) y on x.contact_id=y.contact_id) z left join(select coalesce(kycno,'') as PANNO,contact_id as contactid from " + AddDoubleQuotes(GlobalSchema) + ".vw_contact_kyc where document_name='Pan Card') t on z.contact_id=t.contactid) t1 left join(select coalesce(kycno,'') as AADHARNO,contact_id as contactid from " + AddDoubleQuotes(GlobalSchema) + ".vw_contact_kyc where document_name='Aadhar Card') t2 on t1.contact_id=t2.contactid) t3 left join(select coalesce(kycno,'') as GSTNO,contact_id as contactid from " + AddDoubleQuotes(GlobalSchema) + ".vw_contact_kyc where document_name='Gst Number') t4 on t3.contact_id=t4.contactid order by name

                using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
                {
                    con.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(strQuery, con))
                    {
                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                PartyDTO _PartyDTO = new PartyDTO();
                                _PartyDTO.ppartyid = Convert.ToInt64(dr["contact_id"]);
                                _PartyDTO.ppartyname = Convert.ToString(dr["name"]);
                                _PartyDTO.ppartyreferenceid = Convert.ToString(dr["contact_reference_id"]);
                                _PartyDTO.ppartyreftype = Convert.ToString(dr["partyreftype"]);
                                _PartyDTO.ppartycontactno = Convert.ToString(dr["business_entity_contactno"]);
                                _PartyDTO.ppartyemailid = Convert.ToString(dr["business_entity_emailid"]);
                                _PartyDTO.ppartypannumber = Convert.ToString("");

                                // added below on 18.12.2023
                                _PartyDTO.address1 = Convert.ToString(dr["address1"]);
                                _PartyDTO.area = Convert.ToString(dr["area"]);
                                _PartyDTO.city_name = Convert.ToString(dr["city_name"]);
                                _PartyDTO.district_name = Convert.ToString(dr["district_name"]);
                                _PartyDTO.state_name = Convert.ToString(dr["state_name"]);
                                _PartyDTO.state_code = Convert.ToString(dr["state_code"]);
                                _PartyDTO.pan_no = Convert.ToString(dr["panno"]);
                                _PartyDTO.aadharno = Convert.ToString(dr["aadharno"]);
                                _PartyDTO.gstno = Convert.ToString(dr["gstno"]);

                                partylist.Add(_PartyDTO);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return partylist;
        }

        public List<GstDTo> GetGstPercentages(string ConnectionString, string GlobalSchema, string CompanyCode, string BranchCode, string TaxesSchema)
        {
            List<GstDTo> GstList = new List<GstDTo>();
            try
            {
                using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
                {
                    con.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand("select tbl_mst_gst_id as recordid, coalesce(gst_percentage,0) gstpercentage,coalesce( cgst_percentage,0)cgstpercentage,coalesce( sgst_percentage,0)sgstpercentage,coalesce( utgst_percentage,0) utgstpercentage from " + AddDoubleQuotes(TaxesSchema) + ".tbl_mst_gst where status=true and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' order by gst_percentage;", con))
                    // select tbl_mst_gst_id as recordid,coalesce(gst_percentage,0) gstpercentage,coalesce(cgst_percentage,0) cgstpercentage,coalesce(sgst_percentage,0) sgstpercentage,coalesce(utgst_percentage,0) utgstpercentage from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_gst where status=true order by gst_percentage;
                    {
                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                GstDTo GstDTO = new GstDTo
                                {
                                    pRecordId = Convert.ToInt64(dr["recordid"]),
                                    pgstpercentage = Convert.ToDecimal(dr["gstpercentage"]),
                                    pigstpercentage = Convert.ToDecimal(dr["gstpercentage"]),
                                    psgstpercentage = Convert.ToDecimal(dr["cgstpercentage"]),
                                    pcgstpercentage = Convert.ToDecimal(dr["sgstpercentage"]),
                                    putgstpercentage = Convert.ToDecimal(dr["utgstpercentage"])
                                };

                                GstList.Add(GstDTO);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return GstList;
        }


        public List<BankDTO> GetDebitCardNumbers(
          string ConnectionString,
          string GlobalSchema,
          string BranchSchema,
          string CompanyCode,
          string BranchCode)
        {
            List<BankDTO> bankdebitcardslist = new List<BankDTO>();
            string query = "";

            try
            {
                query = "select t1.tbl_mst_bank_configuration_id,t2.bank_name,t1.debitcard_number from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t1 join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank t2 on t1.bank_id=t2.tbl_mst_bank_id where t1.status=true and t2.company_code='" + CompanyCode + "' and t2.branch_code='" + BranchCode + "' and t1.debitcard_number is not null order by t1.debitcard_number";

                using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
                {
                    con.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(query, con))
                    {
                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                BankDTO _BankDebitCardsDTO = new BankDTO
                                {
                                    pCardNumber = Convert.ToString(dr["debitcard_number"]),
                                    pbankname = Convert.ToString(dr["bank_name"]),
                                    pbankid = Convert.ToInt64(dr["tbl_mst_bank_configuration_id"]),
                                };

                                bankdebitcardslist.Add(_BankDebitCardsDTO);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return bankdebitcardslist;
        }





        public decimal getcashbalance(string ConnectionString, string BranchSchema, string CompanyCode, string BranchCode)
        {
            decimal accountbalance = 0;
            List<TdsSectionDTO> lstTdsSectionDetails = new List<TdsSectionDTO>();
            string query = "";
            try
            {
                query = "select coalesce(sum(balance),0) from (select coalesce(account_balance,0)balance from   " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account     where  account_name='CASH ON HAND' and chracc_type='2' and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'union all select -sum(a.total_received_amount) from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_interbranch_receipt a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_generalreceipt b on a.general_receipt_number=b.receipt_number where a.modeof_receipt='C' AND deposited_status='N' and b.receipt_cancel_reference_number is null)x;";
                // select coalesce(sum(balance),0) from (select coalesce(account_balance,0) balance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where account_name='CASH ON HAND' and chracc_type='2' union all select -sum(a.total_received_amount) from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_interbranch_receipt a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_generalreceipt b on a.general_receipt_number=b.receipt_number where a.modeof_receipt='C' AND deposited_status='N' and b.receipt_cancel_reference_number is null)x;

                using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
                {
                    con.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(query, con))
                    {
                        object result = cmd.ExecuteScalar();
                        accountbalance = Convert.ToDecimal(result);
                    }
                }
            }
            catch (Exception ex)
            {
                {
                }
                throw ex;
            }

            return accountbalance;
        }


        public decimal GetBankBalance(long recordid, string con, string BranchSchema, string CompanyCode, string BranchCode)
        {
            try
            {
                string query = "select coalesce(sum(account_balance),0) as balance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where exists (select 1 from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where bank_account_id=account_id and case when " + recordid + "=0 then tbl_mst_bank_configuration_id>0 else tbl_mst_bank_configuration_id=" + recordid + " end) and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "';";
                //select coalesce(sum(account_balance),0) as balance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account where exists (select 1 from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where bank_account_id=account_id and case when " + recordid + "=0 then tbl_mst_bank_configuration_id>0 else tbl_mst_bank_configuration_id=" + recordid + " end);

                using (NpgsqlConnection connection = new NpgsqlConnection(con))
                {
                    connection.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(query, connection))
                    {
                        object result = cmd.ExecuteScalar();
                        recordid = Convert.ToInt64(result);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return recordid;
        }


        public decimal GetCashRestrictAmount(string type, string con, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode)
        {
            decimal result = 0;
            try
            {
                string query = "";

                if (type == "PAYMENT VOUCHER")
                {
                    query = "select coalesce(cash_payment,0) from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_chit_company_configuration a join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration b on a.tbl_mst_chit_company_configuration_id=b.company_configuration_id and branch_code='" + BranchCode + "'";
                    // select coalesce(cash_payment,0) from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_chit_company_configuration a join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration b on a.tbl_mst_chit_company_configuration_id=b.company_configuration_id and branch_code='" + BranchSchema + "'
                }
                else if (type == "GENERAL RECEIPT")
                {
                    query = "select coalesce(cash_receipt,0) from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_chit_company_configuration a join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration b on a.tbl_mst_chit_company_configuration_id=b.company_configuration_id and branch_code='" + BranchCode + "'";
                    // select coalesce(cash_receipt,0) from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_chit_company_configuration a join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration b on a.tbl_mst_chit_company_configuration_id=b.company_configuration_id and branch_code='" + BranchSchema + "'
                }
                else
                {
                    result = 0;
                }

                if (!string.IsNullOrEmpty(query))
                {
                    using (NpgsqlConnection connection = new NpgsqlConnection(con))
                    {
                        connection.Open();

                        using (NpgsqlCommand cmd = new NpgsqlCommand(query, connection))
                        {
                            object obj = cmd.ExecuteScalar();
                            result = Convert.ToDecimal(obj);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return result;
        }





        public List<PaymentVoucherReportDTO> GetPaymentVoucherReportData(
                string paymentId,
                string LocalSchema,
                string GlobalSchema,
                string Connectionstring,
                string CompanyCode,
                string BranchCode)
        {
            List<PaymentVoucherReportDTO> PaymentVoucherReportlist = new List<PaymentVoucherReportDTO>();

            try
            {
                using (NpgsqlConnection conn = new NpgsqlConnection(Connectionstring))
                {
                    conn.Open();

                    string query = "select distinct debit_account_id, tr.payment_number, tr.payment_date::text, case when (select count(1) from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_gen_receipt_cancel where payment_id=tr.tbl_trans_payment_voucher_id)=1 then narration || ' Posted By:' || (select coalesce(b.contact_mailing_name, '') from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_gen_receipt_cancel_log a, " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact b where a.user_id = b.tbl_mst_contact_id and payment_id = tr.tbl_trans_payment_voucher_id and activity_type = 'C') || ' Apporved By:' || (select coalesce(b.contact_mailing_name, '') from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_gen_receipt_cancel a, " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact b where a.authorized_contact_id = b.tbl_mst_contact_id and payment_id = tr.tbl_trans_payment_voucher_id) else narration end as narration, coalesce(contact_id,0) contact_id, coalesce(t1.account_name,(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=contact_id)) contactname, tr.modeof_payment, reference_number, trr.trans_type, (select contact_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=coalesce(employee_id,0)) employeename, bc.account_name||'-'||account_number as bank_account, coalesce(interbranch_id,0) as interbranch_id from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher tr join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_voucher_details trd on tr.tbl_trans_payment_voucher_id=trd.payment_voucher_id left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_reference trr on trr.payment_number=tr.payment_number left join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_bank_configuration bc on bc.tbl_mst_bank_configuration_id=trr.bank_configuration_id join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account t1 on trd.debit_account_id=t1.account_id where tr.payment_number='" + paymentId + "' and t1.company_code='" + CompanyCode + "' and t1.branch_code='" + BranchCode + "';";

                    using (NpgsqlCommand cmd = new NpgsqlCommand(query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            PaymentVoucherReportDTO pPaymentVoucherReportDTO = new PaymentVoucherReportDTO
                            {
                                ppaymentdate = dr["payment_date"],
                                ppaymentid = Convert.ToString(dr["payment_number"]),
                                pcontactid = Convert.ToString(dr["contact_id"]),
                                pcontactname = Convert.ToString(dr["contactname"]),
                                pnarration = Convert.ToString(dr["narration"]),
                                pmodofPayment = PayModes(Convert.ToString(dr["modeof_payment"]), "D"),
                                pChequenumber = Convert.ToString(dr["reference_number"]),
                                ptypeofpayment = PayModes(Convert.ToString(dr["trans_type"]), "D"),
                                pemployeename = Convert.ToString(dr["employeename"]),
                                pbankaccount = dr["bank_account"],
                                ppaymentslist = GetPaymentVoucherDetailsReportData(
                                    paymentId,
                                    dr["contact_id"],
                                    Connectionstring,
                                    LocalSchema,
                                    GlobalSchema,
                                    dr["debit_account_id"],
                                    dr["interbranch_id"],
                                    BranchCode,
                                    CompanyCode)
                            };

                            PaymentVoucherReportlist.Add(pPaymentVoucherReportDTO);
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }

            return PaymentVoucherReportlist;
        }



        public List<PaymentVoucherReportDTO> GetPettyCashReportData(string paymentId, string LocalSchema, string GlobalSchema, string Connectionstring, string CompanyCode,
                       string BranchCode)
        {
            List<PaymentVoucherReportDTO> PaymentVoucherReportlist = new List<PaymentVoucherReportDTO>();

            try
            {
                string Query = "select distinct tr.payment_number,tr.payment_date::text,narration,coalesce(contact_id,0) contact_id,coalesce((select account_name from " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account where account_id=t1.parent_id)||'-'||(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=contact_id)) contactname,(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=contact_id) contactname,tr.modeof_payment,reference_number,trr.trans_type,(select contact_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=employee_id) employeename from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_pettycash_voucher tr join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_pettycash_voucher_details trd on tr.tbl_trans_pettycash_voucher_id=trd.payment_voucher_id left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_reference trr on trr.payment_number=tr.payment_number join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account t1 on trd.debit_account_id=t1.account_id where tr.payment_number='" + paymentId + "' and t1.company_code='" + CompanyCode + "' and t1.branch_code='" + BranchCode + "';";

                //select distinct tr.payment_number,tr.payment_date::text,narration,coalesce(contact_id,0)contact_id,coalesce((select account_name from " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account where account_id=t1.parent_id)||'-'||(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=contact_id))contactname,(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=contact_id)contactname,tr.modeof_payment,reference_number,trr.trans_type,(select contact_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=employee_id)employeename from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_pettycash_voucher tr join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_pettycash_voucher_details trd on tr.tbl_trans_pettycash_voucher_id = trd.payment_voucher_id left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_payment_reference trr on trr.payment_number=tr.payment_number join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account t1 on trd.debit_account_id=t1.account_id where tr.payment_number='" + paymentId + "';

                using (NpgsqlConnection conn = new NpgsqlConnection(Connectionstring))
                {
                    conn.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            PaymentVoucherReportDTO pPaymentVoucherReportDTO = new PaymentVoucherReportDTO
                            {
                                ppaymentdate = dr["payment_date"],
                                ppaymentid = Convert.ToString(dr["payment_number"]),
                                pcontactid = Convert.ToString(dr["contact_id"]),
                                pcontactname = Convert.ToString(dr["contactname"]),
                                pnarration = Convert.ToString(dr["narration"]),
                                pmodofPayment = PayModes(Convert.ToString(dr["modeof_payment"]), "D"),
                                pChequenumber = Convert.ToString(dr["reference_number"]),
                                ptypeofpayment = PayModes(Convert.ToString(dr["trans_type"]), "D"),
                                pemployeename = Convert.ToString(dr["employeename"]),
                                ppaymentslist = GetPettyCashDetailsReportData(paymentId, dr["contact_id"], Connectionstring, LocalSchema, GlobalSchema, BranchCode, CompanyCode)
                            };

                            PaymentVoucherReportlist.Add(pPaymentVoucherReportDTO);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return PaymentVoucherReportlist;
        }

        public List<GeneralReceiptSubDetails> GetPettyCashDetailsReportData(
    string paymentId,
    object contact_id,
    string Connectionstring,
    string LocalSchema,
    string GlobalSchema,
    string BranchCode, string CompanyCode)
        {
            List<GeneralReceiptSubDetails> GeneralReceiptlist = new List<GeneralReceiptSubDetails>();

            try
            {
                using (NpgsqlConnection con = new NpgsqlConnection(Connectionstring))
                {
                    con.Open();

                    // ✅ QUERY IN ONE LINE
                    string query = "SELECT contactname,tbl_mst_contact_id, accountname, SUM(ledgeramount) ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount FROM (select contact_mailing_name as contactname,c.tbl_mst_contact_id,case when chracc_type = '2' then account_name  else account_name end accountname,(coalesce(ledger_amount, 0)+coalesce(tds_amount,0)) ledgeramount,gst_calculation_type,gst_amount,tac.tds_calculation_type,tds_amount from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_pettycash_voucher_details tr join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account tac on tac.account_id = tr.debit_account_id  join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact c on c.tbl_mst_contact_id = tr.contact_id where payment_voucher_id in(select tbl_trans_pettycash_voucher_id  from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_pettycash_voucher where payment_number = '" + paymentId + "' AND company_code = '" + CompanyCode + "' and branch_code = '" + BranchCode + "'))X group by contactname, tbl_mst_contact_id,accountname, gst_calculation_type, gst_amount, tds_calculation_type, tds_amount";
                    // SELECT contactname,tbl_mst_contact_id,accountname,SUM(ledgeramount) ledgeramount,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount FROM (SELECT contact_mailing_name AS contactname,c.tbl_mst_contact_id,CASE WHEN chracc_type='2' THEN account_name ELSE account_name END accountname,(COALESCE(ledger_amount,0)+COALESCE(tds_amount,0)) ledgeramount,gst_calculation_type,gst_amount,tac.tds_calculation_type,tds_amount FROM " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_pettycash_voucher_details tr JOIN " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account tac ON tac.account_id=tr.debit_account_id JOIN " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact c ON c.tbl_mst_contact_id=tr.contact_id WHERE payment_voucher_id IN (SELECT tbl_trans_pettycash_voucher_id FROM " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_pettycash_voucher WHERE payment_number=@paymentId)) X GROUP BY contactname,tbl_mst_contact_id,accountname,gst_calculation_type,gst_amount,tds_calculation_type,tds_amount

                    using (NpgsqlCommand cmd = new NpgsqlCommand(query, con))
                    {
                        // ✅ Parameterized (VERY IMPORTANT)
                        cmd.Parameters.AddWithValue("@paymentId", paymentId);

                        using (NpgsqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                GeneralReceiptSubDetails _GeneralReceipt = new GeneralReceiptSubDetails();

                                _GeneralReceipt.pLedgeramount = Convert.ToDecimal(dr["ledgeramount"]);
                                _GeneralReceipt.pAccountname = Convert.ToString(dr["accountname"]);
                                _GeneralReceipt.pgstcalculationtype = Convert.ToString(dr["gst_calculation_type"]);

                                if (dr["gst_amount"] != DBNull.Value)
                                    _GeneralReceipt.pcgstamount = Convert.ToDecimal(dr["gst_amount"]);

                                _GeneralReceipt.ptdscalculationtype = Convert.ToString(dr["tds_calculation_type"]);

                                if (dr["tds_amount"] != DBNull.Value)
                                    _GeneralReceipt.ptdsamount = Convert.ToDecimal(dr["tds_amount"]);

                                if (dr["tbl_mst_contact_id"] != DBNull.Value)
                                {
                                    string stateQuery = "select t3.state_code from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact_address_details t1 join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_district t2 on t1.district_id = t2.tbl_mst_district_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_state t3 on t2.state_id = t3.tbl_mst_state_id where contact_id =" + Convert.ToString(dr["tbl_mst_contact_id"]) + " and t1.status=true and t1.isprimary=true AND t1.company_code='" + CompanyCode + "' and t1.branch_code='" + BranchCode + "'";
                                    // SELECT t3.state_code FROM " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact_address_details t1 JOIN " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_district t2 ON t1.district_id=t2.tbl_mst_district_id JOIN " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_state t3 ON t2.state_id=t3.tbl_mst_state_id WHERE contact_id=@contactId AND t1.status=true AND t1.isprimary=true";

                                    // ✅ Use new connection (fixes reader conflict)
                                    using (NpgsqlConnection con2 = new NpgsqlConnection(Connectionstring))
                                    {
                                        con2.Open();

                                        using (NpgsqlCommand stateCmd = new NpgsqlCommand(stateQuery, con2))
                                        {
                                            stateCmd.Parameters.AddWithValue("@contactId",
                                                Convert.ToInt32(dr["tbl_mst_contact_id"]));

                                            object stateResult = stateCmd.ExecuteScalar();
                                            _GeneralReceipt.state_code =
                                                stateResult == null ? "" : Convert.ToString(stateResult);
                                        }
                                    }
                                }

                                GeneralReceiptlist.Add(_GeneralReceipt);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }

            return GeneralReceiptlist;
        }



        public List<AccountReportsDTO> GetAccountLedgerDetails(string con, string fromDate, string toDate,
            long pAccountId, long pSubAccountId, string BranchSchema, string GlobalSchema,
            string BranchCode, string CompanyCode)
        {
            string Query = string.Empty;
            string pQuery = string.Empty;

            List<AccountReportsDTO> lstcashbook = new List<AccountReportsDTO>();

            try
            {
                if (pSubAccountId > 0)
                {
                    Query = "select distinct accounttype,account_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions WHERE account_id=" + pSubAccountId + " and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "';";

                    using (NpgsqlConnection connection = new NpgsqlConnection(con))
                    {
                        connection.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand(Query, connection))
                        {
                            object result = cmd.ExecuteScalar();
                            long accounttype = result == null ? 0 : Convert.ToInt64(result);

                            if (accounttype == 5)
                                pAccountId = pSubAccountId;
                        }
                    }

                    pQuery = " and account_id=" + pSubAccountId + " ";
                }

                Query = "select rownumber as recordid,parent_id,account_id,null as formname,transaction_date::text,transaction_no,PARTICULARS,narration,DEBITAMOUNT,abs(CREDITAMOUNT) as CREDITAMOUNT,abs(balance) as balance,case when balance>0 then 'Dr' else 'Cr' end as balancetype from (select row_number() over (order by transaction_date,RECORDID) as rownumber,*,sum(DEBITAMOUNT+CREDITAMOUNT) OVER(ORDER BY transaction_date,RECORDID) as BALANCE from (SELECT 0 AS RECORDID,'' as formname,0 parent_id,0 account_id,CAST('" + FormatDate(fromDate) + "' AS DATE) AS transaction_date,'0' AS TRANSACTION_NO,'Opening Balance' AS PARTICULARS,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)>0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END AS DEBITAMOUNT,CASE WHEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0)<0 THEN COALESCE(SUM(DEBITAMOUNT)-SUM(CREDITAMOUNT),0) ELSE 0 END AS CREDITAMOUNT,'' narration FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions WHERE transaction_date < '" + FormatDate(fromDate) + "' and parent_id = " + pAccountId + " and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'";

                Query = Query + pQuery + " union all select distinct x.RECORDID,null as formname,parent_id,account_id,transaction_date,TRANSACTION_NO,PARTICULARS,COALESCE(DEBITAMOUNT,0.00) as DEBITAMOUNT,COALESCE(CREDITAMOUNT,0.00) as CREDITAMOUNT,narration from (SELECT tbl_trans_total_transactions_id as RECORDID,parent_id,account_id,transaction_date,TRANSACTION_NO,PARTICULARS,COALESCE(DEBITAMOUNT,0.00) as DEBITAMOUNT,-COALESCE(CREDITAMOUNT,0.00) as CREDITAMOUNT,narration,split_part(REGEXP_REPLACE(TRANSACTION_NO,'[[:digit:]]','','g'),'/',1) as code FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions WHERE transaction_date BETWEEN '" + FormatDate(fromDate) + "' AND '" + FormatDate(toDate) + "' and parent_id = " + pAccountId + " and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "'";

                // Query = Query + pQuery + " AND (debitamount<>0 or creditamount<>0)) x join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_generate_id y on x.code=y.code) as D order by transaction_date,RECORDID ) x order by RECORDID;";
                Query = Query + " AND (debitamount<>0 or creditamount<>0)) x join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_generate_id y on x.code=y.code) as D order by transaction_date,RECORDID ) x order by RECORDID;";


                using (NpgsqlConnection connection = new NpgsqlConnection(con))
                {
                    connection.Open();

                    using (NpgsqlCommand cmd = new NpgsqlCommand(Query, connection))
                    using (NpgsqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            AccountReportsDTO _ObjBank = new AccountReportsDTO();

                            _ObjBank.precordid = Convert.ToInt64(dr["RECORDID"]);
                            _ObjBank.pparentid = Convert.ToInt64(dr["parent_id"]);
                            _ObjBank.paccountid = Convert.ToInt64(dr["account_id"]);
                            _ObjBank.pFormName = Convert.ToString(dr["formname"]);
                            _ObjBank.ptransactiondate = dr["transaction_date"];
                            _ObjBank.pdebitamount = Convert.ToDouble(dr["DEBITAMOUNT"]);
                            _ObjBank.pcreditamount = Convert.ToDouble(dr["CREDITAMOUNT"]);
                            _ObjBank.pparticulars = Convert.ToString(dr["PARTICULARS"]);
                            _ObjBank.pdescription = Convert.ToString(dr["narration"]);
                            _ObjBank.ptransactionno = Convert.ToString(dr["TRANSACTION_NO"]);
                            _ObjBank.popeningbal = Convert.ToDouble(dr["BALANCE"]);
                            _ObjBank.pBalanceType = Convert.ToString(dr["balancetype"]);

                            lstcashbook.Add(_ObjBank);
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }

            return lstcashbook;
        }




public List<PaymentVoucherReportDTO> GetChitReceiptCancelReportData(string paymentId, string LocalSchema, string GlobalSchema, string Connectionstring,string branchCode,string companyCode)
{
    List<PaymentVoucherReportDTO> PaymentVoucherReportlist = new List<PaymentVoucherReportDTO>();
    string Query = string.Empty;

    try
    {
        Query = "select x.*,y.user_id,(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=coalesce(y.user_id,0)) as postedby from(select distinct tcc.transaction_no,tcc.transaction_date::text,cancellation_reason,coalesce(tcc.contact_id,0)contact_id,(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=tcc.contact_id)contactname,tcc.modeof_receipt,(select contact_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=coalesce(tcc.employee_id,0))employeename,coalesce(b.bank_name,'')bank_name,coalesce(rr.trans_type,'')trans_type,coalesce(rr.receipt_branch_name,'')receipt_branch_name,coalesce(rr.reference_number,'')reference_number,coalesce(to_char(rr.cheque_date, 'YYYY-MM-DD'),'')cheque_date,chit_receipt_number from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_chit_receipt_cancel tcc join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_chit_receipt tc on tc.tbl_trans_chit_receipt_id=tcc.chit_receipt_id left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_receipt_reference rr on cast(tc.comman_receipt_number as character varying)=rr.receipt_number left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank b on b.tbl_mst_bank_id=rr.receipt_bank_id where tcc.transaction_no='" + paymentId + "' and tc.company_code='" + companyCode + "' and tc.branch_code='" + branchCode + "') x left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_chit_receipt_cancel_log y on x.transaction_no = y.transaction_no and y.activity_type = 'I';";
       // select x.*,y.user_id,(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=coalesce(y.user_id,0)) as postedby from (select distinct tcc.transaction_no,tcc.transaction_date::text,cancellation_reason,coalesce(tcc.contact_id,0)contact_id,(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=tcc.contact_id)contactname,tcc.modeof_receipt,(select contact_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id=coalesce(tcc.employee_id,0))employeename,coalesce(b.bank_name,'')bank_name,coalesce(rr.trans_type,'')trans_type,coalesce(rr.receipt_branch_name,'')receipt_branch_name,coalesce(rr.reference_number,'')reference_number,coalesce(to_char(rr.cheque_date,'YYYY-MM-DD'),'')cheque_date,chit_receipt_number from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_chit_receipt_cancel tcc join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_chit_receipt tc on tc.tbl_trans_chit_receipt_id=tcc.chit_receipt_id left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_receipt_reference rr on cast(tc.comman_receipt_number as character varying)=rr.receipt_number left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank b on b.tbl_mst_bank_id=rr.receipt_bank_id where tcc.transaction_no='" + paymentId + "') x left join " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_chit_receipt_cancel_log y on x.transaction_no=y.transaction_no and y.activity_type='I';

        using (NpgsqlConnection connection = new NpgsqlConnection(Connectionstring))
        {
            connection.Open();

            using (NpgsqlCommand cmd = new NpgsqlCommand(Query, connection))
            using (NpgsqlDataReader dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                  PaymentVoucherReportDTO  pPaymentVoucherReportDTO = new PaymentVoucherReportDTO
                    {
                        ppaymentdate = dr["transaction_date"],
                        ppaymentid = Convert.ToString(dr["transaction_no"]),
                        pcontactid = Convert.ToString(dr["contact_id"]),
                        pcontactname = Convert.ToString(dr["contactname"]),
                        pnarration = Convert.ToString(dr["cancellation_reason"]),
                        pmodofPayment = Convert.ToString(dr["modeof_receipt"]),
                        pChequenumber = Convert.ToString(dr["reference_number"]),
                        ptypeofpayment = PayModes(Convert.ToString(dr["trans_type"]), "D"),
                        pemployeename = Convert.ToString(dr["employeename"]),
                        pusername = Convert.ToString(dr["postedby"]),
                        pbankaccount = dr["bank_name"],
                        preceiptno = dr["chit_receipt_number"],
                        ppaymentslist = GetChitReceiptCancelDetailsReportData(paymentId, dr["contact_id"], Connectionstring, LocalSchema, GlobalSchema,branchCode,companyCode)
                    };

                    PaymentVoucherReportlist.Add(pPaymentVoucherReportDTO);
                }
            }
        }
    }
    catch (Exception ex)
    {
        throw ex;
    }

    return PaymentVoucherReportlist;
}


public List<GeneralReceiptSubDetails> GetChitReceiptCancelDetailsReportData(string paymentId, object contact_id, string Connectionstring, string LocalSchema, string GlobalSchema,string branchCode,string companyCode)
{
    string Query = string.Empty;
   List<GeneralReceiptSubDetails> GeneralReceiptlist = new List<GeneralReceiptSubDetails>();

    try
    {
        Query = "SELECT contactname, accountname, SUM(receipt_amount)receipt_amount FROM (select contact_mailing_name as contactname,case when chracc_type = '2' then account_name when chracc_type = '3' then (select parentaccountname||'('||accountname||')' from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_total_transactions where accounttype='3' and transaction_no='" + paymentId + "')  else account_name end accountname,(coalesce(receipt_amount, 0)) receipt_amount,gst_calculation_type,tac.tds_calculation_type from  " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_chit_receipt_cancel tr join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account tac on tac.account_id = tr.account_id  join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact c on c.tbl_mst_contact_id = tr.contact_id where transaction_no='" + paymentId + "' and tr.contact_id=" + contact_id + " and c.company_code='" + companyCode + "' and c.branch_code='" + branchCode + "')X group by contactname, accountname";
       // SELECT contactname,accountname,SUM(receipt_amount)receipt_amount FROM (select contact_mailing_name as contactname,case when chracc_type='2' then account_name when chracc_type='3' then (select parentaccountname||'('||accountname||')' from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_total_transactions where accounttype='3' and transaction_no='" + paymentId + "') else account_name end accountname,(coalesce(receipt_amount,0)) receipt_amount,gst_calculation_type,tac.tds_calculation_type from " + AddDoubleQuotes(LocalSchema) + ".tbl_trans_chit_receipt_cancel tr join " + AddDoubleQuotes(LocalSchema) + ".tbl_mst_account tac on tac.account_id=tr.account_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact c on c.tbl_mst_contact_id=tr.contact_id where transaction_no='" + paymentId + "' and tr.contact_id=" + contact_id + ")X group by contactname,accountname

        using (NpgsqlConnection connection = new NpgsqlConnection(Connectionstring))
        {
            connection.Open();

            using (NpgsqlCommand cmd = new NpgsqlCommand(Query, connection))
            using (NpgsqlDataReader dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                   GeneralReceiptSubDetails _GeneralReceipt = new GeneralReceiptSubDetails();
                    _GeneralReceipt.pLedgeramount = Convert.ToDecimal(dr["receipt_amount"]);
                    _GeneralReceipt.pAccountname = Convert.ToString(dr["accountname"]);

                   

                    GeneralReceiptlist.Add(_GeneralReceipt);
                }
            }
        }
    }
    catch (Exception ex)
    {
        throw ex;
    }

    return GeneralReceiptlist;
}



               public List<string> GetCheckDuplicateDebitCardNo(string ConnectionString, string GlobalSchema, BankInformationDTO lstBankInformation,string companycode,string branchcode)
        {
           Int64 DebtCardCount = 0;
           Int64 UPIIdCount = 0;
           Int64 bankcount = 0;
           object BranchSchema;
           List<string> lstdata = new List<string>();

           try
           {
               if (lstBankInformation != null)
               {
                   BranchSchema = lstBankInformation.branchSchema;

                   using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
                   {
                       con.Open();

                       if (Convert.ToString(lstBankInformation.pIsdebitcardapplicable) == "True")
                       {
                           if (lstBankInformation.lstBankdebitcarddtlsDTO != null)
                           {
                               for (int i = 0; i < lstBankInformation.lstBankdebitcarddtlsDTO.Count; i++)
                               {
                                   string query = "select count(*) from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where status=true and tbl_mst_bank_configuration_id <> " + lstBankInformation.lstBankdebitcarddtlsDTO[i].pRecordid + " and debitcard_number=" + lstBankInformation.lstBankdebitcarddtlsDTO[i].pCardNo + " and company_code='" + companycode + "' and branch_code='" + branchcode + "';";
                                  // select count(*) from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where status=true and tbl_mst_bank_configuration_id <>" + lstBankInformation.lstBankdebitcarddtlsDTO[i].pRecordid + " and debitcard_number=" + lstBankInformation.lstBankdebitcarddtlsDTO[i].pCardNo

                                   using (NpgsqlCommand cmd = new NpgsqlCommand(query, con))
                                   {
                                       DebtCardCount = Convert.ToInt64(cmd.ExecuteScalar());
                                   }
                               }
                           }
                       }

                       if (Convert.ToString(lstBankInformation.pIsupiapplicable) == "True")
                       {
                           if (lstBankInformation.lstBankUPI != null)
                           {
                               for (int i = 0; i < lstBankInformation.lstBankUPI.Count; i++)
                               {
                                   if (Convert.ToString(lstBankInformation.lstBankUPI[i].pUpiid) != string.Empty)
                                   {
                                       if (lstBankInformation.lstBankUPI[i].pRecordid == null)
                                       {
                                           lstBankInformation.lstBankUPI[i].pRecordid = 0;
                                       }

                                       string query = "select count(*) from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where status=true and tbl_mst_bank_configuration_id <> " + lstBankInformation.lstBankdebitcarddtlsDTO[i].pRecordid + " and debitcard_number=" + lstBankInformation.lstBankdebitcarddtlsDTO[i].pCardNo + " and company_code='" + companycode + "' and branch_code='" + branchcode + "';";
                                     //  select count(*) from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_upi_details where status=true and bank_upi_address='" + lstBankInformation.lstBankUPI[i].pUpiid + "' and tbl_mst_bank_upi_details_id not in(" + lstBankInformation.lstBankUPI[i].pRecordid + ");

                                       using (NpgsqlCommand cmd = new NpgsqlCommand(query, con))
                                       {
                                           UPIIdCount = UPIIdCount + Convert.ToInt64(cmd.ExecuteScalar());
                                       }
                                   }
                               }
                           }
                       }

                       if (Convert.ToString(lstBankInformation.pBankname) != string.Empty)
                       {
                           string query = "select count(*) from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration  where  account_number='" + lstBankInformation.pAccountnumber + "' and status=true and tbl_mst_bank_configuration_id not in(" + lstBankInformation.pRecordid + ")and company_code='"+companycode +"' and branch_code='" + branchcode + "';";
                           //select count(*) from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where account_number='" + lstBankInformation.pAccountnumber + "' and status=true and tbl_mst_bank_configuration_id not in(" + lstBankInformation.pRecordid + ");

                           using (NpgsqlCommand cmd = new NpgsqlCommand(query, con))
                           {
                               bankcount = Convert.ToInt64(cmd.ExecuteScalar());
                           }
                       }
                   }

                   if (DebtCardCount == 0 && UPIIdCount == 0 && bankcount == 0)
                   {
                       lstdata.Add("TRUE");
                   }
                   else if (DebtCardCount > 0 && UPIIdCount > 0 && bankcount > 0)
                   {
                       lstdata.Add("FALSE");
                   }
                   else
                   {
                       if (bankcount > 0)
                           lstdata.Add("B");

                       if (DebtCardCount > 0)
                           lstdata.Add("D");

                       if (UPIIdCount > 0)
                           lstdata.Add("U");
                   }
               }
           }
           catch (Exception ex)
           {
               throw ex;
           }

           return lstdata;
        }



public ChequesOnHandDTO GetBankBalance1(string brstodate,long recordid, string con, string BranchSchema,string branchCode,string companyCode)
{
    ChequesOnHandDTO obj = new ChequesOnHandDTO();
   AccountsDAL objAccountTransDal = new AccountsDAL();
    DataSet ds = new DataSet();
    string brsfromdate = string.Empty;
    // string brstodate = string.Empty;
    string fromdate = string.Empty;
    string todate = string.Empty;

    try
    {
        obj._BankBalance = objAccountTransDal.GetBankBalance(recordid, con, BranchSchema,companyCode,branchCode);

        using (NpgsqlConnection connection = new NpgsqlConnection(con))
        {
            connection.Open();

            // ---------- DATASET QUERY (ONE LINE) ----------
            string query1 = "select distinct to_char(modifieddate,'DD-MM-YYYY') modifieddate from (select log_entry_date_time::date as modifieddate from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference_log where log_entry_date_time is not null and activity_type='U' and company_code='"+ companyCode +"' and branch_code='"+ branchCode +"' union all select log_entry_date_time::date as modifieddate from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference_log where log_entry_date_time is not null and activity_type='U' and company_code='"+ companyCode +"' and branch_code='"+ branchCode +"' order by modifieddate desc limit 2) t;";
            //select distinct to_char(modifieddate,'DD-MM-YYYY') modifieddate from (select log_entry_date_time::date as modifieddate from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference_log where log_entry_date_time is not null and activity_type='U' union all select log_entry_date_time from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference_log where log_entry_date_time is not null and activity_type='U' order by modifieddate desc limit 2)t;

            using (NpgsqlDataAdapter da = new NpgsqlDataAdapter(query1, connection))
            {
                da.Fill(ds);
            }

            if (ds.Tables[0].Rows.Count > 0)
            {
                if (ds.Tables[0].Rows.Count == 1)
                {
                    brsfromdate = ds.Tables[0].Rows[0]["modifieddate"].ToString();
                    brstodate = ds.Tables[0].Rows[0]["modifieddate"].ToString();
                }
                else
                {
                    brstodate = ds.Tables[0].Rows[0]["modifieddate"].ToString();
                    brsfromdate = ds.Tables[0].Rows[1]["modifieddate"].ToString();
                }

                // ---------- FROM DATE QUERY (ONE LINE) ----------
                string query2 = "select max(cleardate)::date::text createddate from (select coalesce(clear_date,deposited_date) as cleardate from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference_log where activity_type='U' and log_entry_date_time::date='" + FormatDate(brstodate) + "' and company_code='" + companyCode + "' and branch_code='" + branchCode + "' union all select clear_date from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference_log where activity_type='U' and log_entry_date_time::date='" + FormatDate(brstodate) + "' and company_code='" + companyCode + "' and branch_code='" + branchCode + "') x;).ToString();";
               // select max(cleardate)::date::text createddate from (select coalesce(clear_date,deposited_date) as cleardate from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference_log where activity_type='U' and log_entry_date_time::date='" + FormatDate(brstodate) + "' union all select clear_date from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference_log where activity_type='U' and log_entry_date_time::date='" + FormatDate(brstodate) + "')x;

                using (NpgsqlCommand cmd = new NpgsqlCommand(query2, connection))
                {
                    object result = cmd.ExecuteScalar();
                    if (result != null)
                        fromdate = result.ToString();
                }

                if (!string.IsNullOrEmpty(fromdate))
                {
                    obj.ptobrsdate = fromdate;
                }

                // ---------- TO DATE QUERY (ONE LINE) ----------
                string query3 = "select min(cleardate)::date::text createddate from (select coalesce(clear_date,deposited_date) as cleardate from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference_log where activity_type='U' and log_entry_date_time::date='" + FormatDate(brsfromdate) + "' and company_code='" + companyCode + "' and branch_code='" + branchCode + "' union all select clear_date from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference_log where activity_type='U' and log_entry_date_time::date='" + FormatDate(brstodate) + "' and company_code='" + companyCode + "' and branch_code='" + branchCode + "') x;).ToString();";
                //select min(cleardate)::date::text createddate from (select coalesce(clear_date,deposited_date) as cleardate from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference_log where activity_type='U' and log_entry_date_time::date='" + FormatDate(brsfromdate) + "' union all select clear_date from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference_log where activity_type='U' and log_entry_date_time::date='" + FormatDate(brstodate) + "')x;

                using (NpgsqlCommand cmd = new NpgsqlCommand(query3, connection))
                {
                    object result = cmd.ExecuteScalar();
                    if (result != null)
                        todate = result.ToString();
                }

                if (!string.IsNullOrEmpty(todate))
                {
                    obj.pfrombrsdate = todate;
                }
            }
        }
    }
    catch (Exception ex)
    {
        throw ex;
    }

    return obj;
}


public List<ReceiptReferenceDTO> GetPendingautoBRSDetails(string ConnectionString, string GlobalSchema, string BranchSchema, string allocationstatus,string BranchCode,string CompanyCode)
{
    string strWhere = string.Empty;
    List<ReceiptReferenceDTO> lstPendingautoBRS = new List<ReceiptReferenceDTO>();

    try
    {
        if (!string.IsNullOrEmpty(allocationstatus))
        {
            strWhere = " where status=true and allocation_status='" + allocationstatus + "'";
        }
        else
        {
            strWhere = " where status=true ";
        }

        string Query = "select created_Date,transaction_date,reference_number,amount,modeof_receipt,receipt_type,reference_text from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_brs_auto_temp " + strWhere + " and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' order by created_Date";
        //select created_Date,transaction_date,reference_number,amount,modeof_receipt,receipt_type,reference_text from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_brs_auto_temp " + strWhere + " order by created_Date;

        using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
        {
            con.Open();

            using (NpgsqlCommand cmd = new NpgsqlCommand(Query, con))
            using (NpgsqlDataReader dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    lstPendingautoBRS.Add(new ReceiptReferenceDTO
                    {
                        puploadeddate = dr["created_Date"],
                        transactiondate = dr["transaction_date"],
                        pChequenumber = dr["reference_number"],
                        ptotalreceivedamount = dr["amount"],
                        pmodofreceipt = dr["modeof_receipt"],
                        preceiptype = dr["receipt_type"],
                        preferencetext = dr["reference_text"],
                    });
                }
            }
        }
    }
    catch (Exception ex)
    {
        throw ex;
    }

    return lstPendingautoBRS;
}



public List<InitialPaymentVoucherDTO> GetInitialPVDetails(string connectionstring, string fromdate, string todate, string transtype, string localSchema, string GlobalSchema,string CompanyCode,string Branchcode)
{
    List<InitialPaymentVoucherDTO> lstInitialPV = new List<InitialPaymentVoucherDTO>();
    string _query;

    try
    {
        if (transtype == "ALL")
        {
            _query = "select a.chitgroup_id,b.groupcode,ticketno,(select  contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id in (select distinct contact_id from " + AddDoubleQuotes(localSchema) + ".tbl_trans_ZPD_SUBSCRIBER  where  tbl_trans_zpd_subscriber_id=scheme_subscriber_id))as subscriber_name,a.payment_number,coalesce(transaction_date::text,'')transaction_date,coalesce(payment_amount,0) payment_amount,reference_number,bank_name from " + AddDoubleQuotes(localSchema) + ".tbl_trans_zpd_initial_payment_voucher  a join   " + AddDoubleQuotes(localSchema) + ".tbl_mst_chitgroup b on a.chitgroup_id=b.tbl_mst_chitgroup_id join ((select bank_name,reference_number,payment_number from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank l join  " + AddDoubleQuotes(localSchema) + ".tbl_mst_bank_configuration m on tbl_mst_bank_id= m.bank_id join   " + AddDoubleQuotes(localSchema) + ".tbl_trans_payment_reference n on m.tbl_mst_bank_configuration_id=n.bank_configuration_id)) d on a.payment_number=d.payment_number where a.company_code='"+CompanyCode+"' and a.Branch_Code='"+Branchcode+"';";
           // select a.chitgroup_id,b.groupcode,ticketno,(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id in (select distinct contact_id from " + AddDoubleQuotes(localSchema) + ".tbl_trans_ZPD_SUBSCRIBER where tbl_trans_zpd_subscriber_id=scheme_subscriber_id)) as subscriber_name,a.payment_number,coalesce(transaction_date::text,'') transaction_date,coalesce(payment_amount,0) payment_amount,reference_number,bank_name from " + AddDoubleQuotes(localSchema) + ".tbl_trans_zpd_initial_payment_voucher a join " + AddDoubleQuotes(localSchema) + ".tbl_mst_chitgroup b on a.chitgroup_id=b.tbl_mst_chitgroup_id join ((select bank_name,reference_number,payment_number from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank l join " + AddDoubleQuotes(localSchema) + ".tbl_mst_bank_configuration m on tbl_mst_bank_id=m.bank_id join " + AddDoubleQuotes(localSchema) + ".tbl_trans_payment_reference n on m.tbl_mst_bank_configuration_id=n.bank_configuration_id)) d on a.payment_number=d.payment_number;
        }
        else
        {
            _query = "select a.chitgroup_id,b.groupcode,ticketno,(select  contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id in (select distinct contact_id from " + AddDoubleQuotes(localSchema) + ".tbl_trans_ZPD_SUBSCRIBER  where  tbl_trans_zpd_subscriber_id=scheme_subscriber_id))as subscriber_name,a.payment_number,coalesce(transaction_date::text,'')transaction_date,coalesce(payment_amount,0) payment_amount,reference_number,bank_name from " + AddDoubleQuotes(localSchema) + ".tbl_trans_zpd_initial_payment_voucher  a join   " + AddDoubleQuotes(localSchema) + ".tbl_mst_chitgroup b on a.chitgroup_id=b.tbl_mst_chitgroup_id join ((select bank_name,reference_number,payment_number from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank l join  " + AddDoubleQuotes(localSchema) + ".tbl_mst_bank_configuration m on tbl_mst_bank_id= m.bank_id join   " + AddDoubleQuotes(localSchema) + ".tbl_trans_payment_reference n on m.tbl_mst_bank_configuration_id=n.bank_configuration_id)) d on a.payment_number=d.payment_number where transaction_date between '" + FormatDate(fromdate) + "' and '" + FormatDate(todate) + "' and a.company_code='"+CompanyCode+"' and a.branch_code='"+Branchcode+"';";
           // select a.chitgroup_id,b.groupcode,ticketno,(select contact_mailing_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact where tbl_mst_contact_id in (select distinct contact_id from " + AddDoubleQuotes(localSchema) + ".tbl_trans_ZPD_SUBSCRIBER where tbl_trans_zpd_subscriber_id=scheme_subscriber_id)) as subscriber_name,a.payment_number,coalesce(transaction_date::text,'') transaction_date,coalesce(payment_amount,0) payment_amount,reference_number,bank_name from " + AddDoubleQuotes(localSchema) + ".tbl_trans_zpd_initial_payment_voucher a join " + AddDoubleQuotes(localSchema) + ".tbl_mst_chitgroup b on a.chitgroup_id=b.tbl_mst_chitgroup_id join ((select bank_name,reference_number,payment_number from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank l join " + AddDoubleQuotes(localSchema) + ".tbl_mst_bank_configuration m on tbl_mst_bank_id=m.bank_id join " + AddDoubleQuotes(localSchema) + ".tbl_trans_payment_reference n on m.tbl_mst_bank_configuration_id=n.bank_configuration_id)) d on a.payment_number=d.payment_number where transaction_date between '" + FormatDate(fromdate) + "' and '" + FormatDate(todate) + "';
        }

        using (NpgsqlConnection con = new NpgsqlConnection(connectionstring))
        {
            con.Open();

            using (NpgsqlCommand cmd = new NpgsqlCommand(_query, con))
            using (NpgsqlDataReader dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    InitialPaymentVoucherDTO InitialPV = new InitialPaymentVoucherDTO();
                    {
                        InitialPV.pGroupcode = dr["groupcode"];
                        InitialPV.pTicketno = dr["ticketno"];
                        InitialPV.pSubscribername = dr["subscriber_name"];
                        InitialPV.pPayment_number = dr["payment_number"];
                        InitialPV.pTransactiondate = dr["transaction_date"];
                        InitialPV.pPaidamount = dr["payment_amount"];
                        InitialPV.pChequeno = dr["reference_number"];
                        InitialPV.pBankname = dr["bank_name"];
                    };
                    lstInitialPV.Add(InitialPV);
                }
            }
        }
    }
    catch (Exception ex)
    {
        throw ex;
    }

    return lstInitialPV;
}



public List<ReceiptReferenceDTO> GetPendingautoBRSDetailsIssued(string ConnectionString, string BranchSchema, string allocationstatus,string BranchCode,string CompanyCode)
{
    string strWhere = string.Empty;
    List<ReceiptReferenceDTO> lstPendingautoBRS = new List<ReceiptReferenceDTO>();

    try
    {
        if (!string.IsNullOrEmpty(allocationstatus))
        {
            strWhere = " where status=true and allocation_status='" + allocationstatus + "'";
        }
        else
        {
            strWhere = " where status=true ";
        }

        string Query = "select created_Date,transaction_date,reference_number,amount,reference_text from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_brs_auto_temp_issued " + strWhere + " and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' order by created_Date;";
       // select created_Date,transaction_date,reference_number,amount,reference_text from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_brs_auto_temp_issued " + strWhere + " order by created_Date;

        using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
        {
            con.Open();

            using (NpgsqlCommand cmd = new NpgsqlCommand(Query, con))
            using (NpgsqlDataReader dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    lstPendingautoBRS.Add(new ReceiptReferenceDTO
                    {
                        puploadeddate = dr["created_Date"],
                        transactiondate = dr["transaction_date"],
                        pChequenumber = dr["reference_number"],
                        ptotalreceivedamount = dr["amount"],
                        preferencetext = dr["reference_text"],
                    });
                }
            }
        }
    }
    catch (Exception ex)
    {
        throw ex;
    }

    return lstPendingautoBRS;
}



public List<BRSDto> GetBrs(string con, string fromDate, long pBankAccountId, string BranchSchema, string GlobalSchema,string branchCode,string companyCode)
{
    string Query = string.Empty;
    string Query1 = string.Empty;
    string Query2 = string.Empty;
    string Condition = string.Empty;
    decimal BankBookBalance = 0;

   List<BRSDto> lstBRS = new List<BRSDto>();

    try
    {
        using (NpgsqlConnection conn = new NpgsqlConnection(con))
        {
            conn.Open();

            Query2 = "select count(*) from  " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_brsreport where brsdate='" + FormatDate(fromDate) + "' and bankid=" + pBankAccountId + " AND company_code ='" + companyCode + "' and branch_code = '" + branchCode + "';";
           // select count(*) from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_brsreport where brsdate='" + FormatDate(fromDate) + "' and bankid=" + pBankAccountId + ";
            using (NpgsqlCommand cmd = new NpgsqlCommand(Query2, conn))
            {
                Int64 brscount = Convert.ToInt64(cmd.ExecuteScalar());

                if (brscount >= 1)
                {
                    Query = "select * from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_brsreport where  brsdate='" + FormatDate(fromDate) + "' and bankid=" + pBankAccountId + " AND company_code ='" + companyCode + "' and branch_code = '" + branchCode + "' ;";
                   // select * from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_brsreport where brsdate='" + FormatDate(fromDate) + "' and bankid=" + pBankAccountId + ";

                    Query1 = "select coalesce(sum( coalesce(debitamount,0)-coalesce(creditamount,0)),0) as bankbookbalance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration  t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.bank_account_id=t2.parent_id where t1.tbl_mst_bank_configuration_id=" + pBankAccountId + " and transaction_date<='" + FormatDate(fromDate) + "' and t2.company_code = '" + companyCode + "' and t2.branch_code = '" + branchCode + "' group by t1.tbl_mst_bank_configuration_id;";
                   // select coalesce(sum(coalesce(debitamount,0)-coalesce(creditamount,0)),0) as bankbookbalance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.bank_account_id=t2.parent_id where t1.tbl_mst_bank_configuration_id=" + pBankAccountId + " and transaction_date<='" + FormatDate(fromDate) + "' group by t1.tbl_mst_bank_configuration_id;

                    using (NpgsqlCommand cmd1 = new NpgsqlCommand(Query1, conn))
                    {
                        BankBookBalance = Convert.ToDecimal(cmd1.ExecuteScalar());
                    }

                    string _query = "select branch_type from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration  where branch_code='" + branchCode + "'";
                   // select branch_type from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where branch_code='" + BranchSchema + "';
                    string Branchtype;
                    using (NpgsqlCommand cmd2 = new NpgsqlCommand(_query, conn))
                    {
                        Branchtype = Convert.ToString(cmd2.ExecuteScalar());
                    }

                    if (Branchtype == "KGMS")
                        Condition = " and selfchequestatus = true";

                    using (NpgsqlCommand cmd3 = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd3.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            BRSDto _ObjBank = new BRSDto();
                            _ObjBank.precordid = Convert.ToInt64(dr["tbl_trans_brsreport_id"]);
                            _ObjBank.ptransactiondate = dr["cheque_date"];
                            _ObjBank.pChequeNumber = Convert.ToString(dr["reference_number"]);
                            _ObjBank.pparticulars = Convert.ToString(dr["PARTICULARS"]);
                            _ObjBank.ptotalreceivedamount = Convert.ToDecimal(dr["amount"]);
                            _ObjBank.pBankName = Convert.ToString(dr["BANK_name"]);
                            _ObjBank.pBranchName = Convert.ToString(dr["BRANCH_name"]);
                            _ObjBank.pGroupType = Convert.ToString(dr["group_type"]);
                            _ObjBank.pbrsdate = Convert.ToString(dr["brsdate"]);
                            _ObjBank.pbankbalance = Convert.ToString(dr["bankbalance"]);
                            _ObjBank.pBankBookBalance = BankBookBalance;
                            lstBRS.Add(_ObjBank);
                        }
                    }
                }
                else
                {
                    Query = "select coalesce(sum( coalesce(debitamount,0)-coalesce(creditamount,0)),0) as bankbookbalance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration  t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.bank_account_id=t2.parent_id where t1.tbl_mst_bank_configuration_id=" + pBankAccountId + " and transaction_date<='" + FormatDate(fromDate) + "' and t2.company_code = '" + companyCode + "' and t2.branch_code = '" + branchCode + "' group by t1.tbl_mst_bank_configuration_id;";
                  //  select coalesce(sum(coalesce(debitamount,0)-coalesce(creditamount,0)),0) as bankbookbalance from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_total_transactions t2 on t1.bank_account_id=t2.parent_id where t1.tbl_mst_bank_configuration_id=" + pBankAccountId + " and transaction_date<='" + FormatDate(fromDate) + "' group by t1.tbl_mst_bank_configuration_id;

                    using (NpgsqlCommand cmd4 = new NpgsqlCommand(Query, conn))
                    {
                        BankBookBalance = Convert.ToDecimal(cmd4.ExecuteScalar());
                    }

                    string _query = "select branch_type from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration  where branch_code='" + BranchSchema + "'";
                    //select branch_type from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_branch_configuration where branch_code='" + BranchSchema + "';
                    string Branchtype;
                    using (NpgsqlCommand cmd5 = new NpgsqlCommand(_query, conn))
                    {
                        Branchtype = Convert.ToString(cmd5.ExecuteScalar());
                    }

                    if (Branchtype == "KGMS")
                        Condition = " and selfchequestatus = true";

                    Query = "SELECT BS.tbl_mst_bank_configuration_id,CD.deposited_date::text as chequedate,CD.reference_number,CD.received_from PARTICULARS,CD.total_received_amount,(select bank_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank where tbl_mst_bank_id in(select distinct bank_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where tbl_mst_bank_configuration_id=" + pBankAccountId + ")) BANKNAME,BS.bank_branch as branchname,'CHEQUES DEPOSITED BUT NOT CREDITED' groupType,0 as bankbalance FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference CD JOIN " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration   BS ON CD.deposited_bank_id=BS.tbl_mst_bank_configuration_id  AND  CD.deposit_status='P' AND CD.CLEAR_STATUS='N' AND deposited_date<='" + FormatDate(fromDate) + "' and BS.tbl_mst_bank_configuration_id=" + pBankAccountId + " AND CD.total_received_amount>0 " + Condition + " and CD.company_code = '" + companyCode + "' and CD.branch_code = '" + branchCode + "' UNION ALL SELECT P.bank_configuration_id,P.payment_DATE::text,P.reference_number,P.paid_to ||' '||coalesce((select coalesce(e.account_name, '') from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher_details b on a.tbl_trans_payment_voucher_id = b.payment_voucher_id join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference c on a.payment_number = c.payment_number join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration d on d.tbl_mst_bank_configuration_id = c.bank_configuration_id join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account e on debit_account_id = e.account_id and d.account_name like 'Subscriber Bank%3' and d.tbl_mst_bank_configuration_id = P.bank_configuration_id and c.reference_number = P.reference_number and c.clear_status=p.clear_status),'') as particulars,coalesce(p.paid_amount,0) as total_paid_amount,(select bank_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank where tbl_mst_bank_id in(select distinct bank_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where tbl_mst_bank_configuration_id=" + pBankAccountId + "))BANKNAME,(select bank_branch from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where tbl_mst_bank_configuration_id in (select distinct branch_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference)) branchname,'CHEQUES ISSUED BUT NOT CLEARED' as groupType,0 as bankbalance FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference AS P where P.payment_DATE<='" + FormatDate(fromDate) + "' AND P.CLEAR_STATUS='N' and bank_configuration_id=" + pBankAccountId + "and p.paid_amount>0 order by chequedate;";
                   // SELECT BS.tbl_mst_bank_configuration_id,CD.deposited_date::text as chequedate,CD.reference_number,CD.received_from PARTICULARS,CD.total_received_amount,(select bank_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank where tbl_mst_bank_id in(select distinct bank_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where tbl_mst_bank_configuration_id=" + pBankAccountId + ")) BANKNAME,BS.bank_branch as branchname,'CHEQUES DEPOSITED BUT NOT CREDITED' groupType,0 as bankbalance FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_receipt_reference CD JOIN " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration BS ON CD.deposited_bank_id=BS.tbl_mst_bank_configuration_id AND CD.deposit_status='P' AND CD.CLEAR_STATUS='N' AND deposited_date<='" + FormatDate(fromDate) + "' and BS.tbl_mst_bank_configuration_id=" + pBankAccountId + " AND CD.total_received_amount>0 " + Condition + " UNION ALL SELECT P.bank_configuration_id,P.payment_DATE::text,P.reference_number,P.paid_to ||' '||coalesce((select coalesce(e.account_name,'') from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher_details b on a.tbl_trans_payment_voucher_id=b.payment_voucher_id join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference c on a.payment_number=c.payment_number join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration d on d.tbl_mst_bank_configuration_id=c.bank_configuration_id join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_account e on debit_account_id=e.account_id and d.account_name like 'Subscriber Bank%3' and d.tbl_mst_bank_configuration_id=P.bank_configuration_id and c.reference_number=P.reference_number and c.clear_status=p.clear_status),'') as particulars,coalesce(p.paid_amount,0) as total_paid_amount,(select bank_name from " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank where tbl_mst_bank_id in(select distinct bank_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where tbl_mst_bank_configuration_id=" + pBankAccountId + ")) BANKNAME,(select bank_branch from " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration where tbl_mst_bank_configuration_id in (select distinct branch_id from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference)) branchname,'CHEQUES ISSUED BUT NOT CLEARED' as groupType,0 as bankbalance FROM " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference P where P.payment_DATE<='" + FormatDate(fromDate) + "' AND P.CLEAR_STATUS='N' and bank_configuration_id=" + pBankAccountId + " and p.paid_amount>0 order by chequedate;

                    using (NpgsqlCommand cmd6 = new NpgsqlCommand(Query, conn))
                    using (NpgsqlDataReader dr = cmd6.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            BRSDto _ObjBank = new BRSDto();
                            _ObjBank.precordid = Convert.ToInt64(dr["tbl_mst_bank_configuration_id"]);
                            _ObjBank.ptransactiondate = dr["chequedate"];
                            _ObjBank.pChequeNumber = Convert.ToString(dr["reference_number"]);
                            _ObjBank.pparticulars = Convert.ToString(dr["PARTICULARS"]);
                            _ObjBank.ptotalreceivedamount = Convert.ToDecimal(dr["total_received_amount"]);
                            _ObjBank.pBankName = Convert.ToString(dr["BANKNAME"]);
                            _ObjBank.pBranchName = Convert.ToString(dr["BRANCHname"]);
                            _ObjBank.pGroupType = Convert.ToString(dr["groupType"]);
                            _ObjBank.pbankbalance = Convert.ToString(dr["bankbalance"]);
                            _ObjBank.pBankBookBalance = BankBookBalance;
                            lstBRS.Add(_ObjBank);
                        }
                    }
                }
            }
        }

        if (lstBRS.Count == 0)
        {
            BRSDto _ObjBank = new BRSDto();
            _ObjBank.pBankBookBalance = BankBookBalance;
            lstBRS.Add(_ObjBank);
        }
    }
    catch (Exception ex)
    {
        throw ex;
    }

    return lstBRS;
}



public List<ReceiptReferenceDTO> GetIssuedCancelledChequesubi(string ConnectionString, string BrsFromDate, string BrsTodate, Int64 _BankId, string GlobalSchema, string BranchSchema,string BranchCode,string CompanyCode)
{
   List<ReceiptReferenceDTO> lstreceipts = new List<ReceiptReferenceDTO>();
    string Query = string.Empty;
    string strWhere = string.Empty;

    try
    {
        if ((string.IsNullOrEmpty(BrsFromDate) && string.IsNullOrEmpty(BrsTodate)) || (BrsFromDate == null && BrsTodate == null))
        {
            Query = "select * from (select bank_configuration_id,a.payment_number,coalesce(a.payment_date::text,'')payment_date,a.total_paid_amount,b.modeof_payment,reference_number,bank_name,clear_status,coalesce(clear_date ::text,'')clear_date,coalesce(subscriber_details,paid_to)subscriber_details from (select payment_number,payment_date,total_paid_amount,credit_account_id,null as subscriber_details from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher union all select payment_number,t1.transaction_date,transaction_amount,credit_account_id, groupcode||'-'||ticketno||'('||contact_mailing_name||')' as subscriber_details from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_chit_payment t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_chitgroup t2 on t1.chitgroup_id=t2.tbl_mst_chitgroup_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact t3 on t3.tbl_mst_contact_id=t1.contact_id) a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference b on a.payment_number = b.payment_number left join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration c on b.bank_configuration_id=c.tbl_mst_bank_configuration_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank d on c.bank_id=d.tbl_mst_bank_id  where clear_status in ('P','C','R') and case when " + _BankId + "=0 then credit_account_id>0 else tbl_mst_bank_configuration_id=" + _BankId + " and c.company_code='" + CompanyCode + "' and c.branch_code='" + BranchCode + "' end)tt;";
            //select * from (select bank_configuration_id,a.payment_number,coalesce(a.payment_date::text,'')payment_date,a.total_paid_amount,b.modeof_payment,reference_number,bank_name,clear_status,coalesce(clear_date::text,'')clear_date,coalesce(subscriber_details,paid_to)subscriber_details from (select payment_number,payment_date,total_paid_amount,credit_account_id,null as subscriber_details from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher union all select payment_number,t1.transaction_date,transaction_amount,credit_account_id,groupcode||'-'||ticketno||'('||contact_mailing_name||')' as subscriber_details from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_chit_payment t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_chitgroup t2 on t1.chitgroup_id=t2.tbl_mst_chitgroup_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact t3 on t3.tbl_mst_contact_id=t1.contact_id) a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference b on a.payment_number=b.payment_number left join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration c on b.bank_configuration_id=c.tbl_mst_bank_configuration_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank d on c.bank_id=d.tbl_mst_bank_id where clear_status in ('P','C','R') and case when " + _BankId + "=0 then credit_account_id>0 else tbl_mst_bank_configuration_id=" + _BankId + " end) tt;
        }
        else
        {
            Query = "select * from (select bank_configuration_id,a.payment_number,coalesce(a.payment_date::text,'')payment_date,a.total_paid_amount,b.modeof_payment,reference_number,bank_name,clear_status,coalesce(clear_date::text,'')clear_date,coalesce(subscriber_details,paid_to)subscriber_details from (select payment_number,payment_date,total_paid_amount,credit_account_id,null as subscriber_details from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher union all select payment_number,t1.transaction_date,transaction_amount,credit_account_id, groupcode||'-'||ticketno||'('||contact_mailing_name||')' as subscriber_details from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_chit_payment t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_chitgroup t2 on t1.chitgroup_id=t2.tbl_mst_chitgroup_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact t3 on t3.tbl_mst_contact_id=t1.contact_id) a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference b on a.payment_number = b.payment_number left join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration c on b.bank_configuration_id=c.tbl_mst_bank_configuration_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank d on c.bank_id=d.tbl_mst_bank_id  where clear_status in ('P','C','R') and clear_date between '" + FormatDate(BrsFromDate) + "' and '" + FormatDate(BrsTodate) + "' and c.company_code='" + CompanyCode + "' and c.branch_code='" + BranchCode + "')tt where bank_configuration_id=" + _BankId + ";";
          //  select * from (select bank_configuration_id,a.payment_number,coalesce(a.payment_date::text,'')payment_date,a.total_paid_amount,b.modeof_payment,reference_number,bank_name,clear_status,coalesce(clear_date::text,'')clear_date,coalesce(subscriber_details,paid_to)subscriber_details from (select payment_number,payment_date,total_paid_amount,credit_account_id,null as subscriber_details from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher union all select payment_number,t1.transaction_date,transaction_amount,credit_account_id,groupcode||'-'||ticketno||'('||contact_mailing_name||')' as subscriber_details from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_chit_payment t1 join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_chitgroup t2 on t1.chitgroup_id=t2.tbl_mst_chitgroup_id join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_contact t3 on t3.tbl_mst_contact_id=t1.contact_id) a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_reference b on a.payment_number=b.payment_number left join " + AddDoubleQuotes(BranchSchema) + ".tbl_mst_bank_configuration c on b.bank_configuration_id=c.tbl_mst_bank_configuration_id left join " + AddDoubleQuotes(GlobalSchema) + ".tbl_mst_bank d on c.bank_id=d.tbl_mst_bank_id where clear_status in ('P','C','R') and clear_date between '" + FormatDate(BrsFromDate) + "' and '" + FormatDate(BrsTodate) + "') tt where bank_configuration_id=" + _BankId + ";
        }

        using (NpgsqlConnection con = new NpgsqlConnection(ConnectionString))
        {
            con.Open();

            using (NpgsqlCommand cmd = new NpgsqlCommand(Query, con))
            using (NpgsqlDataReader dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    ReceiptReferenceDTO _receiptsDTO = new ReceiptReferenceDTO();
                    _receiptsDTO.preceiptid = Convert.ToString(dr["payment_number"]);
                    _receiptsDTO.pChequenumber = "02" + Convert.ToString(dr["reference_number"]);
                    _receiptsDTO.preceiptdate = dr["payment_date"];
                    _receiptsDTO.ptotalreceivedamount = Convert.ToDecimal(dr["total_paid_amount"]);
                    _receiptsDTO.ptypeofpayment = PayModes(Convert.ToString(dr["modeof_payment"]), "D");
                    _receiptsDTO.pdepositeddate = dr["clear_date"];
                    _receiptsDTO.pdepositbankid = dr["bank_configuration_id"];
                    _receiptsDTO.pdepositbankname = Convert.ToString(dr["bank_name"]);
                    _receiptsDTO.pchequestatus = Convert.ToString(dr["clear_status"]);
                    _receiptsDTO.ppartyname = Convert.ToString(dr["subscriber_details"]);
                    lstreceipts.Add(_receiptsDTO);
                }
            }
        }
    }
    catch (Exception ex)
    {
        throw ex;
    }

    return lstreceipts;
}


public List<AccountsDTO> GetCashAmountAccountWise(string type, string con, string GlobalSchema, string BranchSchema, string account_id, string transaction_date,string CompanyCode,string BranchCode)
{
    string strQuery = string.Empty;
    List<AccountsDTO> lstaccounts = new List<AccountsDTO>();

    try
    {
        if (type == "PAYMENT VOUCHER")
        {
            strQuery = "select debit_account_id as account_id,coalesce(sum(b.ledger_amount),0) amount from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher_details b on a.tbl_trans_payment_voucher_id=b.payment_voucher_id where modeof_payment='C' and payment_date='" + FormatDate(transaction_date) + "' and debit_account_id in (" + account_id + " ) and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' group by debit_account_id;";
           // select debit_account_id as account_id,coalesce(sum(b.ledger_amount),0) amount from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_payment_voucher_details b on a.tbl_trans_payment_voucher_id=b.payment_voucher_id where modeof_payment='C' and payment_date='" + FormatDate(transaction_date) + "' and debit_account_id in (" + account_id + ") group by debit_account_id;
        }
        else if (type == "GENERAL RECEIPT")
        {
            strQuery = "select credit_account_id as account_id,sum(receipt_amount) amount from (select credit_account_id,coalesce(sum(b.ledger_amount),0)receipt_amount from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_generalreceipt a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_generalreceipt_details b on a.tbl_trans_generalreceipt_id=b.generalreceipt_id where modeof_receipt='C' and receipt_date='" + FormatDate(transaction_date) + "' and credit_account_id in (" + account_id + ") and company_code='" + CompanyCode + "' and branch_code='" + BranchCode + "' group by credit_account_id  union all select credit_account_id,coalesce(sum(b.receipt_amount), 0) from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_chit_receipt a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_chit_receipt_details b on a.tbl_trans_chit_receipt_id = b.chit_receipt_id where modeof_receipt = 'C' and chit_receipt_date = '" + FormatDate(transaction_date) + "' and credit_account_id in ( " + account_id + ")and a.company_code='" + CompanyCode + "' and a.branch_code='" + BranchCode + "'group by credit_account_id)t group by credit_account_id;";
           // select credit_account_id as account_id,sum(receipt_amount) amount from (select credit_account_id,coalesce(sum(b.ledger_amount),0)receipt_amount from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_generalreceipt a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_generalreceipt_details b on a.tbl_trans_generalreceipt_id=b.generalreceipt_id where modeof_receipt='C' and receipt_date='" + FormatDate(transaction_date) + "' and credit_account_id in (" + account_id + ") group by credit_account_id union all select credit_account_id,coalesce(sum(b.receipt_amount),0) from " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_chit_receipt a join " + AddDoubleQuotes(BranchSchema) + ".tbl_trans_chit_receipt_details b on a.tbl_trans_chit_receipt_id=b.chit_receipt_id where modeof_receipt='C' and chit_receipt_date='" + FormatDate(transaction_date) + "' and credit_account_id in (" + account_id + ") group by credit_account_id) t group by credit_account_id;
        }
        else
        {
            strQuery = "";
        }

        if (!string.IsNullOrEmpty(strQuery))
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(con))
            {
                connection.Open();

                using (NpgsqlCommand cmd = new NpgsqlCommand(strQuery, connection))
                using (NpgsqlDataReader dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        lstaccounts.Add(new AccountsDTO
                        {
                            psubledgerid = dr["account_id"],
                            accountbalance = dr["amount"]
                        });
                    }
                }
            }
        }
    }
    catch (Exception ex)
    {
        throw ex;
    }

    return lstaccounts;
}




    public bool UnusedhequeCancel(string ConnectionString, string branchSchema, string globalSchema, IssuedChequeDTO issuedChequeDTO,string BranchCode,string CompanyCode)
{
    bool isSaved = false;
    StringBuilder Query = new StringBuilder();

    try
    {
      NpgsqlConnection  con = new NpgsqlConnection(ConnectionString);
        if (con.State != ConnectionState.Open)
        {
            con.Open();
        }


        if (issuedChequeDTO.lstIssuedCheque.Count > 0)
        {
            for (int i = 0; i < issuedChequeDTO.lstIssuedCheque.Count; i++)
            {
                Query.Append("update " + AddDoubleQuotes(branchSchema) + ".tbl_mst_cheques set cheque_status= 'Cancelled' where bank_configuration_id=" + issuedChequeDTO.lstIssuedCheque[i].pbankaccountid + "  and cheque_book_id=" + issuedChequeDTO.lstIssuedCheque[i].pchkBookId + " and cheque_number=" + issuedChequeDTO.lstIssuedCheque[i].pchequenumber + " and cheque_status!='Cancelled' and company_code='"+CompanyCode+"' and branch_code='"+BranchCode+"'");
               // update " + AddDoubleQuotes(branchSchema) + ".tbl_mst_cheques set cheque_status='Cancelled' where bank_configuration_id=" + issuedChequeDTO.lstIssuedCheque[i].pbankaccountid + " and cheque_book_id=" + issuedChequeDTO.lstIssuedCheque[i].pchkBookId + " and cheque_number=" + issuedChequeDTO.lstIssuedCheque[i].pchequenumber + " and cheque_status!='Cancelled';
            }
        }

        if (Query.Length > 0)
        {
            using (NpgsqlCommand cmd = new NpgsqlCommand(Query.ToString(), con))
            {
                cmd.CommandType = CommandType.Text;
                cmd.ExecuteNonQuery();
            }

            isSaved = true;
        }
    }
    catch (Exception ex)
    {
        throw ex;
    }
   

    return isSaved;
}





    }
}
