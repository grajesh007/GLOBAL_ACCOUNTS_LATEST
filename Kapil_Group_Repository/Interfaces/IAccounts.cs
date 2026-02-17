
using Kapil_Group_Repository.Entities;

namespace Kapil_Group_Repository.Interfaces
{
  public interface IAccounts
  {
    List<string> GetBankDetails(string connectionString, string GlobalSchema, string AccountsSchema);
    List<Bank> GetBankDetails1(string connectionString, string globalSchema, string accountsSchema, string BranchCode, string CompanyName);


    #region BankNames
    List<BankNamesDetails> GetBankNamesDetails(string connectionString, string? globalSchema, string? accountsSchema, string? companyCode, string? branchCode);
    #endregion BankNames


    #region CompanyNameAndAddressDetails
    List<CompanyBranchDetails> GetCompanyNameAndAddressDetails(string connectionString, string globalSchema, string companyCode, string branchCode);

    #endregion CompanyNameAndAddressDetails

    #region  BankConfigurationDetails
    List<BankConfigurationDetails> GetBankConfigurationDetails(string connectionString, string? branchSchema, string? companyCode, string? branchCode);

    #endregion BankConfigurationDetails

    #region ViewChequeManagementDetails
    List<ChequeManagementDTO> ViewChequeManagementDetails(string con, string BranchSchema, string GlobalSchema, string CompanyCode, string BranchCode, int PageSize, int PageNo);

    #endregion ViewChequeManagementDetails

    #region GetExistingChequeCount
    List<ExistingChequeCountDTO> GetExistingChequeCount(string connectionString, int bankId, int chqFromNo, int chqToNo, string branchSchema, string companyCode, string branchCode);

    #endregion GetExistingChequeCount


    #region BankUPIDetails...
    List<BankUPIDetails> GetBankUPIDetails(string connectionString, string GlobalSchema, string CompanyCode, string BranchCode);

    #endregion BankUPIDetails...

    #region ViewBankInformationDetails...
    List<ViewBankInformationDetails> GetViewBankInformationDetails(string connectionString, string GlobalSchema, string BranchSchema, string BranchCode, string CompanyCode);

    #endregion ViewBankInformationDetails...

    #region GeneralReceiptsData...
    List<GeneralReceiptsData> GetGeneralReceiptsData(string connectionString, string GlobalSchema, string BranchSchema, string TaxSchema, string CompanyCode, string BranchCode);

    #endregion GeneralReceiptsData...


    #region ViewBankInformation...
    List<ViewBankInformation> GetViewBankInformation(string connectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode, int precordid);
    #endregion ViewBankInformation...

    #region AvailableChequeCount...

    List<AvailableChequeCount> GetAvailableChequeCount(string connectionString, int bankId, int chqFromNo, int chqToNo, string branchSchema, string companyCode, string branchCode);

    #endregion AvailableChequeCount...

    #region PettyCashExistingData...
    List<PettyCashExistingData> GetPettyCashExistingData(string connectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string Branchcode);

    #endregion PettyCashExistingData...

    #region  PaymentVoucherExistingData..
    List<PaymentVoucherDetails> GetPaymentVoucherExistingData(string connectionString, string? globalSchema, string? branchSchema, string? companyCode, string? branchCode);

    #endregion PaymentVoucherExistingData..

    #region  ProductnamesandHSNcodes..
    List<ProductNamesAndHSNCodesDetails> GetProductNamesAndHSNCodes(string connectionString, string? globalSchema);

    #endregion ProductnamesandHSNcodes..

    #region  getReceiptNumber..
    public List<getReceiptNumber> getReceiptNumber(string connectionString, string? globalSchema, string? branchSchema, string? companyCode, string? branchCode);

    #endregion getReceiptNumber..

    #region GetBankUPIList...

    List<BankUPIListDetails> GetBankUPIListDetails(string connectionString, string? branchSchema, string? companyCode, string? branchCode);

    #endregion GetBankUPIList...


    #region GetCAOBranchList...
    List<CAOBranchListDetails> GetCAOBranchListDetails(string connectionString, string? globalSchema, string? branchSchema, string? companyCode, string? branchCode);

    #endregion GetCAOBranchList...

    #region GetBankBookDetails...
    List<AccountReportsDTO> GetBankBookDetails(string con, string fromDate, string toDate, long _pBankAccountId, string AccountsSchema, string GlobalSchema, string CompanyCode, string BranchCode);

    #endregion GetBankBookDetails...

    #region GetRePrintInterBranchGeneralReceiptCount...
    int GetRePrintInterBranchGeneralReceiptCount(string Connectionstring, string ReceiptId, string BranchSchema, string CompanyCode, string BranchCode);

    #endregion GetRePrintInterBranchGeneralReceiptCount.... 

    #region getStatesbyPartyid...
    List<GstDTo> getStatesbyPartyid(long ppartyid, string Connectionstring, int id, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode);

    #endregion getStatesbyPartyid...

    #region  checkAccountnameDuplicates...
    int checkAccountnameDuplicates(string Accountname, string AccountType, int Parentid, string GlobalSchema, string connectionstring, string CompanyCode, string BranchCode);

    #endregion checkAccountnameDuplicates...

    #region GetCashRestrictAmountpercontact....
    decimal GetCashRestrictAmountpercontact(string type, string branchtype, string con, string GlobalSchema, string BranchSchema, long contactid, string checkdate, string CompanyCode, string BranchCode);

    #endregion GetCashRestrictAmountpercontact...

    #region GetGstLedgerAccountList...
    List<AccountsDTO> GetGstLedgerAccountList(string ConnectionString, string formname, string BranchSchema, string CompanyCode, string BranchCode);

    #endregion GetGstLedgerAccountList...

<<<<<<< HEAD
        decimal GetCashRestrictAmountpercontact(string type, string branchtype, string con, string GlobalSchema, string BranchSchema, long contactid, string checkdate, string CompanyCode,
                    string BranchCode);
=======
    #region GetLedgerAccountList...
    List<AccountsDTO> GetLedgerAccountList(string ConnectionString, string formname, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode);
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8

    #endregion GetLedgerAccountList...

<<<<<<< HEAD
        List<AccountsDTO> GetGstLedgerAccountList(
string ConnectionString,
string formname,
string BranchSchema,
string CompanyCode,
string BranchCode);
=======
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8

    #region GetLedgerSummaryAccountList...
    List<AccountsDTO> GetLedgerSummaryAccountList(string ConnectionString, string formname, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode);

<<<<<<< HEAD
        List<AccountsDTO> GetLedgerAccountList(string ConnectionString, string formname, string GlobalSchema, string BranchSchema,
        string CompanyCode,
        string BranchCode);
=======
    #endregion GetLedgerSummaryAccountList...
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8


    #region GetSubAccountLedgerDetails...
    List<subAccountLedgerDTO> GetSubAccountLedgerDetails(string con, string BranchSchema, string CompanyCode, string BranchCode);

<<<<<<< HEAD
        List<AccountsDTO> GetLedgerSummaryAccountList(string ConnectionString, string formname, string GlobalSchema, string BranchSchema, string CompanyCode,
              string BranchCode);
=======
    #endregion GetSubAccountLedgerDetails...
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8

    #region  GetAccountLedgerData...
    List<subAccountLedgerDTO> GetAccountLedgerData(string con, string SubLedgerName, string BranchSchema, string CompanyCode, string BranchCode);

<<<<<<< HEAD
        List<subAccountLedgerDTO> GetSubAccountLedgerDetails(string con, string BranchSchema, string CompanyCode, string BranchCode);


        List<subAccountLedgerDTO> GetAccountLedgerData(
            string con,
            string SubLedgerName,
            string BranchSchema, string CompanyCode,
                  string BranchCode);



        List<subAccountLedgerDTO> GetSubLedgerReportData(string con, string SubLedgerName, long parentid, string fromDate, string toDate, string BranchSchema, string CompanyCode,
                  string BranchCode);

        // List<AccountsDTO> GetSubLedgerAccountList(long ledgerid, string ConnectionString, string GlobalSchema, string BranchSchema,string CompanyCode, string BranchCode);

=======
    #endregion GetAccountLedgerData...


    #region GetSubLedgerReportData....
    List<subAccountLedgerDTO> GetSubLedgerReportData(string con, string SubLedgerName, long parentid, string fromDate, string toDate, string BranchSchema, string CompanyCode, string BranchCode);

    #endregion GetSubLedgerReportData...

    #region GetSubLedgerAccountList...
    List<AccountsDTO> GetSubLedgerAccountList(long ledgerid, string ConnectionString, string GlobalSchema, string BranchSchema, string LocalSchema, string CompanyCode, string BranchCode);

    #endregion GetSubLedgerAccountList...

    #region  GetTrialBalance...
    List<AccountReportsDTO> GetTrialBalance(string con, string LocalSchema, string fromDate, string todate, string groupType, string CompanyCode, string BranchCode, string GlobalSchema);

    #endregion GetTrialBalance...
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8

        List<AccountsDTO> GetSubLedgerAccountList(long ledgerid, string ConnectionString, string GlobalSchema, string BranchSchema, string LocalSchema, string CompanyCode, string BranchCode);

<<<<<<< HEAD
        List<AccountReportsDTO> GetTrialBalance(string con, string LocalSchema, string fromDate, string todate, string groupType, string CompanyCode,
        string BranchCode, string GlobalSchema);

=======
    #region GetIssuedChequeNumbers...

    List<IssuedChequeDTO> GetIssuedChequeNumbers(string con, long bankId, string BranchSchema, string CompanyCode, string BranchCode);
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8

    #endregion GetIssuedChequeNumbers...

        List<IssuedChequeDTO> GetIssuedChequeNumbers(string con, long bankId, string BranchSchema, string CompanyCode,
                          string BranchCode);

<<<<<<< HEAD
=======
    #region GetMainAccounthead....
    List<subAccountLedgerDTO> GetMainAccounthead(string con, string BranchSchema, string CompanyCode, string BranchCode);
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8

    #endregion GetMainAccounthead...

<<<<<<< HEAD
        List<subAccountLedgerDTO> GetMainAccounthead(string con, string BranchSchema, string CompanyCode,
        string BranchCode);


        List<cashBookDto> getCashbookData(string ConnectionString, string fromdate, string todate, string BranchSchema, string CompanyCode,
        string BranchCode);
=======
    #region getCashbookData...
    List<cashBookDto> getCashbookData(string ConnectionString, string fromdate, string todate, string BranchSchema, string CompanyCode, string BranchCode);
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8

    #endregion getCashbookData...

    #region GetBalances...
    List<AccountReportsDTO> GetBalances(string con, string LocalSchema, string fromDate, string todate, string groupType, string formname, string CompanyCode, string BranchCode);

    #endregion GetBalances...

<<<<<<< HEAD
        List<BankTransferTypesDTO> GetBankTransferTypes(string ConnectionString, string branchSchema, string CompanyCode,
        string BranchCode);
=======
    #region GetBankTransferTypes...
    List<BankTransferTypesDTO> GetBankTransferTypes(string ConnectionString, string branchSchema, string CompanyCode, string BranchCode);
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8

    #endregion GetBankTransferTypes...

<<<<<<< HEAD
        List<ChequeEnquiryDTO> GetChequeReturnDetails(string con, string fromdate, string todate, string BranchSchema, string CompanyCode,
        string BranchCode, string GlobalSchema);
=======
    #region GetChequeReturnDetails...
    List<ChequeEnquiryDTO> GetChequeReturnDetails(string con, string fromdate, string todate, string BranchSchema, string CompanyCode, string BranchCode, string GlobalSchema);
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8

    #endregion GetChequeReturnDetails...

<<<<<<< HEAD
        List<IssuedChequeDTO> GetIssuedChequeDetails(string con, long _BankId, long _ChqBookId, long _ChqFromNo, long _ChqToNo, string BranchSchema, string GlobalSchema, string CompanyCode,
        string BranchCode);
=======
    #region GetIssuedChequeDetails...
    List<IssuedChequeDTO> GetIssuedChequeDetails(string con, long _BankId, long _ChqBookId, long _ChqFromNo, long _ChqToNo, string BranchSchema, string GlobalSchema, string CompanyCode, string BranchCode);
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8

    #endregion GetIssuedChequeDetails...

<<<<<<< HEAD
        List<IssuedChequeDTO> GetUnUsedCheques(string con, long _BankId, long _ChqBookId, long _ChqFromNo, long _ChqToNo, string BranchSchema, string GlobalSchema, string CompanyCode,
                          string BranchCode);


        List<CountryDTO> getCountry(string ConnectionString, string GlobalSchema);

        List<StateDTO> getState(string ConnectionString, string GlobalSchema, long id);

        List<District> getDistrict(string ConnectionString, string GlobalSchema, long id);


=======
    #region  GetUnUsedCheques...
    List<IssuedChequeDTO> GetUnUsedCheques(string con, long _BankId, long _ChqBookId, long _ChqFromNo, long _ChqToNo, string BranchSchema, string GlobalSchema, string CompanyCode, string BranchCode);

    #endregion GetUnUsedCheques...

    #region getCountry...
    List<CountryDTO> getCountry(string ConnectionString, string GlobalSchema);

    #endregion getCountry...

    #region  getState...
    List<StateDTO> getState(string ConnectionString, string GlobalSchema, long id);

    #endregion getState...
>>>>>>> 25f8aaa2477bf7f9e41aa226fb2f66a5cbb859f8

    #region getDistrict...
    List<District> getDistrict(string ConnectionString, string GlobalSchema, long id);

    #endregion getDistrict...

    #region Getformnamedetails...
    List<Formnamedetails> Getformnamedetails(string connectionString, string globalSchema, string companyCode, string branchCode);

    #endregion Getformnamedetails...

    List<PaymentVoucherReportDTO> GetChitPaymentReportData(string paymentId, string LocalSchema, string GlobalSchema, string Connectionstring, string CompanyCode, string BranchCode);

    List<GeneralReceiptSubDetails> GetPaymentVoucherDetailsReportData(string paymentId, object contact_id, string Connectionstring, string LocalSchema, string GlobalSchema, object accountid, object interbranch_id, string branchcode, string companycode);

    List<ChequesDTO> GetChequeNumbers(long bankid, string ConnectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode);

    List<BankUPI> GetUpiNames(long bankid, string ConnectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode);


    List<JvListDTO> GetJvListDetails(string con, string fromDate, string toDate, string modeOfTransaction, string BranchSchema, string GlobalSchema, string companyCode, string branchCode);

    List<AccountReportsDTO> GetAgentPaymentDetails(string con, string GlobalSchema, string BranchSchema, string parentaccountname, string accountname, string companyCode, string branchCode);



    List<JournalVoucherReportDTO> GetJournalVoucherReportData(
        string ConnectionString,
        string GlobalSchema,
        string BranchSchema,
        string Jvnumber, string companyCode, string branchCode);

    List<SVOnameDTO> Getmvonames(string connectionstring, string GlobalSchema, string localSchema, string branchcode);

    List<ModulesDTO> GetallRolesModules(string modulename, string globalSchema, string connectionString, string companyCode, string branchCode);

    List<PartyDTO> GetPartyListbygroup(string ConnectionString, string GlobalSchema, string BranchSchema, string subledger, string CompanyCode, string BranchCode);

    #region GetCheckDuplicateDebitCardNo
    CheckDuplicateBankDetailsDTO GetCheckDuplicateDebitCardNo(
    string connectionString,
    string? branchSchema,
    string? companyCode,
    string? branchCode,
    int? debitCardRecordId,
    string? debitCardNo,
    int? upiRecordId,
    string? upiId,
    int? accountRecordId,
    string? accountNumber);


    #endregion GetCheckDuplicateDebitCardNo
  }
}
