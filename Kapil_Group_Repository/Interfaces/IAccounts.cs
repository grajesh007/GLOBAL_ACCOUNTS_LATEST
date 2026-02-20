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

    #region GetLedgerAccountList...
    List<AccountsDTO> GetLedgerAccountList(string ConnectionString, string formname, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode);

    #endregion GetLedgerAccountList...


    #region GetLedgerSummaryAccountList...
    List<AccountsDTO> GetLedgerSummaryAccountList(string ConnectionString, string formname, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode);

    #endregion GetLedgerSummaryAccountList...


    #region GetSubAccountLedgerDetails...
    List<subAccountLedgerDTO> GetSubAccountLedgerDetails(string con, string BranchSchema, string CompanyCode, string BranchCode);

    #endregion GetSubAccountLedgerDetails...

    #region  GetAccountLedgerData...
    List<subAccountLedgerDTO> GetAccountLedgerData(string con, string SubLedgerName, string BranchSchema, string CompanyCode, string BranchCode);

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


    #region GetIssuedChequeNumbers...

    List<IssuedChequeDTO> GetIssuedChequeNumbers(string con, long bankId, string BranchSchema, string CompanyCode, string BranchCode);

    #endregion GetIssuedChequeNumbers...


    #region GetMainAccounthead....
    List<subAccountLedgerDTO> GetMainAccounthead(string con, string BranchSchema, string CompanyCode, string BranchCode);

    #endregion GetMainAccounthead...

    #region getCashbookData...
    List<cashBookDto> getCashbookData(string ConnectionString, string fromdate, string todate, string BranchSchema, string CompanyCode, string BranchCode);

    #endregion getCashbookData...

    #region GetBalances...
    List<AccountReportsDTO> GetBalances(string con, string LocalSchema, string fromDate, string todate, string groupType, string formname, string CompanyCode, string BranchCode);

    #endregion GetBalances...

    #region GetBankTransferTypes...
    List<BankTransferTypesDTO> GetBankTransferTypes(string ConnectionString, string branchSchema, string CompanyCode, string BranchCode);

    #endregion GetBankTransferTypes...

    #region GetChequeReturnDetails...
    List<ChequeEnquiryDTO> GetChequeReturnDetails(string con, string fromdate, string todate, string BranchSchema, string CompanyCode, string BranchCode, string GlobalSchema);

    #endregion GetChequeReturnDetails...

    #region GetIssuedChequeDetails...
    List<IssuedChequeDTO> GetIssuedChequeDetails(string con, long _BankId, long _ChqBookId, long _ChqFromNo, long _ChqToNo, string BranchSchema, string GlobalSchema, string CompanyCode, string BranchCode);

    #endregion GetIssuedChequeDetails...

    #region  GetUnUsedCheques...
    List<IssuedChequeDTO> GetUnUsedCheques(string con, long _BankId, long _ChqBookId, long _ChqFromNo, long _ChqToNo, string BranchSchema, string GlobalSchema, string CompanyCode, string BranchCode);

    #endregion GetUnUsedCheques...

    #region getCountry...
    List<CountryDTO> getCountry(string ConnectionString, string GlobalSchema);

    #endregion getCountry...

    #region  getState...
    List<StateDTO> getState(string ConnectionString, string GlobalSchema, long id);

    #endregion getState...

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

 

List<BankName> GetBankNameDetails(string connectionString,string globalSchema,string branchSchema,string BranchCode,string CompanyName);



List<BankDTO> GetBankntList(string ConnectionString, string GlobalSchema, string BranchSchema,string CompanyCode,string BranchCode);

List<Modeoftransaction> GetModeoftransactions(string ConnectionString, string GlobalSchema,string CompanyCode,string BranchCode);

 List<PartyDTO> GetPartyListGST(string ConnectionString, string GlobalSchema, string BranchSchema,string CompanyCode,string BranchCode);

 List<PartyDTO> GetPartyList(string ConnectionString, string GlobalSchema, string BranchSchema,string CompanyCode,string BranchCode);

 List<GstDTo> GetGstPercentages(string ConnectionString, string GlobalSchema,string CompanyCode,string BranchCode,string TaxesSchema);

 List<BankDTO> GetDebitCardNumbers(string ConnectionString, string GlobalSchema, string BranchSchema,string CompanyCode,string BranchCode);

 decimal getcashbalance(string ConnectionString, string BranchSchema,string CompanyCode,string BranchCode);

 decimal GetBankBalance(long recordid, string con, string BranchSchema,string CompanyCode,string BranchCode);

 decimal GetCashRestrictAmount(string type, string con, string GlobalSchema, string BranchSchema,string CompanyCode,string BranchCode);
  
  
  

List<PaymentVoucherReportDTO> GetPaymentVoucherReportData(
            string paymentId,
            string LocalSchema,
            string GlobalSchema,
            string Connectionstring,
            string CompanyCode,
            string BranchCode);

            List<GeneralReceiptSubDetails> GetPettyCashDetailsReportData(
    string paymentId,
    object contact_id,
    string Connectionstring,
    string LocalSchema,
    string GlobalSchema,
    string BranchCode,string CompanyCode);

            List<PaymentVoucherReportDTO> GetPettyCashReportData(string paymentId, string LocalSchema, string GlobalSchema, string Connectionstring,string CompanyCode,
                  string BranchCode);

                   List<AccountReportsDTO> GetAccountLedgerDetails(string con, string fromDate, string toDate, long pAccountId, long pSubAccountId, string BranchSchema, string GlobalSchema,string BranchCode,string CompanyCode);


                   List<PaymentVoucherReportDTO> GetChitReceiptCancelReportData(string paymentId, string LocalSchema, string GlobalSchema, string Connectionstring,string branchCode,string companyCode);

                    List<GeneralReceiptSubDetails> GetChitReceiptCancelDetailsReportData(string paymentId, object contact_id, string Connectionstring, string LocalSchema, string GlobalSchema,string branchCode,string companyCode);


    List<string> GetCheckDuplicateDebitCardNo(string ConnectionString, string GlobalSchema, BankInformationDTO lstBankInformation,string companycode,string branchcode);

    ChequesOnHandDTO GetBankBalance1(string brstodate,long recordid, string con, string BranchSchema,string branchCode,string companyCode);

    List<ReceiptReferenceDTO> GetPendingautoBRSDetails(string ConnectionString, string GlobalSchema, string BranchSchema, string allocationstatus,string BranchCode,string CompanyCode);

   List<InitialPaymentVoucherDTO> GetInitialPVDetails(string connectionstring, string fromdate, string todate, string transtype, string localSchema, string GlobalSchema,string CompanyCode,string Branchcode);


   List<ReceiptReferenceDTO> GetPendingautoBRSDetailsIssued(string ConnectionString, string BranchSchema, string allocationstatus,string BranchCode,string CompanyCode);

   List<BRSDto> GetBrs(string con, string fromDate, long pBankAccountId, string BranchSchema, string GlobalSchema,string branchCode,string companyCode);

   List<ReceiptReferenceDTO> GetIssuedCancelledChequesubi(string ConnectionString, string BrsFromDate, string BrsTodate, Int64 _BankId, string GlobalSchema, string BranchSchema,string BranchCode,string CompanyCode);

   List<AccountsDTO> GetCashAmountAccountWise(string type, string con, string GlobalSchema, string BranchSchema, string account_id, string transaction_date,string CompanyCode,string BranchCode);

  bool UnusedhequeCancel(string ConnectionString, string branchSchema, string globalSchema, IssuedChequeDTO issuedChequeDTO,string BranchCode,string CompanyCode);


  }
}

