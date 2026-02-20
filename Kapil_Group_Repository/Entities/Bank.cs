namespace Kapil_Group_Repository.Entities
{
    public class Bank : BaseEntity
    {
        public int tbl_mst_bank_configuration_id { get; set; }
        public string BankName { get; set; } = string.Empty;
        public string bankbranch { get; set; } = string.Empty;
        public string ifsccode { get; set; } = string.Empty;
        public string accounttype { get; set; } = string.Empty;


    }
    #region BankNames 
    public class BankNamesDetails : BaseEntity
    {
        public int BankAccountId { get; set; }
        public string BankName { get; set; } = string.Empty;
        public string BankBranch { get; set; } = string.Empty;
        public string IFSCCode { get; set; } = string.Empty;
        public string AccountType { get; set; } = string.Empty;

    }
    #endregion BankNames 

    #region companyNameandaddressDetails
    public class CompanyBranchDetails : BaseEntity
    {
        public int BranchId { get; set; }
        public string CompanyName { get; set; } = string.Empty;
        public string CompanyCode { get; set; } = string.Empty;
        public string BranchCode { get; set; } = string.Empty;
        public string UniqueBranchName { get; set; } = string.Empty;
        public string GstNumber { get; set; } = string.Empty;
        public string CinNumber { get; set; } = string.Empty;
        public string RegistrationAddress { get; set; } = string.Empty;
        public string BranchName { get; set; } = string.Empty;
        public int StateCode { get; set; }
        public string BranchAddress { get; set; } = string.Empty;
        public string ChitRegisterAddress { get; set; } = string.Empty;
        public bool TransactionLockStatus { get; set; }
        public string ApplicationVersionNo { get; set; } = string.Empty;
        public bool UncommencedSJVAllowStatus { get; set; }
        public bool InchargeValidateOnSubscriber { get; set; }
        public string DisplayName { get; set; } = string.Empty;
        public bool FinClosingJVAllowStatus { get; set; }
        public string LegalCellName { get; set; } = string.Empty;
        public string LegalCellDues { get; set; } = string.Empty;
        public bool IsLegalGeneralReceiptAllowStatus { get; set; }
        public string ChitFundActNumber { get; set; } = string.Empty;
        public string LegalDeptAddress { get; set; } = string.Empty;
        public bool IsOnlyFeeReceiptAllowStatus { get; set; }
        public string KgmsStartTime { get; set; } = string.Empty;
        public string KgmsEndTime { get; set; } = string.Empty;
        public int NonPaymentSubscribersGracePeriod { get; set; }
        public int LegalCellDuesDays { get; set; }
        public int MaxChitsPerContact { get; set; }
        public bool DaywiseAuctions { get; set; }
        public bool IcCalculatedPenaltyEditable { get; set; }
        public int OnlineProcessBackDateDays { get; set; }
        public bool IsContactEditableAllowStatus { get; set; }
        public bool FixedChitStatus { get; set; }
        public bool IsAutoBrsImpsApplicable { get; set; }
        public bool IsSubscriberNomineeAllocation { get; set; }
        public bool BiometricDateLockStatus { get; set; }
    }


    #endregion companyNameandaddressDetails

    #region BankConfigurationdetails
    public class BankConfigurationDetails : BaseEntity
    {
        public int BankConfigurationId { get; set; }
        public bool IsPrimary { get; set; }
        public bool IsFormanBank { get; set; }
        public bool IsForemanPaymentBank { get; set; }
        public bool IsInterestPaymentBank { get; set; }
    }

    #endregion BankConfigurationdetails

    #region ViewChequeManagementDetails 
    public class ChequeManagementDTO : BaseEntity
    {
        public int ptotalrecords { get; set; }
        public long pbankconfigurationid { get; set; }
        public long pchequebookid { get; set; }
        public int pnoofcheques { get; set; }
        public long pchequefromnumber { get; set; }
        public long pchequetonumber { get; set; }
        public bool pchequegeneratestatus { get; set; }
        public string pbankname { get; set; } =string.Empty;
        public string paccountnumber { get; set; } = string.Empty;
        public bool pstatus { get; set; }
        public string pchequestatus { get; set; } = string.Empty;
    }

    #endregion ViewChequeManagementDetails


    #region ExistingChequeCount
    public class ExistingChequeCountDTO : BaseEntity
    {
        public int Count { get; set; }
    }

    #endregion ExistingChequeCount


    #region BankUPIDetails...
    public class BankUPIDetails : BaseEntity
    {
        public int recordid { get; set; }

        public string upiname { get; set; } = string.Empty;

    }

    #endregion BankUPIDetails....

    #region  ViewBankInformationDetails...
    public class ViewBankInformationDetails : BaseEntity
    {
        public int tbl_mst_bank_configuration_id { get; set; }

        public int bank_id { get; set; }

        public string BankName { get; set; } = string.Empty;

        public string account_number { get; set; } = string.Empty;
        public string account_name { get; set; } = string.Empty;
        public bool status { get; set; }
        public bool is_debitcard_applicable { get; set; }
        public bool is_upi_applicable { get; set; }
        public bool isprimary { get; set; }
        public bool isformanbank { get; set; }
        public bool is_foreman_payment_bank { get; set; }

        public bool is_interest_payment_bank { get; set; }
    }
    #endregion ViewBankInformationDetails...

    #region GeneralReceiptsData...
    public class GeneralReceiptsData : BaseEntity
    {

        public string receipt_date { get; set; } = string.Empty;
        public string receipt_number { get; set; } = string.Empty;

        public string modeof_receipt { get; set; } = string.Empty;
        public string bank_name { get; set; } = string.Empty;
        public string reference_number { get; set; } = string.Empty;
        public int totalreceivedamount { get; set; }
        public string narration { get; set; } = string.Empty;
        public string contactname { get; set; } = string.Empty;
        public bool is_tds_applicable { get; set; }
        public string tdssection { get; set; } = string.Empty;
        public string pannumber { get; set; } = string.Empty;
        public string tds_calculation_type { get; set; } = string.Empty;
        public int tdspercentage { get; set; }
        public string typeofreceipt { get; set; } = string.Empty;
        public string file_name { get; set; } = string.Empty;

        public string clear_date { get; set; } = string.Empty;

        public string cheque_date { get; set; } = string.Empty;
        public string deposited_date { get; set; } = string.Empty;
    }
    #endregion GeneralReceiptsData...

    #region ViewBankInformation...

    public class ViewBankInformation : BaseEntity
    {
        public int recordid { get; set; }
        public string bank_date { get; set; } = string.Empty;
        public string account_number { get; set; } = string.Empty;
        public string bank_name { get; set; } = string.Empty;

        public string account_name { get; set; } = string.Empty;
        public string bank_branch { get; set; } = string.Empty;
        public string ifsccode { get; set; } = string.Empty;
        public int overdraft { get; set; }
        public int openingbalance { get; set; }
        public bool is_debitcard_applicable { get; set; }
        public bool is_upi_applicable { get; set; }
        public bool statusname { get; set; }
        public string typeofoperation { get; set; } = string.Empty;

        public string account_type { get; set; } = string.Empty;
        public string opening_jvno { get; set; } = string.Empty;
        public string OpeningBalanceType { get; set; } = string.Empty;


    }

    #endregion ViewBankInformation...

    #region AvailableChequeCount...

    public class AvailableChequeCount : BaseEntity
    {

        public int Count { get; set; }


    }

    #endregion AvailableChequeCount...


    #region PettyCashExistingData...

    public class PettyCashExistingData : BaseEntity
    {

        public string PaymentDate { get; set; } = string.Empty;
        public string PaymentId { get; set; } = string.Empty;

        public string ModeOfPayment { get; set; } = string.Empty;
        public string TypeOfPayment { get; set; } = string.Empty;
        public string BankName { get; set; } = string.Empty;

        public string ReferenceNumber { get; set; } = string.Empty;

        public int TotalPaidAmount { get; set; }

    }

    #endregion PettyCashExistingData...

    #region PaymentVoucherExistingData..

    public class PaymentVoucherDetails : BaseEntity
    {
        public string PaymentDate { get; set; } = string.Empty;
        public string PaymentId { get; set; } = string.Empty;
        public string ModeOfPayment { get; set; } = string.Empty;
        public string TypeOfPayment { get; set; } = string.Empty;
        public string BankName { get; set; } = string.Empty;
        public string ReferenceNumber { get; set; } = string.Empty;
        public decimal TotalPaidAmount { get; set; } = 0;
    }

    #endregion PaymentVoucherExistingData..

    #region ProductnamesandHSNcodes..
    public class ProductNamesAndHSNCodesDetails
    {
        public string ProductName { get; set; } = string.Empty;
        public string HSNCode { get; set; } = string.Empty;
    }


    #endregion ProductnamesandHSNcodes..


    #region getReceiptNumber..
    public class getReceiptNumber
    {
        public int tbl_trans_pettycash_voucher_id { get; set; }
        public string payment_number { get; set; } = string.Empty;
    }


    #endregion getReceiptNumber..


    #region GetBankUPIList
    public class BankUPIListDetails : BaseEntity
    {
        public string AccountName { get; set; } = string.Empty;
        public int AccountId { get; set; }
        public decimal AccountBalance { get; set; }
    }

    #endregion GetBankUPIList

  #region GetCAOBranchList
    public class CAOBranchListDetails : BaseEntity
    {
        public int BranchConfigurationId { get; set; }
        public string BranchCode { get; set; } = string.Empty;
        public string BranchName { get; set; } = string.Empty;
    }


 #endregion GetCAOBranchList

 #region GstDTo...
    public class GstDTo
    {
        public object pRecordId { get; set; } 
        public object pgstpercentage { get; set; } 
        public object pigstpercentage { get; set; } 
        public object pcgstpercentage { get; set; } 
        public object psgstpercentage { get; set; } 
        public object putgstpercentage { get; set; } 
        public object pState { get; set; }
        public object pStateId { get; set; }
        public object pgstno { get; set; }
        public object pgsttype { get; set; }
        public object pisgstapplicable { get; set; }
        public object gstnumber { get; set; }
    }

    #endregion GstDTo...


   #region AccountReportsDTO...
    public class AccountReportsDTO
    {


        public string pparentname { get; set; }

        public string pFormName { get; set; }
        public object ptransactiondate { get; set; }
        public string ptransactionno { get; set; }
        public string pparticulars { get; set; }
        public string pdescription { get; set; }
        public double pdebitamount { get; set; }
        public double pcreditamount { get; set; }
        public double popeningbal { get; set; }
        public double pclosingbal { get; set; }
        public double pcashtotal { get; set; }
        public double pchequetotal { get; set; }
        public string pmodeoftransaction { get; set; }
        public long precordid { get; set; }
        public string paccountname { get; set; }
        public long paccountid { get; set; }
        public long pparentid { get; set; }
        public string pBalanceType { get; set; }
        public object groupcode { get; set; }
    }

    #endregion AccountReportsDTO...

    #region AccountsDTO...
    public class AccountsDTO
    {
        public object psubledgerid { get; set; }
        public object psubledgername { get; set; }
        public object pledgerid { get; set; }
        public object pledgername { get; set; }

        public object id { get; set; }
        public object text { get; set; }

        public object ptranstype { get; set; }
        public object accountbalance { get; set; }
        public object pAccounttype { get; set; }
        public object legalcellReceipt { get; set; }
        public object pbranchcode { get; set; }
        public object pbranchtype { get; set; }
        public object groupcode { get; set; }
        public object chitgroupid { get; set; }

    }

    #endregion AccountsDTO...


   #region subAccountLedgerDTO...
    public class subAccountLedgerDTO : AccountReportsDTO
    {
        public List<AccountReportsDTO> plstSubAccountLedger { get; set; }

        public object Acc_Id { get; set; }
        public object Account_name { get; set; }
        public object Parent_Acc_Id { get; set; }
        public object Parent_Account_name { get; set; }
        public object Parent_Account_balance { get; set; }
        public object transaction_date { get; set; }
        public object transaction_no { get; set; }
        public object particulars { get; set; }
        public object debit_amount { get; set; }
        public object credit_amount { get; set; }
        public object description { get; set; }
        public object balance { get; set; }
        public object balance_type { get; set; }
        public object Account_type { get; set; }
        public object chracc_type { get; set; }
        public string paccountname { get; set; }
    }

    #endregion subAccountLedgerDTO...

  #region IssuedChequeDTO...
   public class IssuedChequeDTO 
     {
        public long pchequeNoFrom { get; set; }
        public long pchequeNoTo { get; set; }
        public long pchkBookId { get; set; }
    public List<IssuedChequeItemDTO> lstIssuedCheque { get; set; }
        public string pchqfromto { get; set; }
        public string pchequenumber { get; set; }
        public string ppaymentid { get; set; }
        public string pparticulars { get; set; }
        public object ppaymentdate { get; set; }
        public object pcleardate { get; set; }
        public string pstatus { get; set; }
        public Int64 ppaidamount { get; set; }
        public string pbankname { get; set; }
        public string pchequestatus { get; set; }
        public long pbankaccountid { get; set; }
        public string branchSchema { get; set; }
    }

    #endregion IssuedChequeDTO...
    public class IssuedChequeItemDTO
{
    public long pbankaccountid { get; set; }
    public long pchkBookId { get; set; }
    public string pchequenumber { get; set; }
}


  #region cashBookDto...
     public class cashBookDto : AccountReportsDTO
    {
        public List<AccountReportsDTO> plstcashbookdata { get; set; }
        public List<AccountReportsDTO> plstcashchequetotal { get; set; }
    }

  #endregion cashBookDto...

  #region BankTransferTypesDTO....
     public class BankTransferTypesDTO
    {
        public object bankttransferid { get; set; }
        public object banktransfername { get; set; }
        public object frombankaccountid { get; set; }
        public object tobankaccountid { get; set; }
        public object bankconfigurationid { get; set; }
        public object accountname { get; set; }
    }

    #endregion BankTransferTypesDTO...
    
    #region ChequeEnquiryDTO...
     public class ChequeEnquiryDTO
    {
        public string preferencenumber { get; set; }
        public string pparticulars { get; set; }
        public string preceiptid { get; set; }
        public object pchequedate { get; set; }
        public string pbankname { get; set; }
        public object pdepositeddate { get; set; }
        public object pcleardate { get; set; }
        public Int64 ptotalreceivedamount { get; set; }
        public string pchequesstatus { get; set; }
        public string pdepositbankname { get; set; }
        public List<ChequeEnquiryDTO> plstChequeEnquiry { get; set; }

        public object psubscribername { get; set; }
        public object pchitno { get; set; }
        public object paddress { get; set; }

        public object pchequereturnchargesamount { get; set; }
        public object pbranchname { get; set; }

        public object pvoucherno { get; set; }
        public object pcreditaccountname { get; set; }
        public object pdebitaccountname { get; set; }
        public object pReceiptChqAmount { get; set; }

    }

    #endregion ChequeEnquiryDTO...
    
    #region CountryDTO...
     public class CountryDTO 
    {
        public object tbl_mst_country_id { get; set; }
        public object country_name { get; set; }
        public object country_code { get; set; }
        public object status { get; set; }
    }

    #endregion CountryDTO...

    #region StateDTO...
    public class StateDTO 
    {
        public object tbl_mst_state_id { get; set; }
        public object country_id { get; set; }
        public object country_name { get; set; }
        public object state_code { get; set; }
        public object state_name { get; set; }
        public object union_territory { get; set; }
        public object status { get; set; }
    }

    #endregion StateDTO...

    #region District..
    public class District
    {
        public object tbl_mst_district_id { get; set; }
        public object district_name { get; set; }
        public object status { get; set; }
    }

    #endregion District...

    #region Getformnamedetails...

    public class Formnamedetails:BaseEntity
    {
        public string formNames {get;set;}= string.Empty;
    }

    #endregion Getformnamedetails...



     public class PaymentVoucherReportDTO
    {
        //
        public object pChequenumber { get; set; }
        public object ptypeofpayment { get; set; }
        public object pcontactid { get; set; }
        public object pcontactname { get; set; }

        public object ppaymentid { get; set; }
        public object preceiptno { get; set; }
        public object ppaymentdate { get; set; }
        public object pnarration { get; set; }
        public object pmodofPayment { get; set; }
        public object pemployeename { get; set; }
        public object pbankaccount { get; set; }
        public object accountname { get; set; }
        public object parentaccountname { get; set; }
        public object transaction_amount { get; set; }
        public List<GeneralReceiptSubDetails> ppaymentslist { get; set; }
        public object pusername { get; set; }

    }


    public class GeneralReceiptSubDetails
    {
        public object pLedgeramount { get; set; }
        public object pAccountname { get; set; }
        public object pgstcalculationtype { get; set; }
        public object pcgstamount { get; set; }
        public object psgstamount { get; set; }
        public object pigstamount { get; set; }
        public object ptdscalculationtype { get; set; }
        public object ptdsamount { get; set; }
        public object state_code { get; set; }
    }

    public class BankUPI
    {
        public object pRecordid { get; set; }
        public object pUpiname { get; set; }
        public object pUpiid { get; set; }
        public object pBankconfigurationId { get; set; }
    }

    public class ChequesDTO 
    {
        public Int64 pRecordid { get; set; }
        public Int64 pChequenumber { get; set; }
        public Int64 pChqbookid { get; set; }
        public object pchequeamount { get; set; }
    }
   public class AccountsMasterDTO
    {
        public List<Modeoftransaction> modeofTransactionslist { get; set; }//
        public List<BankDTO> banklist { get; set; }
        public List<BankDTO> adjustmentbanklist { get; set; }


        public List<AccountsDTO> accountslist { get; set; }
        public List<PartyDTO> partylist { get; set; }
        public List<GstDTo> Gstlist { get; set; }
        public List<TdsSectionDTO> lstTdsSectionDetails { get; set; }
        public List<BankDTO> bankdebitcardslist { get; set; }
        public List<BankUPI> bankupilist { get; set; }
        public List<ChequesDTO> chequeslist { get; set; }
        public List<ChequesDTO> adjustmentchequeslist { get; set; }

        public List<GstDTo> statelist { get; set; }
        public object accountbalance { get; set; }
        public object cashbalance { get; set; }
        public object bankbalance { get; set; }
        public object bankpassbookbalance { get; set; }
        public List<PartyDTO> walletlist { get; set; }

        public object CashRestrictAmount { get; set; }
    }

    public class TdsSectionDTO 
    {
        public object istdsapplicable;

        public object pRecordid { get; set; }
        public object pTdsSection { get; set; }
        public object sectionname { get; set; }
        public object pTdsPercentage { get; set; }
        public object withpanpercentage { get; set; }
        public object withcompanypanpercentage { get; set; }
        public object withoutpanpercentage { get; set; }
        public object monthlylimitamount { get; set; }
        public object yearlylimitamount { get; set; }
    }

      public class BankDTO
    {
        public object pCardNumber { get; set; }
        public object pbankname { get; set; }
        public object pbankid { get; set; }
        public object pbranchname { get; set; }
        public object pdepositbankid { get; set; }
        public object pdepositbankname { get; set; }
        public object pbankbalance { get; set; }
        public object pbankaccountnumber { get; set; }
        public object pfrombrsdate { get; set; }
        public object ptobrsdate { get; set; }
        public object pbankpassbookbalance { get; set; }
        public object paccountid { get; set; }
        public object pisprimary { get; set; }
        public object pisformanbank { get; set; }
        public object isForemanPaymentBank { get; set; }
        public object pisInterestPaymentBank { get; set; }
    }

     public class Modeoftransaction 
    {
        public object pRecordid { get; set; }
        public object pmodofPayment { get; set; }
        public object pmodofreceipt { get; set; }
        public object ptranstype { get; set; }
        public object ptypeofpayment { get; set; }
        public object pchqonhandstatus { get; set; }
        public object pchqinbankstatus { get; set; }
    }

     public class JvListDTO : AccountReportsDTO
    {
        public string treeStatus { get; set; }
        public int level { set; get; }
        public bool formshoworhide { get; set; }

        public int roleid { get; set; }
        public int userId { get; set; }
        public string userName { get; set; }
        public string moduleid { get; set; }
        public string branchid { get; set; }

        public List<JvListDTO> plstJvList { get; set; }
        public string id { get; set; }
        public object parentId { get; set; }
        public string formOrModulename { get; set; }
    }

    public class BankBookDTO : AccountReportsDTO
    {
        public long pbankaccountid { get; set; }
        public string pbankname { get; set; }
        public List<AccountReportsDTO> plstBankBook = new List<AccountReportsDTO>();
    }

     public class JournalVoucherReportDTO
    {
        public object pJvnumber { get; set; }
        public object pJvDate { get; set; }
        public object pCreditAmount { get; set; }
        public object pDebitamount { get; set; }
        public object pNarration { get; set; }
        public object pParticulars { get; set; }
        public object pContactName { get; set; }
        public List<JournalVoucherReportDTO> plstJournalVoucherReportDTO { get; set; }
    }

    public class SVOnameDTO
    {
        public object svoname { get; set; }
        public object svoid { get; set; }
        public object mvoname { get; set; }
        public object referred_by { get; set; }
        public object mvoid { get; set; }

    }   

     public class ModulesDTO
    {
        public long pModuleId { get; set; }
        public string pModulename { get; set; }
    }

    public class PartyDTO
    {
        public object ppartypannumber;

        public object ppartyid { get; set; }
        public object ppartyname { get; set; }
        public object ppartyemailid { get; set; }
        public object ppartycontactno { get; set; }
        public object ppartyreferenceid { get; set; }
        public object ppartyreftype { get; set; }

        public object address1 { get; set; }
        public object area { get; set; }
        public object city_name { get; set; }
        public object district_name { get; set; }
        public object state_name { get; set; }
        public object state_code { get; set; }
        public object pan_no { get; set; }
        public object aadharno { get; set; }
        public object gstno { get; set; }
        public object formname { get; set; }


    }


     
   #region GetBankNameDetails...

    public class BankName:BaseEntity
    {
        

    public int tbl_mst_bank_configuration_id { get; set; }
    public string BankNames { get; set; } = string.Empty;
    public string bankbranch { get; set; } = string.Empty;
    public string ifsccode { get; set; } = string.Empty;
    public string accounttype { get; set; } = string.Empty;


    }

    #endregion GetBankNameDetails..



    public class BankInformationDTO :BankdebitcarddtlsDTO
    {

        public List<BankInformationAddressDTO> lstBankInformationAddressDTO { get; set; }
        public List<BankdebitcarddtlsDTO> lstBankdebitcarddtlsDTO { get; set; }
        public List<BankUPI> lstBankUPI { get; set; }
        public List<ChequeManagementDTO> lstChequemanagementDTO { get; set; }
        public List<ChequesDTO> lstCheques { get; set; }
        public object pRecordid { get; set; }
        public object pBankID { get; set; }
      //  public List<GlobalBankDTO> lstGlobalBankDTO { set; get; }
        public object pBankname { get; set; }
        public object pBankbranch { get; set; }
        public object pAccountname { get; set; }
        public object pAcctountype { get; set; }
        public object pAccountnumber { get; set; }
        public object pBankdate { get; set; }
        public object pOverdraft { get; set; }
        public object pAccountid { get; set; }
        public object pIfsccode { get; set; }
        public object pSwiftccode { get; set; }
        public object pOpeningBalance { get; set; }
        public object pIsdebitcardapplicable { get; set; }
        public object pIsupiapplicable { get; set; }
        public object pOpeningBalanceType { get; set; }
        public object pChqegeneratestatus { get; set; }
        public object popeningjvno { get; set; }
        public object branchSchema { get; set; }
        public object isprimary { get; set; }
        public object isformanbank { get; set; }
        public object isforemanpaymentbank { get; set; }
        public object isintrestpaymentbank { get; set; }
        public object pChequeGenerateDate { get; set; }
    }

    public class BankInformationAddressDTO 
    {
        public Int64 pRecordid { get; set; }
        public string pAddressType { get; set; }
        public long pBankId { get; set; }
    }

    public class BankdebitcarddtlsDTO 
    {
        public object pRecordid { get; set; }
        public object pCardNo { get; set; }
        public object pCardName { get; set; }
        public object pValidfrom { get; set; }
        public object pValidto { get; set; }
        public object pBankId { get; set; }
    }

     public class ChequesOnHandDTO 
    {
        public object ptransactiondate { get; set; }
        public object pchequecleardate { get; set; }
        public object pcaobranchcode { get; set; }
        public object pcaobranchname { get; set; }
        public object pcaobranchid { get; set; }

        public object pfrombrsdate { get; set; }
        public object ptobrsdate { get; set; }
        public object _BankBalance { get; set; }
        public object chequestype { get; set; }
        public object _CashBalance { get; set; }
        public object banknameForLegal { get; set; }
        public List<ReceiptReferenceDTO> pchequesOnHandlist { get; set; }
        public List<ReceiptReferenceDTO> pchequesclearreturnlist { get; set; }
        public List<ChequesOtherDetailsDTO> pchequesotherslist { get; set; }
        public object auto_brs_type_name { get; set; }


    }

    public class ReceiptReferenceDTO : GeneralreceiptDTO
    {
        //public object pchequesonhandstatus { get; set; }
        public object pdepositstatus { get; set; }
        public object pcancelstatus { get; set; }
        public object preturnstatus { get; set; }
        public object pbranchname { get; set; }
        public object pchequestatus { get; set; }
        //public object pdepositedBankid { get; set; }
        //public object pdepositedBankName { get; set; }
        public object pcancelcharges { get; set; }
        public object pactualcancelcharges { get; set; }
        public object pledger { get; set; }
        public string cancelstatus { get; set; }
        public string returnstatus { get; set; }
        public string clearstatus { get; set; }
        //public object pdepositeddate { get; set; }
        //public object pcleardate { get; set; }
        public object chqueno { get; set; }
        public object issueddate { get; set; }
        public object chitgroupcode { get; set; }
        public object chitgroupid { get; set; }
        public object ticketno { get; set; }
        public object chequeamount { get; set; }
        public object zpdaccountid { get; set; }
        public object installmentno { get; set; }
        public object schemesubscriberid { get; set; }
        public object contactid { get; set; }
        public object schemetype { get; set; }
        public object checksentryrecordid { get; set; }
        public object cheque_bank { get; set; }
        public object selfchequestatus { get; set; }
        public object branch_name { get; set; }
        public object receipt_branch_name { get; set; }
        public object subscriber_details { get; set; }
        public object ChitReceiptNo { get; set; }
        public object total_count { get; set; }
        public object transactionNo { get; set; }
        public object transactiondate { get; set; }
        public object chitstatus { get; set; }
        public object chitgroupstatus { get; set; }
        public object receiptnumbers { get; set; }

        public object pdepositedBankid { get; set; }
        public object pdepositedBankName { get; set; }
        // added these above two objects on 29.10.2024 for caodeposited bank purpose
        public object preferencetext { get; set; }
        public object preceiptype { get; set; }
        public object puploadeddate { get; set; }
        public object subscriberbankaccountno { get; set; }
        public object pkgmsreceiptdate { get; set; }
    }

    public class ChequesOtherDetailsDTO
    {
        public object ptransactionnumber { get; set; }
        public object ptransactiondate { get; set; }
        public object particulars { get; set; }
        public object debitamount { get; set; }
        public object creditamount { get; set; }
        public object accountname { get; set; }
        public object chequereturncharges { get; set; }
    }

     public class GeneralreceiptDTO : ModeofPaymentDTO
    {
        public object preceiptid { get; set; }
        public object preceiptdate { get; set; }
        public object pmodofreceipt { get; set; }
        public object ptotalreceivedamount { get; set; }
        public object pnarration { get; set; }
        public object ppartyname { get; set; }
        public object ppartyid { get; set; }
        public object pistdsapplicable { get; set; }
        public object pTdsSection { get; set; }
        public object pTdsPercentage { get; set; }
        public object ptdsamount { get; set; }
        public object ptdscalculationtype { get; set; }
        public object ppartypannumber { get; set; }
        public object ppartyreftype { get; set; }
        public object ppartyreferenceid { get; set; }
        public List<ReceiptsDTO> preceiptslist { get; set; }
        public object pFilename { get; set; }
        public object pFilepath { get; set; }
        public object pFileformat { get; set; }
        public object pCleardate { get; set; }
        public object pdepositeddate { get; set; }
        public object ptdsaccountid { get; set; }
        public object preceiptrecordid { get; set; }
        public object pTdsSectionId { get; set; }
        public object groupcode { get; set; }
        public object preceiptno { get; set; }
        public object pchequestatus { get; set; }
        public object preferencetext { get; set; }
        public object formname { get; set; }
        public object chitpaymentid { get; set; }
        public object adjustmentid { get; set; }
    }
    
     public class ReceiptsDTO : AccountsDTO
    {
        public object pamount { get; set; }
        public object pgsttype { get; set; }
        public object pgstcalculationtype { get; set; }
        public object pgstpercentage { get; set; }
        public object pigstamount { get; set; }
        public object pcgstamount { get; set; }
        public object psgstamount { get; set; }
        public object putgstamount { get; set; }
        public object pState { get; set; }
        public object pStateId { get; set; }
        public object pgstno { get; set; }

        public object pisgstapplicable { get; set; }

        public object ptdsamountindividual { get; set; }

        public object pTdsSection { get; set; }
        public object pTdsPercentage { get; set; }
        public object preferencetext { get; set; }
    }

    public class ModeofPaymentDTO : BankUPI
    {
        //public object pBankId { get; set; }
        public object pBankName { get; set; }

        public object pbranchname { get; set; }
        public object ptranstype { get; set; }
        public object ptypeofpayment { get; set; }
        public object pChequenumber { get; set; }
        public object pchequedate { get; set; }
        public object pchequedepositdate { get; set; }
        public object pchequecleardate { get; set; }
        public object pbankid { get; set; }
        public object branchid { get; set; }
        public object pCardNumber { get; set; }
        public object pdepositbankid { get; set; }
        public object pdepositbankname { get; set; }
        public object pAccountnumber { get; set; }
        public object ChallanaNo { get; set; }
    }

      public class InitialPaymentVoucherDTO
    {
        public object pPayment_number { get; set; }
        public object pGroupcode { get; set; }
        public object pTicketno { get; set; }
        public object pSubscribername { get; set; }
        public object pTransactiondate { get; set; }
        public object pPaymentdate { get; set; }
        public object pPaidamount { get; set; }
        public object pChequeno { get; set; }
        public object pBankname { get; set; }

    }

    public class BRSDto : AccountReportsDTO
    {
        public string pChequeNumber { get; set; }
        public decimal ptotalreceivedamount { get; set; }
        public string pBankName { get; set; }
        public string pBranchName { get; set; }
        public string pGroupType { get; set; }
        public decimal pBankBookBalance { get; set; }
        public object selfchequestatus { get; set; }
        public object receiptrecordid { get; set; }
        public object receiptid { get; set; }
        public object receiptdate { get; set; }
        public object total_received_amount { get; set; }
        public object contact_name { get; set; }
        public object modeof_receipt { get; set; }
        public object reference_number { get; set; }
        public object chequedate { get; set; }
        public object deposit_status { get; set; }
        public object depositebankid { get; set; }
        public object depositeddate { get; set; }
        public object clear_date { get; set; }
        public object cheque_bank { get; set; }
        public object receipt_branch_name { get; set; }
        public object received_form { get; set; }
        public object chit_receipt_number { get; set; }
        public object transaction_no { get; set; }
        public object transaction_date { get; set; }
        public List<BRSDto> lstBRSDto { get; set; }
        public List<BRSDto> lstBRSDto1 { get; set; }
        public object ptransactionTime { get; set; }
        public object pUserName { get; set; }
        
        // added below on 25.12.2024
        public object pbrsdate { get; set; }
        public object pbankbalance { get; set; }
        public object preferencetext { get; set; }

    }

      public class TDSJVDetails 
    {
        public object account_id { get; set; }
        public object account_trans_type { get; set; }
        public object particulars { get; set; }
        public object debit_amount { get; set; }
        public object credit_amount { get; set; }
        public object payroll_month { get; set; }
        public object transaction_date { get; set; }
        public object jv_type { get; set; }
        public object creditledger { get; set; }
    }



        public class ChequesRowCountDTO
    {
        public object cheques_count { get; set; }
        public object others_count { get; set; }
        public object total_count { get; set; }
        public object clear_count { get; set; }
        public object return_count { get; set; }
        public object cancel_count { get; set; }

        public object clearedcount { get; set; }
        public object pendingcount { get; set; }
        public object totalcount { get; set; }
        public object clearedamount { get; set; }
        public object pendingamount { get; set; }
        public object totalamount { get; set; }

        public object matchedcount { get; set; }

    }



    
  

    #region subledgerdata...

    public class SubLedgerdata : BaseEntity
    {
        public long AccountId { get; set; }

        public string AccountName { get; set; } = string.Empty;

        public decimal Balance { get; set; }

    }

    #endregion subledgerdata...

    #region  BrsBankBalance...

    public class BrsBankBalance : BaseEntity
    {
        public int BankAccountId { get; set; }

        public decimal BankBookBalance { get; set; }
    }

    #endregion BrsBankBalance...

    #region ChequeReturnCharges...

    public class ChequeReturnCharges : BaseEntity
    {
        public decimal ChargeAmount { get; set; }
    }

    #endregion ChequeReturnCharges...

    #region JournalVoucherData...

    public class JournalVoucherData : BaseEntity

    {

        public long JournalVoucherId { get; set; }
        public string JVDate { get; set; } = string.Empty;
        public string JournalVoucherNo { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public string Narration { get; set; } = string.Empty;

    }


    #endregion JournalVoucherData...

    #region GlobalBanks...

    public class GlobalBanks : BaseEntity
    {
        public long BankId { get; set; }
        public string BankName { get; set; } = string.Empty;

    }

    #endregion GlobalBanks...

    #region ChequeCancelDetails...

    public class ChequeCancelDetails : BaseEntity
    {
        public string DepositedDate { get; set; } = string.Empty;
        public string ReferenceNumber { get; set; } = string.Empty;
        public decimal TotalReceivedAmount { get; set; }
        public string BankName { get; set; } = string.Empty;
        public string ReceiptNumber { get; set; } = string.Empty;
        public string Particulars { get; set; } = string.Empty;
        public string ReceiptDate { get; set; } = string.Empty;

    }

    #endregion ChequeCancelDetails...
	
	
}