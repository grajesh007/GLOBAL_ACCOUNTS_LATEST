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

    public class ChequeManagementDTO
    {
        public int ptotalrecords { get; set; }
        public long pbankconfigurationid { get; set; }
        public long pchequebookid { get; set; }
        public int pnoofcheques { get; set; }
        public long pchequefromnumber { get; set; }
        public long pchequetonumber { get; set; }
        public bool pchequegeneratestatus { get; set; }
        public string pbankname { get; set; }
        public string paccountnumber { get; set; }
        public bool pstatus { get; set; }
        public string pchequestatus { get; set; }
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

    // public class CAOBranchDetails : BaseEntity
    // {
    //     public int BranchConfigurationId { get; set; }
    //     public string BranchCode { get; set; } = string.Empty;
    //     public string BranchName { get; set; } = string.Empty;
    // }

    public class CAOBranchListDetails : BaseEntity
    {
        public int BranchConfigurationId { get; set; }
        public string BranchCode { get; set; } = string.Empty;
        public string BranchName { get; set; } = string.Empty;
    }


    #endregion GetCAOBranchList


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



public class IssuedChequeDTO 
    {
        public long pchequeNoFrom { get; set; }
        public long pchequeNoTo { get; set; }
        public long pchkBookId { get; set; }
        public List<IssuedChequeDTO> lstIssuedCheque { get; set; }
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




     public class cashBookDto : AccountReportsDTO
    {
        public List<AccountReportsDTO> plstcashbookdata { get; set; }
        public List<AccountReportsDTO> plstcashchequetotal { get; set; }
    }
    // public class PaymentVoucherReportDTO
    //     {
    //         //
    //         public object pChequenumber { get; set; }
    //         public object ptypeofpayment { get; set; }
    //         public object pcontactid { get; set; }
    //         public object pcontactname { get; set; }

    //         public object ppaymentid { get; set; }
    //         public object preceiptno { get; set; }
    //         public object ppaymentdate { get; set; }
    //         public object pnarration { get; set; }
    //         public object pmodofPayment { get; set; }
    //         public object pemployeename { get; set; }
    //         public object pbankaccount { get; set; }
    //         public object accountname { get; set; }
    //         public object parentaccountname { get; set; }
    //         public object transaction_amount { get; set; }
    //         public List<GeneralReceiptSubDetails> ppaymentslist { get; set; }
    //         public object pusername { get; set; }

    //     }

    //      public class GeneralReceiptSubDetails
    //     {
    //         public object pLedgeramount { get; set; }
    //         public object pAccountname { get; set; }
    //         public object pgstcalculationtype { get; set; }
    //         public object pcgstamount { get; set; }
    //         public object psgstamount { get; set; }
    //         public object pigstamount { get; set; }
    //         public object ptdscalculationtype { get; set; }
    //         public object ptdsamount { get; set; }
    //         public object state_code { get; set; }
    //     }
}