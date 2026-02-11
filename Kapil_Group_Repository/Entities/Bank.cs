namespace Kapil_Group_Repository.Entities;

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
public class ViewChequeManagementDTO : BaseEntity
{
    public int BankConfigurationId { get; set; }
    public int ChequeBookId { get; set; }
    public int NoOfCheques { get; set; }
    public int ChequeFromNumber { get; set; }
    public int ChequeToNumber { get; set; }
    public bool ChequeGenerateStatus { get; set; }
    public string BankName { get; set; } = string.Empty;
    public string AccountNumber { get; set; } = string.Empty;
    public bool Status { get; set; }
    public string ChequeStatus { get; set; } = string.Empty;
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
    public int recordid {get; set;}

    public string upiname {get;set;} = string.Empty;
    
    
}

#endregion BankUPIDetails....

#region  ViewBankInformationDetails...
public class ViewBankInformationDetails : BaseEntity
{
   public int tbl_mst_bank_configuration_id {get; set;}

   public int bank_id {get; set;}

   public string BankName {get; set;}= string.Empty;

   public string account_number {get; set;}=string.Empty;
   public string account_name {get;set;} = string.Empty;
   public bool status {get; set;}
   public bool is_debitcard_applicable {get; set;}
   public bool is_upi_applicable {get; set;}
   public bool isprimary {get; set;}
   public bool isformanbank {get; set;}
   public bool is_foreman_payment_bank  {get; set;}

   public bool is_interest_payment_bank {get; set;}
}
#endregion ViewBankInformationDetails...

#region GeneralReceiptsData...
public class GeneralReceiptsData :BaseEntity
{
    
    public string receipt_date {get; set;} = string.Empty;
    public string receipt_number {get; set;} = string.Empty;

    public string modeof_receipt {get;set;} = string.Empty;
    public string bank_name {get;set;} =string.Empty;
    public string reference_number {get; set;} = string.Empty;
    public int totalreceivedamount {get; set;}
    public string narration {get;set;}=string.Empty;
    public string contactname {get; set;} = string.Empty;
    public bool is_tds_applicable {get;set;}
    public string tdssection {get;set;} = string.Empty;
    public string pannumber {get;set;}= string.Empty;
    public string tds_calculation_type {get;set;}=string.Empty;
    public int tdspercentage {get;set;}
    public string typeofreceipt {get; set;} =string.Empty;
    public string file_name {get;set;} = string.Empty;

    public string clear_date {get; set;} = string.Empty;

    public string cheque_date {get;set;}= string.Empty;
    public string deposited_date {get; set;}=string.Empty;
}
#endregion GeneralReceiptsData...

#region ViewBankInformation...

public class ViewBankInformation : BaseEntity
{
    public int recordid {get; set;}
    public string bank_date {get;set;} = string.Empty;
    public string account_number {get;set;} = string.Empty;
    public string bank_name {get;set;} = string.Empty;

    public string account_name {get;set;} = string.Empty;
    public string bank_branch {get;set;} = string.Empty;
    public string ifsccode {get;set;} = string.Empty;
    public int overdraft {get;set;}
    public int openingbalance {get;set;}
    public bool is_debitcard_applicable {get;set;}
    public bool is_upi_applicable {get;set;}
    public bool statusname {get;set;}
    public string typeofoperation {get;set;} = string.Empty;

    public string account_type {get;set;} = string.Empty;
    public string opening_jvno {get;set;} = string.Empty;
    public string OpeningBalanceType {get;set;} = string.Empty;

    
}

#endregion ViewBankInformation...

#region AvailableChequeCount...

public class AvailableChequeCount :BaseEntity
{
    
    public int Count { get; set; }


}

#endregion AvailableChequeCount...

