
using Kapil_Group_Repository.Entities;

namespace Kapil_Group_Repository.Interfaces;

public interface IAccounts
{
  List<string> GetBankDetails(string connectionString, string GlobalSchema, string AccountsSchema);
  List<Bank> GetBankDetails1(string connectionString, string globalSchema, string accountsSchema,
 string BranchCode, string CompanyName);

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
  List<ViewChequeManagementDTO> ViewChequeManagementDetails(string connectionString, string branchSchema, string globalSchema, string companyCode, string branchCode, int pageSize, int pageNo);

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
  List<ViewBankInformation> GetViewBankInformation(string connectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string BranchCode, string precordid);
  #endregion ViewBankInformation...

  #region AvailableChequeCount...

  List<AvailableChequeCount> GetAvailableChequeCount(string connectionString,  int bankId,int chqFromNo,int chqToNo,string branchSchema,string companyCode,string branchCode);

  #endregion AvailableChequeCount...

#region PettyCashExistingData...

List<PettyCashExistingData> GetPettyCashExistingData(string connectionString, string GlobalSchema, string BranchSchema, string CompanyCode, string Branchcode);


#endregion PettyCashExistingData...

#region  PaymentVoucherExistingData..
List<PaymentVoucherDetails> GetPaymentVoucherExistingData(string connectionString, string? globalSchema, string? branchSchema, string? companyCode, string? branchCode);


#endregion PaymentVoucherExistingData..
#region  ProductnamesandHSNcodes..
List<ProductNamesAndHSNCodesDetails> GetProductNamesAndHSNCodes(string connectionString,string? globalSchema);

#endregion ProductnamesandHSNcodes..





}
