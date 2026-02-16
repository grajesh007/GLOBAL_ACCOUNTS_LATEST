using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kapil_Group_Repository.Interfaces;
using Kapil_Group_Infrastructure.Data;
using Kapil_Group_Repository.Entities;


namespace Kapil_Group_ERP_API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AccountsController : ControllerBase
    {
        private readonly string _con;
        private readonly string _globalSchema;
        private readonly string _accountsSchema;
        private readonly IConfiguration _configuration;

        private readonly IWebHostEnvironment _hostingEnvironment;

        private readonly IAccounts _accountDal;

        public object BranchCode { get; private set; }

        public AccountsController(IConfiguration configuration, IWebHostEnvironment hostingEnvironment)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _hostingEnvironment = hostingEnvironment ?? throw new ArgumentNullException(nameof(hostingEnvironment));

            _con = _configuration.GetConnectionString("DefaultConnection") ?? string.Empty;
            _globalSchema = _configuration.GetValue<string>("GlobalSchemeName") ?? string.Empty;
            _accountsSchema = _configuration.GetValue<string>("AccountsSchema") ?? string.Empty;

            // If you prefer DI for AccountsDAL, register it and inject IAccounts instead
            _accountDal = new AccountsDAL();
        }



        [HttpGet("GetBanks")]
        public IActionResult GetBanks(string? globalSchema = null, string? accountsSchema = null)
        {
            try
            {
                // use configured schemas by default; override by query parameters if needed later
                var banks = _accountDal.GetBankDetails(_con, globalSchema ?? _globalSchema,accountsSchema ?? _accountsSchema);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("Getbanks1")]
        public IActionResult GetBanks1(string? globalSchema = null, string? accountsSchema = null, string? BranchCode = null, string? CompanyName = null)
        {
            try
            {
                // use configured schemas by default; override by query parameters if needed later
                var banks = _accountDal.GetBankDetails1(_con, globalSchema ?? _globalSchema, accountsSchema ?? _accountsSchema, BranchCode ?? BranchCode, CompanyName ?? CompanyName);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

#region BankNames

        [HttpGet("GetBankNames")]
        public IActionResult GetBankNames(string? GlobalSchema = null,string? AccountsSchema = null,string? CompanyCode = null,string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.GetBankNamesDetails( _con,GlobalSchema ?? GlobalSchema,AccountsSchema ?? AccountsSchema,CompanyCode ?? CompanyCode,BranchCode ?? BranchCode);

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }


#endregion BankNames

#region companyNameandaddressDetails

        [HttpGet("GetCompanyNameAndAddress")]
        public IActionResult GetCompanyNameAndAddressDetails(string? GlobalSchema = null,string? CompanyCode = null,string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.GetCompanyNameAndAddressDetails(_con,GlobalSchema ?? GlobalSchema,CompanyCode ?? CompanyCode,BranchCode ?? BranchCode);

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

#endregion companyNameandaddressDetails

#region BankConfigurationdetails

        [HttpGet("GetBankConfigurationDetails")]
        public IActionResult GetBankConfigurationDetails(string? BranchSchema = null,string? CompanyCode = null,string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.GetBankConfigurationDetails(_con,BranchSchema ?? BranchSchema,CompanyCode ?? CompanyCode,BranchCode ?? BranchCode);

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

#endregion BankConfigurationdetails


#region ViewChequeManagementDetails

        [HttpGet("GetViewChequeManagementDetails")]
        public IActionResult ViewChequeManagementDetails(string BranchSchema,string GlobalSchema,string CompanyCode,string BranchCode,int PageSize,int PageNo)
        {
            try
            {
                var result = _accountDal.ViewChequeManagementDetails(_con,BranchSchema,GlobalSchema,CompanyCode,BranchCode,PageSize,PageNo);

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }


#endregion ViewChequeManagementDetails

#region ExistingChequeCount

        [HttpGet("GetExistingChequeCount")]
        public IActionResult GetExistingChequeCount(int bankId,int chqFromNo,int chqToNo,string? BranchSchema = null,string? CompanyCode = null,string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.GetExistingChequeCount( _con,bankId,chqFromNo,chqToNo,BranchSchema ?? BranchSchema,CompanyCode ?? CompanyCode,BranchCode ?? BranchCode);

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }


#endregion ExistingChequeCount


#region BankUPIDetails...

        [HttpGet("GetBankUPIDetails")]

        public IActionResult GetBankUPIDetails(string? GlobalSchema = null, string? BranchCode = null, string? CompanyCode = null)
        {
            try
            {
                var banks = _accountDal.GetBankUPIDetails(_con, GlobalSchema, CompanyCode, BranchCode);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

#endregion BankUPIDetails...

#region ViewBankInformationDetails....

        [HttpGet("GetViewBankInformationDetails")]
        public IActionResult GetViewBankInformationDetails(string? GlobalSchema = null, string? BranchSchema = null, string? BranchCode = null, string? CompanyCode = null)
        {
            try
            {
                var banks = _accountDal.GetViewBankInformationDetails(_con, GlobalSchema, BranchSchema, BranchCode, CompanyCode);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

#endregion ViewBankInformationDetails...


#region GeneralReceiptsData...

        [HttpGet("GetGeneralReceiptsData")]
        public IActionResult GetGeneralReceiptsData(string? GlobalSchema = null, string? BranchSchema = null, string? TaxSchema = null, string? CompanyCode = null, string? BranchCode = null)
        {
            try
            {
                var banks = _accountDal.GetGeneralReceiptsData(_con, GlobalSchema, BranchSchema, TaxSchema, CompanyCode, BranchCode);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

#endregion GeneralReceiptsData...

#region  ViewBankInformation...

        [HttpGet("GetViewBankInformation")]
        public IActionResult GetViewBankInformation(int precordid, string? GlobalSchema = null, string? BranchSchema = null, string? CompanyCode = null, string? BranchCode = null)
        {
            try
            {
                var banks = _accountDal.GetViewBankInformation(_con, GlobalSchema ?? _globalSchema, BranchSchema, CompanyCode, BranchCode, precordid);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }

        }


#endregion ViewBankInformation...

#region AvailableChequeCount...

        [HttpGet("GetAvailableChequeCount")]
        public IActionResult GetAvailableChequeCount(int bankId,int chqFromNo,int chqToNo,string? BranchSchema = null,string? CompanyCode = null,string? BranchCode = null)
        {
            try
            {

                var banks = _accountDal.GetAvailableChequeCount(_con, bankId,chqFromNo,chqToNo,BranchSchema ?? BranchSchema,CompanyCode ?? CompanyCode,BranchCode ?? BranchCode);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

#endregion AvailableChequeCount...


#region PettyCashExistingData...

        [HttpGet("GetPettyCashExistingData")]
        public IActionResult GetPettyCashExistingData(string? GlobalSchema = null, string? BranchSchema = null, string? CompanyCode = null, string? Branchcode = null)
        {
            try
            {

                var banks = _accountDal.GetPettyCashExistingData(_con, GlobalSchema, BranchSchema, CompanyCode, Branchcode);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

#endregion PettyCashExistingData...

#region PaymentVoucherExistingData..

        [HttpGet("GetPaymentVoucherExistingData")]
        public IActionResult GetPaymentVoucherExistingData(string? GlobalSchema = null,string? BranchSchema = null,string? CompanyCode = null,string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.GetPaymentVoucherExistingData(_con,GlobalSchema ?? GlobalSchema,BranchSchema ?? BranchSchema,CompanyCode ?? CompanyCode,BranchCode ?? BranchCode);

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

 #endregion PaymentVoucherExistingData..

#region ProductnamesandHSNcodes..

        [HttpGet("GetProductNamesAndHSNCodes")]
        public IActionResult GetProductNamesAndHSNCodes(string? GlobalSchema = null)
        {
            try
            {
                var result = _accountDal.GetProductNamesAndHSNCodes(_con,GlobalSchema ?? GlobalSchema);

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }


#endregion ProductnamesandHSNcodes

#region getReceiptNumber..

        [HttpGet("GetReceiptNumber")]
        public IActionResult getReceiptNumber(string? GlobalSchema = null,string? BranchSchema = null,string? CompanyCode = null,string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.getReceiptNumber(_con,GlobalSchema ?? GlobalSchema,BranchSchema ?? BranchSchema,CompanyCode ?? CompanyCode,BranchCode ?? BranchCode);

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }


 #endregion ProductnamesandHSNcodes

#region GetBankUPIList

        [HttpGet("BankUPIList")]
        public IActionResult GetBankUPIList(string? BranchSchema = null,string? CompanyCode = null,string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.GetBankUPIListDetails(_con,BranchSchema ?? BranchSchema,CompanyCode ?? CompanyCode,BranchCode ?? BranchCode
                );

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

#endregion GetBankUPIList

#region GetCAOBranchList

        [HttpGet("GetCAOBranchList")]
        public IActionResult GetCAOBranchList(string? GlobalSchema = null,string? BranchSchema = null,string? CompanyCode = null,string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.GetCAOBranchListDetails(_con,GlobalSchema ?? GlobalSchema,BranchSchema ?? BranchSchema,CompanyCode ?? CompanyCode,BranchCode ?? BranchCode);

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

#endregion GetCAOBranchList

#region GetBankBookDetails...
       
        [HttpGet("GetBankBookDetails")]
        public IActionResult GetBankBookDetails(string fromDate,string toDate,long _pBankAccountId,string AccountsSchema,string GlobalSchema,string CompanyCode,string BranchCode)
        {
            try
            {
                var plstBankBook = _accountDal.GetBankBookDetails( _con, fromDate, toDate, _pBankAccountId,AccountsSchema, GlobalSchema, CompanyCode, BranchCode);

                return Ok(plstBankBook);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

#endregion GetBankBookDetails...

 #region GetRePrintInterBranchGeneralReceiptCount....

        [HttpGet("GetRePrintInterBranchGeneralReceiptCount")]
        public int GetRePrintInterBranchGeneralReceiptCount([FromQuery] string ReceiptId,[FromQuery] string BranchSchema,[FromQuery] string CompanyCode,[FromQuery] string BranchCode)

        {
            int count = 0;

            try
            {
                count = _accountDal.GetRePrintInterBranchGeneralReceiptCount(_con,ReceiptId,BranchSchema,CompanyCode,BranchCode);
            }
            catch (Exception)
            {
                throw;
            }
            return count;
        }

#endregion GetRePrintInterBranchGeneralReceiptCount....


#region GetPartywiseStates....

        [HttpGet("GetPartywiseStates")]
        public IActionResult GetPartywiseStates(string BranchSchema,string partyid,string GlobalSchema,string CompanyCode,string BranchCode)
        {
            try
            {
                var statelist = _accountDal.getStatesbyPartyid(Convert.ToInt64(partyid),_con,0,GlobalSchema,BranchSchema,CompanyCode,BranchCode);

                return Ok(statelist);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }
 #endregion GetPartywiseStates...

     
#region checkAccountnameDuplicates...

        [HttpGet("checkAccountnameDuplicates")]
        public IActionResult checkAccountnameDuplicates(string Accountname, string AccountType, int Parentid, string CompanyCode, string GlobalSchema, string BranchSchema, string BranchCode)
        {
            int count = 0;
            try
            {
                count = _accountDal.checkAccountnameDuplicates(Accountname, AccountType, Parentid, GlobalSchema, _con, CompanyCode,BranchCode);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError);
            }
            return Ok(count); ;
        }

#endregion checkAccountnameDuplicates...

#region GetCashRestrictAmountpercontact...

        [HttpGet("GetCashRestrictAmountpercontact")]
        public IActionResult GetCashRestrictAmountpercontact(string type, string branchtype, string BranchSchema, long contactid, string checkdate, string CompanyCode, string GlobalSchema,
                   string BranchCode)
        {
            decimal result = 0;

            try
            {
                result = _accountDal.GetCashRestrictAmountpercontact(type, branchtype, _con, GlobalSchema, BranchSchema, contactid, checkdate, CompanyCode, BranchCode);
                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }

        }

#endregion GetCashRestrictAmountpercontact...


 #region GetGstLedgerAccountList...

        [HttpGet("GetGstLedgerAccountList")]

        public IActionResult GetGstLedgerAccountList(string BranchSchema, string formname, string CompanyCode,string BranchCode)
        {

            List<AccountsDTO> accountslist = new List<AccountsDTO>();
            try
            {
                accountslist = _accountDal.GetGstLedgerAccountList(_con, formname, BranchSchema, CompanyCode,BranchCode);
                if (accountslist.Count > 0)
                {
                    accountslist.Insert(0, new AccountsDTO
                    {
                        pledgerid = 0,
                        pledgername = "ALL"
                    });
                }

                return Ok(accountslist);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }

        }

#endregion GetGstLedgerAccountList...


#region GetLedgerAccountList....

        [HttpGet("GetLedgerAccountList")]

        public IActionResult GetLedgerAccountList(string formname, string BranchSchema, string CompanyCode,string BranchCode, string GlobalSchema)
        {

            List<AccountsDTO> accountslist = new List<AccountsDTO>();
            try
            {
                accountslist = _accountDal.GetLedgerAccountList(_con, formname, GlobalSchema, BranchSchema, CompanyCode,BranchCode);
                return Ok(accountslist);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }

        }

#endregion GetLedgerAccountList...


#region  GetLedgerSummaryAccountList...

        [HttpGet("GetLedgerSummaryAccountList")]

        public IActionResult GetLedgerSummaryAccountList(string formname, string BranchSchema, string CompanyCode,string BranchCode, string GlobalSchema)
        {

            List<AccountsDTO> accountslist = new List<AccountsDTO>();
            try
            {
                accountslist = _accountDal.GetLedgerSummaryAccountList(_con, formname, GlobalSchema, BranchSchema, CompanyCode,BranchCode);
                return Ok(accountslist);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }

        }

#endregion GetLedgerSummaryAccountList...

#region  GetSubAccountLedgerDetails...

        [HttpGet("GetSubAccountLedgerDetails")]
        public IActionResult GetSubAccountLedgerDetails(string BranchSchema, string CompanyCode,string BranchCode)
        {
            List<subAccountLedgerDTO> _subAccountLedgerList = new List<subAccountLedgerDTO>();
            try
            {
                _subAccountLedgerList = _accountDal.GetSubAccountLedgerDetails(_con, BranchSchema, CompanyCode, BranchCode);
                return Ok(_subAccountLedgerList);

            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

 #endregion GetSubAccountLedgerDetails...

#region GetAccountLedgerNames...

        [HttpGet("GetAccountLedgerNames")]
        public IActionResult GetAccountLedgerNames(string SubLedgerName, string BranchSchema, string CompanyCode,string BranchCode)
        {
            List<subAccountLedgerDTO> _SubAccountLedgerDTO = new List<subAccountLedgerDTO>();
            try
            {
                _SubAccountLedgerDTO = _accountDal.GetAccountLedgerData(_con, SubLedgerName, BranchSchema, CompanyCode,BranchCode);
                return Ok(_SubAccountLedgerDTO);

            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

#endregion GetAccountLedgerNames....


#region GetSubAccountLedgerReportData....

        [HttpGet("GetSubAccountLedgerReportData")]
        public IActionResult GetSubAccountLedgerReportData(string SubLedgerName, long parentid, string fromDate, string toDate, string BranchSchema, string CompanyCode,string BranchCode)
        {
            List<subAccountLedgerDTO> _SubAccountLedgerDTO = new List<subAccountLedgerDTO>();
            try
            {
                _SubAccountLedgerDTO = _accountDal.GetSubLedgerReportData(_con, SubLedgerName, parentid, fromDate, toDate, BranchSchema, CompanyCode,BranchCode);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
            return Ok(_SubAccountLedgerDTO);
        }

#endregion GetSubAccountLedgerReportData...

#region GetSubLedgerData...

        [HttpGet("GetSubLedgerData")]
        public IActionResult GetSubLedgerData(long pledgerid, string BranchSchema, string CompanyCode, string LocalSchema,
        string BranchCode, string GlobalSchema)
        {

            List<AccountsDTO> accountslist = new List<AccountsDTO>();
            try

            {
                accountslist = _accountDal.GetSubLedgerAccountList(pledgerid, _con, GlobalSchema, BranchSchema, LocalSchema, CompanyCode,
           BranchCode);
                return Ok(accountslist);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }

        }

#endregion GetSubLedgerData...

#region GetTrialBalance...

        [HttpGet("GetTrialBalance")]
        public IActionResult GetTrialBalance(string fromDate, string todate, string GroupType, string LocalSchema, string CompanyCode,string BranchCode, string GlobalSchema)
        {
            List<AccountReportsDTO> lstAccountReportsDTO = new List<AccountReportsDTO>();
            try
            {
                lstAccountReportsDTO = _accountDal.GetTrialBalance(_con, LocalSchema, fromDate, todate, GroupType, CompanyCode,BranchCode, GlobalSchema);

            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }

            return Ok(lstAccountReportsDTO);
        }

#endregion GetTrialBalance...

#region GetIssuedChequeNumbers...

        [HttpGet("GetIssuedChequeNumbers")]
        public IActionResult GetIssuedChequeNumbers(long _BankId, string BranchSchema, string CompanyCode,string BranchCode)
        {
            List<IssuedChequeDTO> _IssuedChequeDTO = new List<IssuedChequeDTO>();
            try
            {
                _IssuedChequeDTO = _accountDal.GetIssuedChequeNumbers(_con, _BankId, BranchSchema, CompanyCode,BranchCode);
                return Ok(_IssuedChequeDTO);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

#endregion GetIssuedChequeNumbers...

#region  GetMainAccounthead...

        [HttpGet("GetMainAccounthead")]
        public IActionResult GetMainAccounthead(string BranchSchema, string CompanyCode,string BranchCode)
        {
            List<subAccountLedgerDTO> _SubAccLedgerDTO = new List<subAccountLedgerDTO>();
            try
            {
                _SubAccLedgerDTO = _accountDal.GetMainAccounthead(_con, BranchSchema, CompanyCode,BranchCode);

                return Ok(_SubAccLedgerDTO);

            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);

            }

        }

#endregion GetMainAccounthead....

#region getCashbookData...

        [HttpGet("getCashbookData")]
        public IActionResult getCashbookData(string fromdate, string todate, string BranchSchema, string CompanyCode,string BranchCode)
        {
            List<cashBookDto> _CashbookDTO = new List<cashBookDto>();
            try
            {
                _CashbookDTO = _accountDal.getCashbookData(_con, fromdate, todate, BranchSchema, CompanyCode,BranchCode);
                //_CashbookDTO.plstcashchequetotal = await _AccountingReportsDAL.getCashbookDataTotals(Con, fromdate, todate);

                return Ok(_CashbookDTO);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }

        }

#endregion getCashbookData...

#region  GetBalances....

        [HttpGet("GetBalances")]
        public IActionResult GetBalances(string fromDate, string todate, string GroupType, string LocalSchema, string formname, string CompanyCode,string BranchCode)
        {
            List<AccountReportsDTO> lstAccountReportsDTO = new List<AccountReportsDTO>();
            try
            {
                lstAccountReportsDTO = _accountDal.GetBalances(_con, LocalSchema, fromDate, todate, GroupType, formname, CompanyCode,BranchCode);

            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }

            return Ok(lstAccountReportsDTO);
        }

#endregion GetBalances...

 #region GetBankTransferTypes....

        [HttpGet("GetBankTransferTypes")]
        public IActionResult GetBankTransferTypes(string branchSchema, string CompanyCode,string BranchCode)
        {
            List<BankTransferTypesDTO> _BankTransferTypesDTO = new List<BankTransferTypesDTO>();
            try
            {
                _BankTransferTypesDTO = _accountDal.GetBankTransferTypes(_con, branchSchema, CompanyCode,BranchCode);
                return Ok(_BankTransferTypesDTO);
            }
            catch (Exception)
            {
                return StatusCode(StatusCodes.Status500InternalServerError);
            }
        }

#endregion GetBankTransferTypes...


#region GetChequeReturnDetails...

        [HttpGet("GetChequeReturnDetails")]
        public  IActionResult GetChequeReturnDetails(string fromdate, string todate, string BranchSchema,string GlobalSchema ,string CompanyCode,string BranchCode)
        {
           List<ChequeEnquiryDTO> _ChequeEnquiryDTO = new List<ChequeEnquiryDTO>();
            try
            {
                _ChequeEnquiryDTO = _accountDal.GetChequeReturnDetails(_con, fromdate, todate, BranchSchema,GlobalSchema,CompanyCode,BranchCode);
                return Ok(_ChequeEnquiryDTO);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError);
            }

        }

#endregion GetChequeReturnDetails....


#region GetIssuedChequeDetails...

         [HttpGet("GetIssuedChequeDetails")]
        public IActionResult GetIssuedChequeDetails(long _BankId, long _ChqBookId, long _ChqFromNo, long _ChqToNo, string BranchSchema,string GlobalSchema,string CompanyCode,string BranchCode)
        {
            List<IssuedChequeDTO> _IssuedChequeDTO = new List<IssuedChequeDTO>();

            try
            {
                _IssuedChequeDTO = _accountDal.GetIssuedChequeDetails(_con, _BankId, _ChqBookId, _ChqFromNo, _ChqToNo, BranchSchema, GlobalSchema,CompanyCode,BranchCode);
                return Ok(_IssuedChequeDTO);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError);
            }
        }

#endregion GetIssuedChequeDetails...

#region GetUnUsedCheques....

         [HttpGet("GetUnUsedCheques")]
        public  IActionResult GetUnUsedCheques(long _BankId, long _ChqBookId, long _ChqFromNo, long _ChqToNo, string BranchSchema,string GlobalSchema,string CompanyCode,string BranchCode)
        {
            List<IssuedChequeDTO> _IssuedChequeDTO = new List<IssuedChequeDTO>();

            try
            {
                _IssuedChequeDTO = _accountDal.GetUnUsedCheques(_con, _BankId, _ChqBookId, _ChqFromNo, _ChqToNo, BranchSchema, GlobalSchema, CompanyCode,BranchCode);
                return Ok(_IssuedChequeDTO);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError);
            }
        }
  
#endregion GetUnUsedCheques...

#region getCountry...

         [HttpGet("getCountry")]
        public  IActionResult getCountry(string GlobalSchema)
        {
            try
            {
                List<CountryDTO> _lstCountryDTO = new List<CountryDTO>();
                _lstCountryDTO = _accountDal.getCountry(_con, GlobalSchema);
                return Ok(_lstCountryDTO);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError);
            }
        }

#endregion getCountry...

#region getstate...

         [HttpGet("getstate")]
        public IActionResult getstate(string GlobalSchema,long id)
        {
            try
            {
                 List<StateDTO> _lstStateDTO  = new  List<StateDTO>();
                 _lstStateDTO = _accountDal.getState(_con, GlobalSchema, id);
                return Ok(_lstStateDTO);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError);
            }
        }

#endregion getstate...

#region getDistrict...

        [HttpGet("getDistrict")]
        public  IActionResult getDistrict(string GlobalSchema,long id)
        {
            try
            {
                List<District> _lstDistrictDTO = new List<District>();
                 _lstDistrictDTO = _accountDal.getDistrict(_con, GlobalSchema, id);
                return Ok(_lstDistrictDTO);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError);
            }
        }

#endregion getDistrict...

#region Getformnamedetails...

        [HttpGet("Getformnamedetails")]
        public IActionResult Getformnamedetails(string? globalSchema = null, string? companyCode = null, string? BranchCode = null)
        {
            try
            {
                // use configured schemas by default; override by query parameters if needed later
                var banks = _accountDal.Getformnamedetails(_con, globalSchema ?? _globalSchema, companyCode ?? companyCode, BranchCode ?? BranchCode);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

#endregion Getformnamedetails...



    }
}
