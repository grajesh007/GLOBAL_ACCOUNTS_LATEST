using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kapil_Group_Repository.Interfaces;
using Kapil_Group_Infrastructure.Data;

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
                var banks = _accountDal.GetBankDetails(_con, globalSchema ?? _globalSchema,
                 accountsSchema ?? _accountsSchema);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }
        [HttpGet("banks1")]
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

        [HttpGet("BankNames")]
        public IActionResult GetBankNames(
            string? GlobalSchema = null,
            string? AccountsSchema = null,
            string? CompanyCode = null,
            string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.GetBankNamesDetails(
                    _con,
                    GlobalSchema ?? GlobalSchema,
                    AccountsSchema ?? AccountsSchema,
                    CompanyCode ?? CompanyCode,
                    BranchCode ?? BranchCode
                );

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }


        #endregion BankNames
        #region companyNameandaddressDetails
        [HttpGet("CompanyNameAndAddress")]
        public IActionResult GetCompanyNameAndAddressDetails(
   string? GlobalSchema = null,
   string? CompanyCode = null,
   string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.GetCompanyNameAndAddressDetails(
                    _con,
                    GlobalSchema ?? GlobalSchema,
                    CompanyCode ?? CompanyCode,
                    BranchCode ?? BranchCode
                );

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }
        #endregion companyNameandaddressDetails
        #region BankConfigurationdetails
        [HttpGet("BankConfigurationDetails")]
        public IActionResult GetBankConfigurationDetails(
    string? BranchSchema = null,
    string? CompanyCode = null,
    string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.GetBankConfigurationDetails(
                    _con,
                    BranchSchema ?? BranchSchema,
                    CompanyCode ?? CompanyCode,
                    BranchCode ?? BranchCode
                );

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        #endregion BankConfigurationdetails

        #region ViewChequeManagementDetails
        [HttpGet("ViewChequeManagementDetails")]
        public IActionResult ViewChequeManagementDetails(
     string branchSchema,
     string globalSchema,
     string companyCode,
     string branchCode,
     int pageSize = 10,
     int pageNo = 0)
        {
            try
            {
                var result = _accountDal.ViewChequeManagementDetails(
                    _con,
                    branchSchema,
                    globalSchema,
                    companyCode,
                    branchCode,
                    pageSize,
                    pageNo
                );

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

        #endregion ViewChequeManagementDetails

        #region ExistingChequeCount
        [HttpGet("ExistingChequeCount")]
        public IActionResult GetExistingChequeCount(
    int bankId,
    int chqFromNo,
    int chqToNo,
    string? BranchSchema = null,
    string? CompanyCode = null,
    string? BranchCode = null)
        {
            try
            {
                var result = _accountDal.GetExistingChequeCount(
                    _con,
                    bankId,
                    chqFromNo,
                    chqToNo,
                    BranchSchema ?? BranchSchema,
                    CompanyCode ?? CompanyCode,
                    BranchCode ?? BranchCode
                );

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
        public IActionResult GetGeneralReceiptsData(string? GlobalSchema = null, string? BranchSchema = null, string? TaxSchema = null, string? CompanyCode = null,string? BranchCode = null)
        {
            try
            {  
                var banks = _accountDal.GetGeneralReceiptsData(_con, GlobalSchema, BranchSchema, TaxSchema,CompanyCode, BranchCode);
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
        public IActionResult GetViewBankInformation(string? GlobalSchema = null, string? BranchSchema = null, string? CompanyCode = null, string? BranchCode = null, string? precordid = null)
        {
            try
            {
                var banks = _accountDal.GetViewBankInformation(_con, GlobalSchema ?? _globalSchema, BranchSchema , CompanyCode , BranchCode , precordid );
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }                                
    
        }


    #endregion ViewBankInformation...








    }
}
