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
                var banks = _accountDal.GetBankDetails(_con, globalSchema ?? _globalSchema, accountsSchema ?? _accountsSchema);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }
        [HttpGet("banks1")]
        public IActionResult GetBanks1(string? globalSchema = null, string? accountsSchema = null,string? BranchCode = null, string? CompanyName = null)
        {
            try
            {
                // use configured schemas by default; override by query parameters if needed later
                var banks = _accountDal.GetBankDetails1(_con, globalSchema ?? _globalSchema, accountsSchema ?? _accountsSchema,  BranchCode ?? BranchCode,CompanyName ?? CompanyName);
                return Ok(banks);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

    }
}
