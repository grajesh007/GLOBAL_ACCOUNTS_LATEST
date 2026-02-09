namespace Kapil_Group_Repository.Entities;

public class Bank : BaseEntity
{
    public int tbl_mst_bank_configuration_id{get; set;}
    public string BankName { get; set; } = string.Empty;
    public string bankbranch { get; set; } = string.Empty;
    public string ifsccode { get; set; } = string.Empty;
    public string accounttype { get; set; } = string.Empty;
}
