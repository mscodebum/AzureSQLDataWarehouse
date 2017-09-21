workflow ResumeAzureSQLDataWarehouse {
    Param(
        $ConnectionName = "AzureRunAsConnection",
        [string]$ServerName,
        [string]$DWName        
    )

    $AutomationConnection= Get-AutomationConnection -Name $ConnectionName
    $null = Add-AzureRmAccount -ServicePrincipal -TenantId $AutomationConnection.TenantId -ApplicationId $AutomationConnection.ApplicationId -CertificateThumbprint $AutomationConnection.CertificateThumbprint
    $DWDetail = (Get-AzureRmResource | Where-Object {$_.Kind -like "*datawarehouse*" -and $_.Name -like "*/$DWName"}).ResourceId.Split("/")
    Get-AzureRmSqlDatabase -ResourceGroup $DWDetail[4] -ServerName $ServerName.Split(".")[0] -DatabaseName $DWDetail[10] | Resume-AzureRmSqlDatabase
}