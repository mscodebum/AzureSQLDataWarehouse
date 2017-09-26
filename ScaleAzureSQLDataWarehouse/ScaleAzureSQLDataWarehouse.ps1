workflow ScaleAzureSQLDataWarehouse {
    Param(
        $ConnectionName = "AzureRunAsConnection",
        [parameter(Mandatory=$true)]
        [string]$ServerName,
        [parameter(Mandatory=$true)]
        [string]$DWName,
        [parameter(Mandatory=$true)]
        [ValidateSet(
            "DW100", 
            "DW200", 
            "DW300",
            "DW400",
            "DW500",
            "DW600",
            "DW1000",
            "DW1200",
            "DW1500",
            "DW2000",
            "DW3000",
            "DW6000"
        )]
        [string]$RequestedServiceObjectiveName,
        [int]$RetryCount = 5,
        [int]$RetryTime = 60  
    )

    $AutomationConnection= Get-AutomationConnection -Name $ConnectionName
    $null = Add-AzureRmAccount -ServicePrincipal -TenantId $AutomationConnection.TenantId -ApplicationId $AutomationConnection.ApplicationId -CertificateThumbprint $AutomationConnection.CertificateThumbprint
    $DWDetail = (Get-AzureRmResource | Where-Object {$_.Kind -like "*datawarehouse*" -and $_.Name -like "*/$DWName"}).ResourceId.Split("/")
    $cRetry = 0
    #Ensure that the ADW is online. Wait to ensure that if it is transitioning, the proper action is taken
    do {
        if ($cRetry -ne 0) {Start-Sleep -Seconds $RetryTime}
        $DWStatus = (Get-AzureRmSqlDatabase -ResourceGroup $DWDetail[4] -ServerName $DWDetail[8] -DatabaseName $DWDetail[10]).Status
        Write-Verbose "Test $cRetry status is $DWStatus looking for Online"
        $cRetry++
    } while ($DWStatus -ne "Online" -and $cRetry -le $RetryCount )
    if ($DWStatus -eq "Online") {
        $DWSON = (Get-AzureRmSqlDatabase -ResourceGroup $DWDetail[4] -ServerName $ServerName.Split(".")[0] -DatabaseName $DWDetail[10]).CurrentServiceObjectiveName
        Write-Verbose "Requested SLO is $RequestedServiceObjectiveName current SLO is $DWSON"
        if ($DWSON -ne $RequestedServiceObjectiveName) {
            Write-Verbose "Calling Scale"
            Set-AzureRmSqlDatabase -ResourceGroup $DWDetail[4] -ServerName $ServerName.Split(".")[0] -DatabaseName $DWDetail[10] -RequestedServiceObjectiveName $RequestedServiceObjectiveName -ErrorAction SilentlyContinue
        }
    }
    $cRetry = 0
    #Now lets wait to ensure that the ADW has come online before completing
    do {
        if ($cRetry -ne 0) {Start-Sleep -Seconds $RetryTime}
        $DWStatus = (Get-AzureRmSqlDatabase -ResourceGroup $DWDetail[4] -ServerName $DWDetail[8] -DatabaseName $DWDetail[10]).Status
        Write-Verbose "Test $cRetry status is $DWStatus looking for Online"
        $cRetry++
    } while ($DWStatus -ne "Online" -and $cRetry -le $RetryCount )
    if ($DWStatus -ne "Online") {
        Write-Error "Scale operation submitted. Operation did not complete timely."
    }
}