workflow RebuildReplicatedTables
{
    Param(
        $ConnectionName = "AzureRunAsConnection",
        [string]$SQLActionAccountName,
        [string]$ServerName,
        [string]$DWName,
        [int]$RetryCount = 4,
        [int]$RetryTime = 15
    )

    #Pulling in credentials used in the process
    $credSQL = Get-AutomationPSCredential -Name $SQLActionAccountName
    $AutomationConnection = Get-AutomationConnection -Name $ConnectionName
    $null = Add-AzureRmAccount -ServicePrincipal -TenantId $AutomationConnection.TenantId -ApplicationId $AutomationConnection.ApplicationId -CertificateThumbprint $AutomationConnection.CertificateThumbprint
    $DWDetail = (Get-AzureRmResource | Where-Object {$_.Kind -like "*datawarehouse*" -and $_.Name -like "*/$DWName"})
    if ($DWDetail.Count -eq 1) {
        $DWDetail = $DWDetail.ResourceId.Split("/")
        $SQLUser = $credSQL.Username
        $SQLPass = $credSQL.GetNetworkCredential().Password
        $cRetry = 0
        do {
            if ($cRetry -ne 0) {Start-Sleep -Seconds $RetryTime}
            $DWStatus = (Get-AzureRmSqlDatabase -ResourceGroup $DWDetail[4] -ServerName $DWDetail[8] -DatabaseName $DWDetail[10]).Status
            Write-Verbose "Test $cRetry status is $DWStatus looking for Online"
            $cRetry++
        } while ($DWStatus -ne "Online" -and $cRetry -le $RetryCount )
        if ($DWStatus -eq "Online") {
            Write-Verbose "Rebuilding Replicated Tables"
            InLineScript {
                $ReplicatedTablesQuery = "select object_id from sys.pdw_table_distribution_properties where distribution_policy = 3"
                $DBConnection = New-Object System.Data.SqlClient.SqlConnection("Server=$($Using:ServerName); Database=$($Using:DWName);User ID=$($Using:SQLUser);Password=$($Using:SQLPass);")
                $DBConnection.Open()
                $DBCommand = New-Object System.Data.SqlClient.SqlCommand($ReplicatedTablesQuery, $DBConnection)
                $DBAdapter = New-Object -TypeName System.Data.SqlClient.SqlDataAdapter
                $ReplicatedDataSet = New-Object -TypeName System.Data.DataSet
                $DBAdapter.SelectCommand = $DBCommand
                $DBAdapter.Fill($ReplicatedDataSet) | Out-Null
                if ($ReplicatedDataSet.Tables[0].Rows.Count -gt 0) {
                    $RebuildQuery = ""
                    foreach ($ReplicatedTableId in $ReplicatedDataSet.Tables[0].Rows.object_id) {
                        $RebuildQuery += "SELECT TOP 1 * FROM object_name($($ReplicatedTableId))`r`n"
                    }
                    $DBCommand = New-Object System.Data.SqlClient.SqlCommand($RebuildQuery, $DBConnection)
                    $DBAdapter.SelectCommand = $DBCommand
                }
            }
        }
}