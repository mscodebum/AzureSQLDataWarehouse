workflow SuspendOrPauseAzureSQLDataWarehouse
{
    Param(
        $ConnectionName = "AzureRunAsConnection",
        [string]$SQLActionAccountName,
        [string]$ServerName,
        [string]$DWName
    )
        $credSQL = Get-AutomationPSCredential -Name $SQLActionAccountName
        $SQLUser = $credSQL.Username
        $SQLPass = $credSQL.GetNetworkCredential().Password

        $CanPause = InLineScript {
                $testquery = @"
                with test as 
                (
                    select
                    (select @@version) version_number
                    ,(select count(*) from sys.dm_pdw_exec_requests where status in ('Running', 'Pending', 'CancelSubmitted')) active_query_count
                    ,(select count(*) from sys.dm_pdw_exec_sessions where is_transactional = 1) as session_transactional_count
                    ,(select count(*) from sys.dm_pdw_waits where type = 'Exclusive') as pdw_waits
                )
                select
                    case when
                            version_number like 'Microsoft Azure SQL Data Warehouse%'
                            and active_query_count = 1
                            and session_transactional_count = 0
                            and pdw_waits = 0
                            then 1
                    else 0
                    end as CanPause
                from test
"@
                $DBConnection = New-Object System.Data.SqlClient.SqlConnection("Server=$($Using:ServerName); Database=$($Using:DWName);User ID=$($Using:SQLUser);Password=$($Using:SQLPass);")
                $DBConnection.Open()
                $DBCommand = New-Object System.Data.SqlClient.SqlCommand($testquery, $DBConnection)
                $DBAdapter = New-Object -TypeName System.Data.SqlClient.SqlDataAdapter
                $DBDataSet = New-Object -TypeName System.Data.DataSet
                $DBAdapter.SelectCommand = $DBCommandVersion
                $DBAdapter.Fill($DBDataSet) | Out-Null
                # Returning result to CanPause
                if ($DBDataSet.Tables[0].Rows[0].Column1) {$true} else {$false}
        }
        if ($CanPause) {
            $AutomationConnection=Get-AutomationConnection -Name $ConnectionName
            $null=Add-AzureRmAccount -ServicePrincipal -TenantId $AutomationConnection.TenantId -ApplicationId $AutomationConnection.ApplicationId -CertificateThumbprint $AutomationConnection.CertificateThumbprint
            $DWDetail = (Get-AzureRmResource | Where-Object {$_.Kind -like "*datawarehouse*" -and $_.Name -like "*/$DWName"}).ResourceId.Split("/")
            Get-AzureRmSqlDatabase -ResourceGroup $DWDetail[4] -ServerName $DWDetail[8] -DatabaseName $DWDetail[10] | Suspend-AzureRmSqlDatabase
        } else {
            Write-Error "Azure SQL Data Warehouse $DWName on server $ServerName has outstanding request and will not be paused at this time."
        }

}