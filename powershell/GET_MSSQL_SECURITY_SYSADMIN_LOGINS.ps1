Import-Module SQLPS -DisableNameChecking
Function Parse-ServerGroup($serverGroup)
{
$results = $serverGroup.RegisteredServers;
foreach($group in $serverGroup.ServerGroups)
{
$results += Parse-ServerGroup -serverGroup $group;
}
return $results;
}
Function Get-ServerList ([string]$cmsName, [string]$serverGroup, [switch]$recurse)
{
$connectionString = "data source=$cmsName;initial catalog=master;integrated security=sspi;"
$sqlConnection = New-Object ("System.Data.SqlClient.SqlConnection") $connectionstring
$conn = New-Object ("Microsoft.SQLServer.Management.common.serverconnection") $sqlconnection
$cmsStore = New-Object ("Microsoft.SqlServer.Management.RegisteredServers.RegisteredServersStore") $conn
$cmsRootGroup = $cmsStore.ServerGroups["DatabaseEngineServerGroup"].ServerGroups[$serverGroup]

if($recurse)
{
return Parse-ServerGroup -serverGroup $cmsRootGroup | select ServerName
}
else
{
return $cmsRootGroup.RegisteredServers | select ServerName
}
}

$CentralManagementServer = ".\DBA01"
$serverList = Get-ServerList -cmsName $CentralManagementServer -serverGroup ALL -recurse

$serverList | Format-Table

#Load SMO assemblies

$MS='Microsoft.SQLServer'
@('.SMO', '.Management.RegisteredServers', '.ConnectionInfo') |
 foreach-object {if ([System.Reflection.Assembly]::LoadWithPartialName("$MS$_") -eq $null) {"missing SMO component $MS$_"}}
 
$extractdate=$(get-date).ToLongDateString()

 
$My="$ms.Management.Smo" #
$serverList|
 Foreach-object {new-object ("$My.Server") $_.ServerName } | # create an SMO server object
 Where-Object {$_.ServerType -ne $null} | # did you positively get the server?
 Foreach-object {$_.Logins } | #logins for every server successfully reached 
 where {$_.IsMember("sysadmin")} | #only sysadmin
 Select-object ${extractdate},@{Name="Server"; Expression={$_.parent}}, Name, DefaultDatabase , CreateDate, DateLastModified , IsDisabled |
 Export-Csv -Path .\sysadmins_logins.csv -Delimiter '|' -NoTypeInformation -Force