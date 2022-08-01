<#
    Name:            Anthony E. Nocentino aen@centinosystems.com
    Date:            04/28/2015
    Name:           Romain Ferraton romain.ferraton [chez] architecture-performance.fr
    Description: Script to move SQL Server data between filegroups
    
     Prerequisites   For powershell 7  : Install-Module -Name SqlServer -RequiredVersion 21.1.18256
     
    Notes:           Does not migrate the following 
                     Partitioned tables
                     LOB objects
    Notes:          Steps :
                    Step 1 : List objects to rebuild
                    Step 2 : disable non-clustered indexes
                    Step 3 : rebuild clustered indexes into the Target Filegroup
                    Step 4 : Rebuild Heaps into the Target Filegroup
                    Step 5 : Rebuild Non-Clustered indexes into the Target Filegroup
#>

# simulation 
#.\FileGroupsMove.ps1 -server ".\DEV" -dbName "DWH_CORP_FIN" -doWork $FALSE -onlineOpt $FALSE -tablesToMove "*" -schemaToMove "*" -TargetfileGroup "SECONDARY"
#
# run
# .\FileGroupsMove.ps1 -server ".\DEV" -dbName "DWH_CORP_FIN" -doWork $TRUE -onlineOpt $FALSE -tablesToMove "*" -schemaToMove "*" -TargetfileGroup "SECONDARY"


param 
(
    [Parameter(Mandatory)] [string] $server = ".",
    [Parameter(Mandatory)] [string] $dbName,
    [Parameter(Mandatory)] [bool] $doWork = $FALSE, #safety net, true actually moves the data, false just outputs what the process will do
    [Parameter(Mandatory)] [bool] $onlineOpt = $FALSE, #request an online index move
    [Parameter(Mandatory)] [string] $tablesToMove = "*", # * is default, enter a matching string for example tableName*
    [Parameter(Mandatory)] [string] $schemaToMove = "*",
    [Parameter(Mandatory)] [string] $SourcefileGroup = "*",
    [Parameter(Mandatory)] [string] $TargetfileGroup = "SECONDARY"
          
)
Write-host "Parameters======================================================================="
Write-host "Server = ${server}"
Write-host "dbName = ${dbName}"
Write-host "doWork = ${doWork}"
Write-host "onlineOpt = ${onlineOpt}"
Write-host "tablesToMove = ${tablesToMove}"
Write-host "schemaToMove = ${schemaToMove}"
Write-host "SourcefileGroup = ${SourcefileGroup}"
Write-host "TargetfileGroup = ${TargetfileGroup}"
Write-host "Parameters======================================================================="


[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO")  #| out-null

$sqlServer = New-Object ('Microsoft.SqlServer.Management.Smo.Server') -argumentlist $server
$db = $sqlServer.Databases | Where-Object { $_.Name -eq $dbName }
$onlineIndex = $FALSE
$tableCount = 0 #simple counter for tables

if ($db.Name -ne $dbName) {
    Write-Output('Database not found')
    return
}



$destFileGroup = ($db.FileGroups | Where-Object { $_.Name -eq $TargetfileGroup } )

#check to see if the destination file group exists
if ( $destFileGroup.State -ne "Existing") {
    Write-Output('Destination filegroup not found')
    return
}

Write-Output ('Database: ' + $db.Name)

#if edition supports online indexing and the user requested it, turn it on
if ( $sqlServer.Information.EngineEdition -eq 'EnterpriseOrDeveloper' -and $onlineOpt -eq $TRUE ) {
    $onlineIndex = $TRUE
}

#all tables that are not paritioned, that meet our search criteria specified as cmd line parameters
$tables = $db.Tables | Where-Object { $_.Name -like $tablesToMove -and $_.Schema -like $schemaToMove -and $_.IsPartitioned -eq $FALSE -and $_.FileGroup -like $SourcefileGroup }

$indexesClusteredToMove = @()
$indexesNonClusteredToMove = @()
$heapsToMove = @()

#build a list of tables to be moved
foreach ( $table in $tables ) {     
    #get a list of all indexes on this table
    $indexes = $table.Indexes 

    #iterate over the set of indexes
    foreach ( $index in $indexes ) {
        #$itype= $index.IndexType 
        #Write-Host "${index} is a ${itype}"
        #if this table is a clustered or Non-Clustered index. Ignore special index types.
        if ( $index.IndexType -ne "HeapIndex") {
            if ( $index.IndexType -eq "ClusteredIndex" -or $index.IndexType -eq "ClusteredColumnStoreIndex" ) {
                if ( $index.FileGroup -ne $TargetfileGroup ) {
                    Write-Output( $table.Schema + '.' + $table.Name + " " + $index.Name)
                    $tableCount++
                    $indexesClusteredToMove += $index
                }
            }
            else { # non clustered indexes
                if ( $index.FileGroup -ne $TargetfileGroup ) {
                    Write-Output( $table.Schema + '.' + $table.Name + " " + $index.Name)
                    $tableCount++
                    $indexesNonClusteredToMove += $index
                }
            }
        }
  
        
    }
    if ($table.HasClusteredIndex -eq $FALSE -and $table.FileGroup -ne $TargetfileGroup) {
        Write-Output( $table.Schema + '.' + $table.Name + " as a heap")
        $tableCount++
        $heapsToMove += $table
    }
}

#confirmation of the move request
$confirmation = Read-Host "Are you sure you want to move the" $tableCount "objects listed above to the destination filegroup? (y/n)"
if ($confirmation -ne 'y') {
    Write-Output('No tables moved')
    return
}


#Deactivate NonClusteredToMove
foreach ( $index in $indexesNonClusteredToMove ) {
    try {
        Write-Output ('Deactivate Non clustered index: ' + $index.Name + 'on ' +$index.Parent.Name)
        
        if ( $doWork -eq $TRUE ) {
            $index.Disable()
        }
    }
    catch {
        Write-Output ('Failed Disabling index ' + $index + ' ' + $error[0].Exception.InnerException )
        return
    }
}#end for each index

# rebuild clustered index
foreach ( $index in $indexesClusteredToMove ) {
    try {
        Write-Output ('Moving: ' + $index.Name + ' on ' +$index.Parent.Name)
        $index.FileGroup = $TargetfileGroup

        if ( $doWork -eq $TRUE ) {
            if ($index.isOnlineRebuildSupported ) {$index.OnlineIndexOperation = $onlineIndex}
            $index.Recreate()
        }
    }
    catch {
        Write-Output ('Failed moving index ' + $index + 'on ' +$index.Parent.Name + ' to ' + $TargetfileGroup + ' ' + $error[0].Exception.InnerException )
        return
    }
}#end for each index

#if we didn't find a clustered index after looking at all the indexes, it's a heap. Let's move that too
<#
        our algortihm is as follows
        Find the leading column on the table

        Instantiate a new Smo.Index object on the table named "TempIndex"
        Instantiate a new Smo.IndexesColumn object on leading column and add it to our TempIndex
        Set the Index as IsClustered
        Add the column to the index
        Create the Index
        Use DropAndMove to move the index to the destination filegroup and leave it as a heap

    #>
foreach ($table in $heapsToMove) {
    $cols = $table.Columns[0]
    $leadingCol = $cols[0].Name
    
    $idx = New-Object -TypeName Microsoft.SqlServer.Management.SMO.Index -argumentlist $table, "TempIndex"
    $icol1 = New-Object -TypeName Microsoft.SqlServer.Management.SMO.IndexedColumn -argumentlist $idx, $leadingCol, $true
    $idx.IsClustered = $TRUE
    $idx.IndexedColumns.Add($icol1)

    #Write-Output $idx.Script()

    #check to see if the table is not already in the destionation filegroup and the table is indexable
    if ( $table.FileGroup -ne $TargetfileGroup -and $table.IsIndexable -eq $TRUE) {
        try {
            Write-Output('Moving Heap: ' + $table.Name)
            if ( $doWork -eq $TRUE ) {
                $idx.OnlineIndexOperation = $onlineIndex
                $idx.Create()
                $idx.OnlineIndexOperation = $onlineIndex
                $idx.DropAndMove($TargetfileGroup)
            }
        }
        catch {
            Write-Output('Failed moving heap: ' + $table + ' to ' + $TargetfileGroup + ' ' + $error[0].Exception.InnerException )
            Write-Output('Remove any Tempory indexes created')
            #return
        }
    }
    else {
        Write-Output($table.Name + ' is already in destination filegroup')
    }
}

#Rebuild NonClusteredToMove into the new target filegroup
foreach ( $index in $indexesNonClusteredToMove ) {
    try {
        Write-Output ('Rebuild Non clustered index: ' + $index.Name)
        $index.FileGroup = $TargetfileGroup
        if ( $doWork -eq $TRUE ) {
            $index.OnlineIndexOperation = $onlineIndex
            $index.Recreate()
        }
    }
    catch {
        Write-Output ('Failed Disabling index ' + $index + ' ' + $error[0].Exception.InnerException )
        return
    }
}#end for each index


#spit out data about data file size and allocation
$db.Refresh()
$tables.Refresh()

$dbfileGroup = $db.FileGroups 
Write-Output('Filegroup contents')
Write-Output($dbfileGroup.Files | Sort-Object -Property ID |  Format-Table Parent, ID, FileName, Name, Size, UsedSpace)

Write-Output('Tables')
Write-Output($tables | Select Parent, Schema, Name, FileGroup | Format-Table )
