#####
# PRODUCT:    PerformanceEye  https://github.com/AaronMorelli/PerformanceEye
# PROCEDURE:	uninstall_database_objects.ps1
#
# AUTHOR: Aaron Morelli
#	email@TBD.com
#	@sqlcrossjoin
#	sqlcrossjoin.wordpress.com					
#
#	PURPOSE: Uninstall the PerformanceEye database, its schema and T-SQL objects, and SQL Agent jobs
#  on a SQL Server instance.
#
#	OUTSTANDING ISSUES: None at this time.
#
#   CHANGE LOG:	
#		2016-09-30	Aaron Morelli		Final code run-through and commenting
#
#	MIT License
#
#	Copyright (c) 2016 Aaron Morelli
#
#	Permission is hereby granted, free of charge, to any person obtaining a copy
#	of this software and associated documentation files (the "Software"), to deal
#	in the Software without restriction, including without limitation the rights
#	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#	copies of the Software, and to permit persons to whom the Software is
#	furnished to do so, subject to the following conditions:
#
#	The above copyright notice and this permission notice shall be included in all
#	copies or substantial portions of the Software.
#
#	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#	SOFTWARE.

# To Execute
# ------------------------
# <this should not be called directly. Call the parent Uninstall_PerformanceEye.ps1 
# script instead>
#####

param ( 
[Parameter(Mandatory=$true)][string]$Server, 
[Parameter(Mandatory=$true)][string]$Database,
[Parameter(Mandatory=$true)][string]$ServerObjectsOnly,
[Parameter(Mandatory=$true)][string]$DropDatabase,
[Parameter(Mandatory=$true)][string]$curScriptLocation
) 

$ErrorActionPreference = "Stop"

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Parameter Validation complete. Proceeding with uninstall on server " + $Server + ", Database " + $Database + ", ServerObjectsOnly " + $ServerObjectsOnly + ", DropDatabase " + $DropDatabase
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Loading SqlPs module"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

Import-Module SqlPs

Write-Host "" -foregroundcolor cyan -backgroundcolor black

# Installation scripts
$pedbexistcheck = $curScriptLocation + "InstallerScripts\DBExistenceCheck.sql"
$deletedbobj = $curScriptLocation + "InstallerScripts\DeleteDatabaseObjects.sql"
$deleteservobj = $curScriptLocation + "InstallerScripts\DeleteServerObjects.sql"
$droppedatabase = $curScriptLocation + "InstallerScripts\DropPEDatabase.sql"

if ($ServerObjectsOnly -eq "N") {
    # we are deleting objects from a DB. We need to check that it exists first.
    $DBExists = "Y"

    # Check for the existence of the DB. We pass in $DBExists, and it will RAISERROR if the actual state of the DB's existence
    # doesn't match what $DBExists says
    try {
	   $MyVariableArray = "DBName = $Database", "DBExists = $DBExists"
	
	   invoke-sqlcmd -inputfile $pedbexistcheck -serverinstance $Server -database master -Variable $MyVariableArray -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	   #In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	   CD $curScriptLocation

        $curtime = Get-Date -format s
        $outmsg = $curtime + "------> Finished if-exists check for database " + $Database
        Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan
    }
    catch [system.exception] {
	   Write-Host "Error occurred in InstallerScripts\DBExistenceCheck.sql: " -foregroundcolor red -backgroundcolor black
	   Write-Host "$_" -foregroundcolor red -backgroundcolor black
        $curtime = Get-Date -format s
	   Write-Host "Aborting uninstall, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
       throw "Uninstall failed"
	   break
    }

    # if we get this far, the DB exists. Scrub it of any existing PE objects
    $curtime = Get-Date -format s
    $outmsg = $curtime + "------> Scrubbing PE database: " + $Database + " of any installed PE objects."
    Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

    try {
	   invoke-sqlcmd -inputfile $deletedbobj -serverinstance $Server -database $Database -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	   #In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	   CD $curScriptLocation

        $curtime = Get-Date -format s
        $outmsg = $curtime + "------> Finished scrubbing PE objects for database " + $Database
        Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan
    }
    catch [system.exception] {
    	Write-Host "Error occurred in InstallerScripts\DeleteDatabaseObjects.sql: " -foregroundcolor red -backgroundcolor black
    	Write-Host "$_" -foregroundcolor red -backgroundcolor black
        $curtime = Get-Date -format s
    	Write-Host "Aborting uninstall, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
        throw "Uninstall failed"
    	break
    }
    
    if ($DropDatabase -eq "Y") {
        try {
	       $MyVariableArray = "DBName = $Database"
	
	       invoke-sqlcmd -inputfile $droppedatabase -serverinstance $Server -database master -Variable $MyVariableArray -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	       #In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	       CD $curScriptLocation

            $curtime = Get-Date -format s
            $outmsg = $curtime + "------> Finished drop for database " + $Database
            Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan
        }
        catch [system.exception] {
	       Write-Host "Error occurred in InstallerScripts\DropPEDatabase.sql: " -foregroundcolor red -backgroundcolor black
	       Write-Host "$_" -foregroundcolor red -backgroundcolor black
            $curtime = Get-Date -format s
	       Write-Host "Aborting uninstall, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
           throw "Uninstall failed"
	       break
        }
    }
} # end of DB object cleanup logic



# clean up any server objects (SQL Agent jobs, master procs)
try {
    # we still pass DB name b/c even though these are all instance-level objects, the DB name is used to construct the name for the jobs
    $MyVariableArray = "DBName = $Database"
	
    invoke-sqlcmd -inputfile $deleteservobj -serverinstance $Server -database master -Variable $MyVariableArray -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
    #In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
    CD $curScriptLocation

    $curtime = Get-Date -format s
    $outmsg = $curtime + "------> Finished server object cleanup (DB tag used: " + $Database + ")"
    Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan
}
catch [system.exception] {
    Write-Host "Error occurred in InstallerScripts\DeleteServerObjects.sql: " -foregroundcolor red -backgroundcolor black
    Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
    Write-Host "Aborting uninstall, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Uninstall failed"
    break
}

Write-Host "" -foregroundcolor cyan -backgroundcolor black