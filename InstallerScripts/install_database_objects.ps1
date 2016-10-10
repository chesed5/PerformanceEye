#####
# PRODUCT:    PerformanceEye  https://github.com/AaronMorelli/PerformanceEye
# PROCEDURE:	install_database_objects.ps1
#
# AUTHOR: Aaron Morelli
#	email@TBD.com
#	@sqlcrossjoin
#	sqlcrossjoin.wordpress.com					
#
#	PURPOSE: Install the PerformanceEye database, its schema and T-SQL objects, and SQL Agent jobs
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
# <this should not be called directly. Call the parent PerformanceEye_Installer.ps1 
# script instead>
#####

param ( 
[Parameter(Mandatory=$true)][string]$Server, 
[Parameter(Mandatory=$true)][string]$Database,
[Parameter(Mandatory=$true)][string]$HoursToKeep,
[Parameter(Mandatory=$true)][string]$DBExists,
[Parameter(Mandatory=$true)][string]$curScriptLocation
) 

$ErrorActionPreference = "Stop"

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Parameter Validation complete. Proceeding with installation on server " + $Server + ", Database " + $Database + ", HoursToKeep " + $HoursToKeep
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Loading SqlPs module"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

Import-Module SqlPs

Write-Host "" -foregroundcolor cyan -backgroundcolor black

$core_parent = $curScriptLocation + "CorePE\"
$core_config = $core_parent + "CorePEConfig.sql"
$core_schemas = $core_parent + "CreateSchemas.sql"
$core_tables = $core_parent + "01_Tables"
$core_triggerstypes = $core_parent + "02_TriggersAndTypes"
$core_functions = $core_parent + "03_Functions"
$core_views = $core_views + "04_Views"
$core_procedures = $core_parent + "05_Procs"


$autowho_parent = $curScriptLocation + "AutoWho\"
$autowho_config = $autowho_parent + "AutoWhoConfig.sql"
$autowho_tables = $autowho_parent + "01_Tables"
$autowho_triggers = $autowho_parent + "02_Triggers"
$autowho_views = $autowho_parent + "03_Views"
$autowho_procedures = $autowho_parent + "04_Procs"

## this is not implemented yet
# $servereye_parent = $curScriptLocation + "servereye\"
# $servereye_functions = $servereye_parent + "functions"
# $servereye_procedures = $servereye_parent + "procedures"
# $servereye_schemas = $servereye_parent + "schemas"
# $servereye_tables = $servereye_parent + "tables"
# $servereye_views = $servereye_parent + "views"

$job_core = $curScriptLocation + "Jobs\PerfEyeMaster.sql"
$job_autowho = $curScriptLocation + "Jobs\AutoWhoTrace.sql"

$masterprocs_parent = $curScriptLocation + "masterobjs\"


# Installation scripts
$createpedb = $curScriptLocation + "InstallerScripts\CreatePEDatabase.sql"
$pedbexistcheck = $curScriptLocation + "InstallerScripts\DBExistenceCheck.sql"
$deletedbobj = $curScriptLocation + "InstallerScripts\DeleteDatabaseObjects.sql"
$deleteservobj = $curScriptLocation + "InstallerScripts\DeleteServerObjects.sql"

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
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}


if ( $DBExists -eq "N" ) {
    #Create it!
    $curtime = Get-Date -format s
    $outmsg = $curtime + "------> Creating PE database: " + $Database
    Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

    try {
	   $MyVariableArray = "DBName = $Database"
	
	   invoke-sqlcmd -inputfile $createpedb -serverinstance $Server -database master -Variable $MyVariableArray -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	   #In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	   CD $curScriptLocation

        $curtime = Get-Date -format s
        $outmsg = $curtime + "------> Finished create for database " + $Database
        Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan
    }
    catch [system.exception] {
    	Write-Host "Error occurred in InstallerScripts\CreatePEDatabase.sql: " -foregroundcolor red -backgroundcolor black
    	Write-Host "$_" -foregroundcolor red -backgroundcolor black
        $curtime = Get-Date -format s
    	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
        throw "Installation failed"
    	break
    }
}
else {
    # scrub it of any existing PE objects. (Other objects aren't touched, so that PE can safely exist in an already-existing utility database)
    $curtime = Get-Date -format s
    $outmsg = $curtime + "------> Scrubbing PE database: " + $Database + " of any previously-installed PE objects."
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
    	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
        throw "Installation failed"
    	break
    }

}  # end of if $DBExists -eq "N" block

Write-Host "" -foregroundcolor cyan -backgroundcolor black


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
	Write-Host "Error occurred in DeleteServerObjects.sql: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}

Write-Host "" -foregroundcolor cyan -backgroundcolor black


$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating schemas"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# Schemas  
try {
	invoke-sqlcmd -inputfile $core_schemas -serverinstance $Server -database $Database -Variable $MyVariableArray -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished creating schemas" -foregroundcolor cyan -backgroundcolor black
}
catch [system.exception] {
	Write-Host "Error occurred when creating schemas: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
} # Schemas

Write-Host "" -foregroundcolor cyan -backgroundcolor black

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating core tables"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# Core tables
try {
	(dir $core_tables) |  
		ForEach-Object {  
			$curScript = $_.FullName
			$curFileName = $_.Name

			invoke-sqlcmd -inputfile $curScript -serverinstance $Server -database $Database -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
			#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
			CD $curScriptLocation

		}
}
catch [system.exception] {
	Write-Host "Error occurred when creating core tables, in file: " + $curScript -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}  # end of Core Tables block

Write-Host "" -foregroundcolor cyan -backgroundcolor black

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating core triggers and types"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# Core triggers and types
try {
	(dir $core_triggerstypes) |  
		ForEach-Object {  
			$curScript = $_.FullName
			$curFileName = $_.Name

			invoke-sqlcmd -inputfile $curScript -serverinstance $Server -database $Database -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
			#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
			CD $curScriptLocation

		}
}
catch [system.exception] {
	Write-Host "Error occurred when creating core triggers and types, in file: " + $curScript -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}  # end of Core triggers and types block

Write-Host "" -foregroundcolor cyan -backgroundcolor black


### NOTE: skipping "03_Functions" until we have a function.

### NOTE: skipping "04_Views" until we have a view

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating core procedures"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# Core procedures
try {
	(dir $core_procedures) |  
		ForEach-Object {  
			$curScript = $_.FullName
			$curFileName = $_.Name

			invoke-sqlcmd -inputfile $curScript -serverinstance $Server -database $Database -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
			#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
			CD $curScriptLocation
		}
}
catch [system.exception] {
	Write-Host "Error occurred when creating core procedures, in file " + $curScript -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}  # end of Core procedures

Write-Host "" -foregroundcolor cyan -backgroundcolor black

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Configuring core objects"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# AutoWho config
try {
	invoke-sqlcmd -inputfile $core_config -serverinstance $Server -database $Database -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished core configuration" -foregroundcolor cyan -backgroundcolor black
}
catch [system.exception] {
	Write-Host "Error occurred during core configuration: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
} # core config

Write-Host "" -foregroundcolor cyan -backgroundcolor black


$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating AutoWho tables"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# AutoWho tables
try {
	(dir $autowho_tables) |  
		ForEach-Object {  
			$curScript = $_.FullName
			$curFileName = $_.Name

			invoke-sqlcmd -inputfile $curScript -serverinstance $Server -database $Database -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
			#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
			CD $curScriptLocation
		}
}
catch [system.exception] {
	Write-Host "Error occurred when creating AutoWho tables, in file: " + $curScript -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}  # end of AutoWho tables block

Write-Host "" -foregroundcolor cyan -backgroundcolor black

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating AutoWho triggers"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# AutoWho triggers
try {
	(dir $autowho_triggers) |  
		ForEach-Object {  
			$curScript = $_.FullName
			$curFileName = $_.Name

			invoke-sqlcmd -inputfile $curScript -serverinstance $Server -database $Database -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
			#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
			CD $curScriptLocation
		}
}
catch [system.exception] {
	Write-Host "Error occurred when creating AutoWho triggers, in file: " + $curScript -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}  # end of AutoWho functions

Write-Host "" -foregroundcolor cyan -backgroundcolor black

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating AutoWho views"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# AutoWho Views
try {
	(dir $autowho_views) |  
		ForEach-Object {  
			$curScript = $_.FullName
			$curFileName = $_.Name

			invoke-sqlcmd -inputfile $curScript -serverinstance $Server -database $Database -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
			#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
			CD $curScriptLocation
		}
}
catch [system.exception] {
	Write-Host "Error occurred when creating AutoWho views, in file: " + $curScript -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}  # end of AutoWho views

Write-Host "" -foregroundcolor cyan -backgroundcolor black

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating AutoWho procedures"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# AutoWho procedures
try {
	(dir $autowho_procedures) |  
		ForEach-Object {  
			$curScript = $_.FullName
			$curFileName = $_.Name

			invoke-sqlcmd -inputfile $curScript -serverinstance $Server -database $Database -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
			#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
			CD $curScriptLocation
		}
}
catch [system.exception] {
	Write-Host "Error occurred when creating AutoWho procedures, in file: " + $curScript -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}  # end of AutoWho procedures

Write-Host "" -foregroundcolor cyan -backgroundcolor black

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Configuring AutoWho"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# AutoWho config
try {
	$MyVariableArray = "HoursToKeep = $HoursToKeep"
	
	invoke-sqlcmd -inputfile $autowho_config -serverinstance $Server -database $Database -Variable $MyVariableArray -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished AutoWho configuration" -foregroundcolor cyan -backgroundcolor black
}
catch [system.exception] {
	Write-Host "Error occurred during AutoWho configuration, in file: " + $curScript -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
} # AutoWho config

Write-Host "" -foregroundcolor cyan -backgroundcolor black


# we take our __TEMPLATE versions of the master procs and create versions with $Database substituted for @@PEDATABASENAME@@
# Note: currently sp_PE_JobMatrix and sp_PE_MDFUsed do not have any references to the PE database
$masterproc_JM = $masterprocs_parent + "sp_PE_JobMatrix.sql"

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating sp_PE_JobMatrix"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

try {
	invoke-sqlcmd -inputfile $masterproc_JM -serverinstance $Server -database master -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished creating sp_PE_JobMatrix" -foregroundcolor cyan -backgroundcolor black

}
catch [system.exception] {
	Write-Host "Error occurred while creating sp_PE_JobMatrix: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}

Write-Host "" -foregroundcolor cyan -backgroundcolor black


$masterproc_MDF = $masterprocs_parent + "sp_PE_MDFUsed.sql"

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating sp_PE_MDFUsed"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

try {
	invoke-sqlcmd -inputfile $masterproc_MDF -serverinstance $Server -database master -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished creating sp_PE_MDFUsed" -foregroundcolor cyan -backgroundcolor black

}
catch [system.exception] {
	Write-Host "Error occurred while creating sp_PE_MDFUsed: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}

Write-Host "" -foregroundcolor cyan -backgroundcolor black


$masterproc_LR = $masterprocs_parent + "sp_PE_LongRequests__TEMPLATE.sql"
$masterproc_LR_Replace = $masterprocs_parent + "sp_PE_LongRequests__" + $Database + ".sql"

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating sp_PE_LongRequests"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

if (Test-Path $masterproc_LR_Replace) {
	Remove-Item $masterproc_LR_Replace
}

(Get-Content $masterproc_LR) | Foreach-Object { $_ -replace '@@PEDATABASENAME@@', $Database } | Set-Content $masterproc_LR_Replace

try {
	invoke-sqlcmd -inputfile $masterproc_LR_Replace -serverinstance $Server -database master -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished creating sp_PE_LongRequests" -foregroundcolor cyan -backgroundcolor black

}
catch [system.exception] {
	Write-Host "Error occurred while creating sp_PE_LongRequests: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}

Write-Host "" -foregroundcolor cyan -backgroundcolor black

$masterproc_QC = $masterprocs_parent + "sp_PE_QueryCamera__TEMPLATE.sql"
$masterproc_QC_Replace = $masterprocs_parent + "sp_PE_QueryCamera__" + $Database + ".sql"

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating sp_PE_QueryCamera"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

if (Test-Path $masterproc_QC_Replace) {
	Remove-Item $masterproc_QC_Replace
}

(Get-Content $masterproc_QC) | Foreach-Object { $_ -replace '@@PEDATABASENAME@@', $Database } | Set-Content $masterproc_QC_Replace

try {
	invoke-sqlcmd -inputfile $masterproc_QC_Replace -serverinstance $Server -database master -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished creating sp_PE_QueryCamera" -foregroundcolor cyan -backgroundcolor black

}
catch [system.exception] {
	Write-Host "Error occurred while creating sp_PE_QueryCamera: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}

Write-Host "" -foregroundcolor cyan -backgroundcolor black

$masterproc_QP = $masterprocs_parent + "sp_PE_QueryProgress__TEMPLATE.sql"
$masterproc_QP_Replace = $masterprocs_parent + "sp_PE_QueryProgress__" + $Database + ".sql"

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating sp_PE_QueryProgress"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

if (Test-Path $masterproc_QP_Replace) {
	Remove-Item $masterproc_QP_Replace
}

(Get-Content $masterproc_QP) | Foreach-Object { $_ -replace '@@PEDATABASENAME@@', $Database } | Set-Content $masterproc_QP_Replace

try {
	invoke-sqlcmd -inputfile $masterproc_QP_Replace -serverinstance $Server -database master -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished creating sp_PE_QueryProgress" -foregroundcolor cyan -backgroundcolor black

}
catch [system.exception] {
	Write-Host "Error occurred while creating sp_PE_QueryProgress: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}

Write-Host "" -foregroundcolor cyan -backgroundcolor black


$masterproc_SS = $masterprocs_parent + "sp_PE_SessionSummary__TEMPLATE.sql"
$masterproc_SS_Replace = $masterprocs_parent + "sp_PE_SessionSummary__" + $Database + ".sql"

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating sp_PE_SessionSummary"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

if (Test-Path $masterproc_SS_Replace) {
	Remove-Item $masterproc_SS_Replace
}

(Get-Content $masterproc_SS) | Foreach-Object { $_ -replace '@@PEDATABASENAME@@', $Database } | Set-Content $masterproc_SS_Replace

try {
	invoke-sqlcmd -inputfile $masterproc_SS_Replace -serverinstance $Server -database master -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished creating sp_PE_SessionSummary" -foregroundcolor cyan -backgroundcolor black

}
catch [system.exception] {
	Write-Host "Error occurred while creating sp_PE_SessionSummary: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}

Write-Host "" -foregroundcolor cyan -backgroundcolor black


$masterproc_SV = $masterprocs_parent + "sp_PE_SessionViewer__TEMPLATE.sql"
$masterproc_SV_Replace = $masterprocs_parent + "sp_PE_SessionViewer__" + $Database + ".sql"

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating sp_PE_SessionViewer"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

if (Test-Path $masterproc_SV_Replace) {
	Remove-Item $masterproc_SV_Replace
}

(Get-Content $masterproc_SV) | Foreach-Object { $_ -replace '@@PEDATABASENAME@@', $Database } | Set-Content $masterproc_SV_Replace

try {
	invoke-sqlcmd -inputfile $masterproc_SV_Replace -serverinstance $Server -database master -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished creating sp_PE_SessionViewer" -foregroundcolor cyan -backgroundcolor black

}
catch [system.exception] {
	Write-Host "Error occurred while creating sp_PE_SessionViewer: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
}

Write-Host "" -foregroundcolor cyan -backgroundcolor black



$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating Performance Eye master job"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# PerfEye Master job
try {
	$MyVariableArray = "DBName = $Database"
	
	invoke-sqlcmd -inputfile $job_core -serverinstance $Server -database msdb -Variable $MyVariableArray -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished creating Performance Eye Master job" -foregroundcolor cyan -backgroundcolor black
}
catch [system.exception] {
	Write-Host "Error occurred when creating Performance Eye Master job: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
} # Trace Master job

Write-Host "" -foregroundcolor cyan -backgroundcolor black

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Creating AutoWho trace job"
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

# AutoWho job
try {
	$MyVariableArray = "DBName = $Database"
	
	invoke-sqlcmd -inputfile $job_autowho -serverinstance $Server -database msdb -Variable $MyVariableArray -QueryTimeout 65534 -AbortOnError -Verbose -outputsqlerrors $true
	#In Windows 2012 R2, we are ending up in the SQLSERVER:\ prompt, when really we want to be in the file system provider. Doing a simple "CD" command gets us back there
	CD $curScriptLocation

	Write-Host "Finished creating AutoWho trace job" -foregroundcolor cyan -backgroundcolor black
}
catch [system.exception] {
	Write-Host "Error occurred when creating AutoWho trace job: " -foregroundcolor red -backgroundcolor black
	Write-Host "$_" -foregroundcolor red -backgroundcolor black
    $curtime = Get-Date -format s
	Write-Host "Aborting installation, abort time: " + $curtime -foregroundcolor red -backgroundcolor black
    throw "Installation failed"
	break
} # AutoWho job

Write-Host "" -foregroundcolor cyan -backgroundcolor black