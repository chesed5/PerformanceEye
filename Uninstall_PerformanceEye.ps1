#####
# PRODUCT:    PerformanceEye  https://github.com/amorelli005/PerformanceEye
# PROCEDURE:	Uninstall_PerformanceEye.ps1
#
# AUTHOR: Aaron Morelli
#	email@TBD.com
#	@sqlcrossjoin
#	sqlcrossjoin.wordpress.com					
#
#	PURPOSE: Uninstall the PerformanceEye database, its schema and T-SQL objects, and SQL Agent jobs
#   from a SQL Server instance.
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
# this only gets rid of server objects. (Even if a DB exists, it is not touched). A default DB tag of "PerformanceEye" is used to find the SQL Agent jobs
# ps prompt>.\Uninstall_PerformanceEye.ps1 -Server . 

# this only gets rid of server objects. The DB name has been specified as it is a non-default name, and is used to find the SQL Agent jobs to delete.
# ps prompt>.\Uninstall_PerformanceEye.ps1 -Server . -Database PerfEye1 -ServerObjectsOnly Y

# this gets rid of everything for the specified DB, including the database. If the DB is a general-purpose utility database (i.e. holds other DBA-related objects)
# then specifying -DropDatabase N will only remove the PE-related objects
# ps prompt>.\Uninstall_PerformanceEye.ps1 -Server . -Database PerformanceEye -ServerObjectsOnly N -DropDatabase Y
#####

param ( 
[Parameter(Mandatory=$true)][string]$Server, 
[Parameter(Mandatory=$false)][string]$Database,
[Parameter(Mandatory=$false)][string]$ServerObjectsOnly,
[Parameter(Mandatory=$false)][string]$DropDatabase
) 

Write-Host "PerformanceEye v0.5" -backgroundcolor black -foregroundcolor cyan
Write-Host "MIT License" -backgroundcolor black -foregroundcolor cyan
Write-Host "Copyright (c) 2016 Aaron Morelli" -backgroundcolor black -foregroundcolor cyan

## basic parameter checking 
if ($Server -eq $null) {
	Write-Host "Parameter -Server must be specified." -foregroundcolor red -backgroundcolor black
	Break
}

if ($Server -eq "") {
	Write-Host "Parameter -Server cannot be blank." -foregroundcolor red -backgroundcolor black
	Break
}

if ($Database -eq $null) {
    $Database = ""
}
else {
    $Database = $Database.TrimStart().TrimEnd()
}

if ( $DropDatabase -eq $null) {
    $DropDatabase = ""
}
else {
    $DropDatabase = $DropDatabase.TrimStart().TrimEnd()
}

#ServerObjectsOnly parm handling
$ServerObjectsOnly = $ServerObjectsOnly.TrimStart().TrimEnd().ToUpper()


if ( ( $ServerObjectsOnly -eq $null) -or ($ServerObjectsOnly -eq "") ) {
    if ($Database -eq "" ) {
        # a DB name wasn't specified, so we default ServerObjectsOnly to Y. This is sort of a safety measure... we want the user
        # to intentionally enter the DB name before blowing away all of the data that may have been collected. Recreating the procs
        # and jobs isn't as big of a loss, since it's just code.
        Write-Host "No DB name specified, so defaulting -ServerObjectsOnly parameter to Y. Only server objects will be deleted." -backgroundcolor black -foregroundcolor yellow
        Write-Host "SQL Agent jobs will be searched for with the prefix 'PerformanceEye'" -backgroundcolor black -foregroundcolor yellow
        $ServerObjectsOnly = "Y"
    }
    else {
        # we've been given a DB to target, so default to N
        $ServerObjectsOnly = "N"
    }
}
else {
    if ( ($ServerObjectsOnly -ne "N") -and ($ServerObjectsOnly -ne "Y") ) {
        Write-Host "Parameter -ServerObjectsOnly must be Y or N if specified" -foregroundcolor red -backgroundcolor black
	   Break
    }
}

if ($ServerObjectsOnly -eq "Y") {
    if ($Database -eq "") {
        # no DB specified, default to PerformanceEye (We need a DB for the SQL Agent prefix tag search)
        $Database = "PerformanceEye"
    }

    if ($DropDatabase -eq "Y") {
        Write-Host "Parameter -DropDatabase cannot be Y if -ServerObjectsOnly is Y" -foregroundcolor red -backgroundcolor black
	    Break    
    }
} 
else {
    # we're deleting DB objects, too. DB name and DropDatabase are required
    if ($Database -eq "") {
        Write-Host "Parameter -Database must be specified if -ServerObjectsOnly is N" -foregroundcolor red -backgroundcolor black
	    Break    
    }
    
    if ( ($DropDatabase -ne "N") -and ($DropDatabase -ne "Y") ) {
        Write-Host "Parameter -DropDatabase must be Y or N if -ServerObjectsOnly is N" -foregroundcolor red -backgroundcolor black
	    Break
    }
} #parm handling for DropDatabase & Database based on value of $ServerObjectsOnly

# avoid sql injection by limiting $Database to alphanumeric. (Yeah, this is cheap and dirty. Will revisit)
if ($Database -notmatch '^[a-z0-9]+$') { 
    Write-Host "Parameter -Database can only contain alphanumeric characters." -foregroundcolor red -backgroundcolor black
	Break
}

$CurScriptName = $MyInvocation.MyCommand.Name
$CurDur = $MyInvocation.MyCommand.Path
$CurDur = $CurDur.Replace($CurScriptName,"")
$curScriptLoc = $CurDur.TrimStart().TrimEnd()

if ( !($curScriptLoc.EndsWith("\")) ) {
	$curScriptLoc = $curScriptLoc + "\"
}

$installerlogsloc = $curScriptLoc + "InstallationLogs\"

$installerLogFile = $installerlogsloc + "PerformanceEye_uninstall" + "_{0:yyyyMMdd_HHmmss}.log" -f (Get-Date)

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Beginning uninstall..." 
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Parameter Validation complete. Proceeding with uninstall on server " + $Server + ", Database " + $Database + ", ServerObjectsOnly " + $ServerObjectsOnly
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan


$curtime = Get-Date -format s
$outmsg = $curtime + "------> Uninstall operations will be logged to " + $installerLogFile
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

CD $curScriptLoc 

powershell.exe -noprofile -file .\InstallerScripts\uninstall_database_objects.ps1 -Server $Server -Database $Database -ServerObjectsOnly $ServerObjectsOnly -DropDatabase $DropDatabase -curScriptLocation $curScriptLoc > $installerLogFile
