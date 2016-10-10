#####
# PRODUCT:    PerformanceEye  https://github.com/AaronMorelli/PerformanceEye
# PROCEDURE:	PerformanceEye_Installer.ps1
#
# AUTHOR:			Aaron Morelli
#					email@TBD.com
#					@sqlcrossjoin
#					sqlcrossjoin.wordpress.com					
#
#	PURPOSE: Install the PerformanceEye database, its schema and T-SQL objects, and SQL Agent jobs
#  on a SQL Server instance.
#
#	OUTSTANDING ISSUES: None at this time.
#
#   CHANGE LOG:	
#				2016-09-30	Aaron Morelli		Final code run-through and commenting
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
# ps prompt>.\PerformanceEye_Installer.ps1 -Server . -Database PerformanceEye -HoursToKeep 336

# the Database name can be any alphanumeric string. The Hours to Keep defines how much
# time the collected data is kept by default. (More specific retention policies can be 
# set up in the various Options tables
#####

param ( 
[Parameter(Mandatory=$true)][string]$Server, 
[Parameter(Mandatory=$false)][string]$Database,
[Parameter(Mandatory=$false)][string]$HoursToKeep,
[Parameter(Mandatory=$false)][string]$DBExists
) 

$ErrorActionPreference = "Stop"

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

$Database = $Database.TrimStart().TrimEnd()

if ( ($Database -eq $null) -or ($Database -eq "") )  {
	$Database = "PerformanceEye"
}

if ( ($HoursToKeep -eq $null) -or ($HoursToKeep -eq "") ) {
	$HoursToKeep = "336"
    # 14 days
}

[int]$HoursToKeep_num = [convert]::ToInt32($HoursToKeep, 10)

if ( ($HoursToKeep_num -le 0) -or ($HoursToKeep_num -gt 4320) ) {
    Write-Host "Parameter -HoursToKeep cannot be <= 0 or > 4320 (180 days)" -foregroundcolor red -backgroundcolor black
	Break
}

$DBExists = $DBExists.ToUpper().TrimStart().TrimEnd()

if ( ( $DBExists -eq $null) -or ($DBExists -eq "") ) {
    $DBExists = "N"
}

if ( ($DBExists -ne "N") -and ($DBExists -ne "Y") ) {
    Write-Host "Parameter -DBExists must be Y or N if specified" -foregroundcolor red -backgroundcolor black
	Break
}

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

$installerLogFile = $installerlogsloc + "PerformanceEye_installation" + "_{0:yyyyMMdd_HHmmss}.log" -f (Get-Date)

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Beginning installation..." 
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

$curtime = Get-Date -format s
$outmsg = $curtime + "------> Parameter Validation complete. Proceeding with installation on server " + $Server + ", Database " + $Database + ", HoursToKeep " + $HoursToKeep + ", DBExists " + $DBExists
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan


$curtime = Get-Date -format s
$outmsg = $curtime + "------> Installation operations will be logged to " + $installerLogFile
Write-Host $outmsg -backgroundcolor black -foregroundcolor cyan

CD $curScriptLoc 

powershell.exe -noprofile -command .\InstallerScripts\install_database_objects.ps1 -Server $Server -Database $Database -HoursToKeep $HoursToKeep -DBExists $DBExists -curScriptLocation $curScriptLoc > $installerLogFile
$scriptresult = $?

$curtime = Get-Date -format s

if ($scriptresult -eq $true) {
    Write-Host "Installation completed successfully" -backgroundcolor black -foregroundcolor green
}
else {
    Write-Host "Installation failed. Please consult $installerLogFile for more details." -backgroundcolor black -foregroundcolor red
    Write-Host "Installation aborted at: " + $curtime -foregroundcolor red -backgroundcolor black
}

