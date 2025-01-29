<#
.SYNOPSIS
    
.DOCUMENTATION
    https://github.com/msftmroth/MessageStats

.DESCRIPTION
    Uses the MessageTrackingLogs of alle Exchange Servers in an Oragnization
    to create a csv of all mailflow statistics of a particular day.

.EXAMPLE
    ".\messagestats.ps1"               --> yesterday
    ".\messagestats.ps1 -AnalyzeDay 2" --> before yesterday

.VERSIONS
 V1.0 02/03/2011 - https://devblogs.microsoft.com/scripting/use-powershell-to-track-email-messages-in-exchange-server/
 V1.1 24/02/2011
 V1.2 08/10/2014 - to work with Exchange 2013
 V2.0 04/04/2019 
 V2.2 27/01/2025
 V2.4 28/01/2025 - Script cleanup, add/fixed some parts
 
#>

param(
    # Days to analyze in the past  
    [Parameter( Mandatory=$false,
                Position = 0,
                HelpMessage = "Days to analyze in the past (Default 1 (Yesterday) 2 the day before yesterday and so on... )")]
                [ValidatePattern("[0-9]")]
                [int] $AnalyzeDay = 1
               )

$version = "V2.4_28.01.2025"

try
{
    $ScriptPath = Split-Path -parent $MyInvocation.MyCommand.Path -ErrorAction Stop
}
catch
{
    Write-Host "`nDo not forget to save the script!" -ForegroundColor Red
}

Write-Host "Scriptversion: $version`n"

#Check if Exchange SnapIn is available and load it
if (!(Get-PSSession).ConfigurationName -eq "Microsoft.Exchange")
{
    if ((Get-PSSnapin -Registered).name -contains "Microsoft.Exchange.Management.PowerShell.SnapIn")
    {
        Write-Host "`nLoading the Exchange Powershell SnapIn..." -ForegroundColor Yellow
        Add-PSSnapin Microsoft.Exchange.Management.PowerShell.SnapIn -ErrorAction SilentlyContinue
        . $env:ExchangeInstallPath\bin\RemoteExchange.ps1
        Connect-ExchangeServer -auto -AllowClobber
    }
    else
    {
        write-host "`nExchange Management Tools are not installed. Run the script on a different machine." -ForegroundColor Red
        Return
    }
}

#Detect, where the script is executed
if (!(Get-ExchangeServer -Identity $env:COMPUTERNAME -ErrorAction SilentlyContinue))
{
    write-host "`nATTENTION: Script is executed on a non-Exchangeserver...`n" -ForegroundColor Cyan
}

$now = [DateTime]::Now
$today = [DateTime]::Today
$startdate = $today.AddDays(-$AnalyzeDay)
$stopdate = $startdate.AddHours(24).AddMilliseconds(-1)

$rundate = $($today.adddays(-$AnalyzeDay)).ToString("MM\/dd\/yyyy")

Write-Host "Analyzing mailflow for" -> $($startdate).ToString("MMMM dd, yyyy")

$outfile_date = ([datetime]$rundate).tostring("yyyy_MM_dd")

$outfile = "email_stats_" + $outfile_date + ".csv"

$dl_stat_file = "DL_stats.csv"

$accepted_domains = (Get-AcceptedDomain).domainname.domain
[regex]$dom_rgx = "`(?i)(?:" + (($accepted_domains | ForEach-Object {"@" + [regex]::escape($_)}) -join "|") + ")$"

$mbx_servers = (Get-ExchangeServer | where-object serverrole -match "Mailbox").fqdn
[regex]$mbx_rgx = "`(?i)(?:" + (($mbx_servers | ForEach-Object {"@" + [regex]::escape($_)}) -join "|") + ")\>$"

$msgid_rgx = "^\.+@.+\..+\$"

$hts = (Get-ExchangeServer | where-object IsHubTransportServer -eq $True).Name

$exch_addrs = @{}

$msgrec = @{}
$bytesrec = @{}

$msgrec_exch = @{}
$bytesrec_exch = @{}

$msgrec_smtpext = @{}
$bytesrec_smtpext = @{}

$total_msgsent = @{}
$total_bytessent = @{}
$unique_msgsent = @{}
$unique_bytessent = @{}

$total_msgsent_exch = @{}
$total_bytessent_exch = @{}
$unique_msgsent_exch = @{}
$unique_bytessent_exch = @{}

$total_msgsent_smtpext = @{}
$total_bytessent_smtpext = @{}
$unique_msgsent_smtpext=@{}
$unique_bytessent_smtpext = @{}

$dl = @{}

$obj_table = {
@"
Date = $rundate
User = $($address.split("@")[0])
Domain = $($address.split("@")[1])
Sent Total = $(0 + $total_msgsent[$address])
Sent MB Total = $("{0:F2}" -f $($total_bytessent[$address]/1mb))
Received Total = $(0 + $msgrec[$address])
Received MB Total = $("{0:F2}" -f $($bytesrec[$address]/1mb))
Sent Internal = $(0 + $total_msgsent_exch[$address])
Sent Internal MB = $("{0:F2}" -f $($total_bytessent_exch[$address]/1mb))
Sent External = $(0 + $total_msgsent_smtpext[$address])
Sent External MB = $("{0:F2}" -f $($total_bytessent_smtpext[$address]/1mb))
Received Internal = $(0 + $msgrec_exch[$address])
Received Internal MB = $("{0:F2}" -f $($bytesrec_exch[$address]/1mb))
Received External = $(0 + $msgrec_smtpext[$address])
Received External MB = $("{0:F2}" -f $($bytesrec_smtpext[$address]/1mb))
Sent Unique Total = $(0 + $unique_msgsent[$address])
Sent Unique MB Total = $("{0:F2}" -f $($unique_bytessent[$address]/1mb))
Sent Internal Unique  = $(0 + $unique_msgsent_exch[$address]) 
Sent Internal Unique MB = $("{0:F2}" -f $($unique_bytessent_exch[$address]/1mb))
Sent External  Unique = $(0 + $unique_msgsent_smtpext[$address])
Sent External Unique MB = $("{0:F2}" -f $($unique_bytessent_smtpext[$address]/1mb))
"@
}

$props = $obj_table.ToString().Split("`n") | ForEach-Object {if ($_ -match "(.+)="){$matches[1].trim()}}

$stat_recs = @()

function time_pipeline
{
    Param ($increment = 1000)
    
    Begin {$i=0 ; $timer = [diagnostics.stopwatch]::startnew()}
    
    Process
    {
        $i++
        if (!($i % $increment))
        {
            if ($Host.Name -ne "Windows PowerShell ISE Host")
            {
                Write-host -NoNewline "`rProcessed $i in $($timer.elapsed.totalseconds) seconds..."
            }
        }
        $_
    }
    End
    {
        write-host "`rProcessed $i log records in $($timer.elapsed.totalseconds) seconds."
        Write-Host "Average rate: $([int]($i/$timer.elapsed.totalseconds)) log recs/sec."
    }
}

foreach ($ht in $hts)
{
    Write-Host "`nStarted processing $ht"

    $events = Get-MessageTrackingLog -Server $ht -Start $startdate -End $stopdate -resultsize unlimited | time_pipeline
    
    Foreach ($event in $events)        
    {
        if ($event.eventid -eq "DELIVER" -and $event.source -eq "STOREDRIVER")
        {
            if ($event.messageid -match $mbx_rgx -and $event.sender -match $dom_rgx)
            {
                $total_msgsent[$event.sender] += $event.recipientcount
                $total_bytessent[$event.sender] += ($event.recipientcount * $event.totalbytes)
                $total_msgsent_exch[$event.sender] += $event.recipientcount
                $total_bytessent_exch[$event.sender] += ($event.totalbytes * $event.recipientcount)
                              
                ForEach ($rcpt in $event.recipients)
                {
                    $exch_addrs[$rcpt] ++
                    $msgrec[$rcpt] ++
                    $bytesrec[$rcpt] += $event.totalbytes
                    $msgrec_exch[$rcpt] ++
                    $bytesrec_exch[$rcpt] += $event.totalbytes
                }
            }
            else
            {
                if ($event.messageid -match $messageid_rgx)
                {
                    foreach ($rcpt in $event.recipients)
                    {
                        $exch_addrs[$rcpt] ++
                        $msgrec[$rcpt] ++
                        $bytesrec[$rcpt] += $event.totalbytes
                        $msgrec_smtpext[$rcpt] ++
                        $bytesrec_smtpext[$rcpt] += $event.totalbytes
                    }
                }
            }
        }
        if ($event.eventid -eq "RECEIVE" -and $event.source -eq "STOREDRIVER")
        {
            $exch_addrs[$event.sender] ++
            $unique_msgsent[$event.sender] ++
            $unique_bytessent[$event.sender] += $event.totalbytes
                              
            if ($event.recipients -match $dom_rgx)
            {
                $unique_msgsent_exch[$event.sender] ++
                $unique_bytessent_exch[$event.sender] += $event.totalbytes
            }

            if ($event.recipients -notmatch $dom_rgx)
            {
                $ext_count = ($event.recipients -notmatch $dom_rgx).count
                $unique_msgsent_smtpext[$event.sender] ++
                $unique_bytessent_smtpext[$event.sender] += $event.totalbytes
                $total_msgsent[$event.sender] += $ext_count
                $total_bytessent[$event.sender] += ($ext_count * $event.totalbytes)
                $total_msgsent_smtpext[$event.sender] += $ext_count
                $total_bytessent_smtpext[$event.sender] += ($ext_count * $event.totalbytes)
            }
        }
        if ($event.eventid -eq "EXPAND")
        {
            $dl[$event.relatedrecipientaddress] ++
        }
    }
}              

foreach ($address in $exch_addrs.keys)
{
    $stat_rec = (new-object psobject -property (ConvertFrom-StringData (&$obj_table)))
    $stat_recs += $stat_rec | select $props
}

$stat_recs | Export-Csv -Path $ScriptPath\$outfile -NoTypeInformation

if (Test-Path $dl_stat_file)
{
    $dl_stats = Import-Csv $dl_stat_file
    $dl_list = $dl_stats | ForEach-Object {$_.address}
}
else
{
    $dl_list = @()
    $dl_stats = @()
}

foreach ($dl_stat in $dl_stats)
{
    if ($dl[$dl_stat.address])
    {
        if ([datetime]$dl_stat.lastused -le [datetime]$rundate)
        { 
            $dl_stat.used = [int]$dl_stat.used + [int]$dl[$dl_stat.address]
            $dl_stat.lastused = $rundate
        }
    }
}
               
foreach ($key in $dl.keys)
{
    if ($dl_list -notcontains $key)
    {
        $new_rec = "" | select Address,Used,Since,LastUsed
        $new_rec.address = $key
        $new_rec.used = $dl[$key]
        $new_rec.Since = $rundate
        $new_rec.lastused = $rundate
        $dl_stats += @($new_rec)
    }
}

$dl_stats | Export-Csv -Path $ScriptPath\$dl_stat_file -NoTypeInformation -Force

Write-Host "`nRun time was $(((get-date) - $now).totalseconds) seconds."
Write-Host "Email stats file is $outfile"
Write-Host "DL usage stats file is $dl_stat_file"