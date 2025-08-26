# Instance name seen in Services
$InstanceName = 'SQLEXPRESS'

# Find the internal instance ID (e.g., MSSQL16.SQLEXPRESS)
$InstanceId = (Get-ItemProperty -Path 'HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL').$InstanceName
if (-not $InstanceId) { throw "SQL instance '$InstanceName' not found. Check Services.msc for the exact name." }

# Enable TCP at the protocol level
$TcpKey = "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\$InstanceId\MSSQLServer\SuperSocketNetLib\Tcp"
Set-ItemProperty -Path $TcpKey -Name 'Enabled' -Type DWord -Value 1

# Set a static port (1433) and clear dynamic ports for IPAll
$IpAllKey = Join-Path $TcpKey 'IPAll'
Set-ItemProperty -Path $IpAllKey -Name 'TcpPort' -Value '1433'
Set-ItemProperty -Path $IpAllKey -Name 'TcpDynamicPorts' -Value ''