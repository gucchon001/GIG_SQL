# Interactive Table Selection and Update Script
# Select from active table list and update

param(
    [string]$Mode = "test",
    [string]$Config = "config/settings.ini"
)

Write-Host "========================================" -ForegroundColor Green
Write-Host "Interactive Table Update Tool" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

# Execution mode setting
if ($Mode.ToLower() -eq "production") {
    Write-Host "Production Mode" -ForegroundColor Yellow
    $ExecutionColumn = "実行対象"
} elseif ($Mode.ToLower() -eq "test") {
    Write-Host "Test Mode" -ForegroundColor Yellow
    $ExecutionColumn = "テスト実行"
} else {
    Write-Host "Invalid mode: $Mode" -ForegroundColor Red
    Write-Host "Available modes: production, test" -ForegroundColor Red
    exit 1
}

Write-Host "Config file: $Config" -ForegroundColor Cyan
Write-Host "Execution column: $ExecutionColumn" -ForegroundColor Cyan
Write-Host "----------------------------------------" -ForegroundColor Green

# Create temporary Python script to get active table list
$TempScript = @"
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from core.config.config_loader import load_config
from core.data.subcode_loader import load_sql_file_list_from_spreadsheet

try:
    config_file = '$Config'
    ssh_config, db_config, local_port, additional_config = load_config(config_file)
    
    # Get active table list from spreadsheet
    sql_files_list = load_sql_file_list_from_spreadsheet(
        additional_config['spreadsheet_id'],
        additional_config['eachdata_sheet'],
        additional_config['json_keyfile_path'],
        '$ExecutionColumn'
    )
    
    # Create unique table name list
    active_tables = []
    for entry in sql_files_list:
        main_table_name = entry[10]  # Main table name
        if main_table_name and main_table_name.strip() and main_table_name not in active_tables:
            active_tables.append(main_table_name)
    
    # Output table names
    for i, table in enumerate(sorted(active_tables), 1):
        print(f"{i}:{table}")
        
except Exception as e:
    print(f"ERROR:{e}")
    sys.exit(1)
"@

try {
    # Activate Python virtual environment
    Write-Host "Activating Python virtual environment..." -ForegroundColor Yellow
    $ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)
    $venvPath = Join-Path $ProjectRoot "venv"
    $activateScript = "$venvPath\Scripts\Activate.ps1"
    $activateBat = "$venvPath\Scripts\activate.bat"
    
    if (Test-Path $activateScript) {
        & $activateScript
        Write-Host "Virtual environment activated (PowerShell)" -ForegroundColor Green
    } elseif (Test-Path $activateBat) {
        Write-Host "Using batch file activation..." -ForegroundColor Yellow
        & $activateBat
        Write-Host "Virtual environment activated (Batch)" -ForegroundColor Green
    } else {
        Write-Warning "Virtual environment not found: $venvPath"
        Write-Host "Continuing without virtual environment..." -ForegroundColor Yellow
    }

    # Move to project root
    Push-Location $ProjectRoot
    
    # Create temporary Python script file
    $TempScriptFile = Join-Path $ProjectRoot "temp_get_tables.py"
    $TempScript | Out-File -FilePath $TempScriptFile -Encoding UTF8
    
    Write-Host "Getting active table list..." -ForegroundColor Yellow
    
    # Set UTF-8 encoding for Python output and console
    $env:PYTHONIOENCODING = "utf-8"
    $env:PYTHONLEGACYSTDIO = "utf-8"
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::InputEncoding = [System.Text.Encoding]::UTF8
    
    # Execute Python script to get table list
    $Output = python $TempScriptFile 2>&1
    
    # Delete temporary file
    Remove-Item $TempScriptFile -Force -ErrorAction SilentlyContinue
    
    # Error check
    $ErrorLines = $Output | Where-Object { $_ -like "ERROR:*" }
    if ($ErrorLines) {
        Write-Host "Error occurred:" -ForegroundColor Red
        $ErrorLines | ForEach-Object { Write-Host $_ -ForegroundColor Red }
        exit 1
    }
    
    # Parse table list
    $Tables = @()
    foreach ($line in $Output) {
        if ($line -match "^(\d+):(.+)$") {
            $Tables += @{
                Number = $matches[1]
                Name = $matches[2]
            }
        }
    }
    
    if ($Tables.Count -eq 0) {
        Write-Host "No active tables found." -ForegroundColor Yellow
        Write-Host "No tables are set to TRUE in execution column '$ExecutionColumn'." -ForegroundColor Yellow
        exit 0
    }
    
    # Display table list
    Write-Host ""
    Write-Host "Active Table List:" -ForegroundColor Cyan
    Write-Host "==================" -ForegroundColor Cyan
    foreach ($table in $Tables) {
        Write-Host "  $($table.Number). $($table.Name)" -ForegroundColor White
    }
    Write-Host ""
    
    # Prompt user for selection
    do {
        $UserInput = Read-Host "Enter table number to update (1-$($Tables.Count), 0=Cancel)"
        
        if ($UserInput -eq "0") {
            Write-Host "Cancelled." -ForegroundColor Yellow
            exit 0
        }
        
        $SelectedNumber = [int]::TryParse($UserInput, [ref]$null)
        if ($SelectedNumber -and $UserInput -ge 1 -and $UserInput -le $Tables.Count) {
            $SelectedTable = $Tables[$UserInput - 1]
            break
        } else {
            Write-Host "Invalid number. Please enter a number between 1 and $($Tables.Count)." -ForegroundColor Red
        }
    } while ($true)
    
    Write-Host ""
    Write-Host "Selected table: $($SelectedTable.Name)" -ForegroundColor Green
    Write-Host "----------------------------------------" -ForegroundColor Green
    
    # Confirmation prompt
    $Confirm = Read-Host "Do you want to update this table? (y/N)"
    if ($Confirm -notmatch "^[Yy]") {
        Write-Host "Cancelled." -ForegroundColor Yellow
        exit 0
    }
    
    # Call individual update script
    $UpdateScript = Join-Path $ScriptDir "update_individual_table.ps1"
    Write-Host ""
    Write-Host "Executing individual update script..." -ForegroundColor Yellow
    Write-Host "----------------------------------------" -ForegroundColor Green
    
    # Execute PowerShell script
    & $UpdateScript -TableName $SelectedTable.Name -Mode $Mode -Config $Config
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "Table update completed successfully!" -ForegroundColor Green
        Write-Host "Table: $($SelectedTable.Name)" -ForegroundColor Green
    } else {
        Write-Host ""
        Write-Host "Error occurred during table update." -ForegroundColor Red
        Write-Host "Exit code: $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
    
} catch {
    Write-Host "----------------------------------------" -ForegroundColor Red
    Write-Host "Exception occurred during execution: $_" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    exit 1
} finally {
    # Return to original directory
    Pop-Location
}

Write-Host ""
Write-Host "Usage examples:" -ForegroundColor Cyan
Write-Host "  # Interactive selection in test mode" -ForegroundColor White
Write-Host "  .\select_and_update_table.ps1" -ForegroundColor White
Write-Host ""
Write-Host "  # Interactive selection in production mode" -ForegroundColor White  
Write-Host "  .\select_and_update_table.ps1 -Mode production" -ForegroundColor White
Write-Host ""
Write-Host "  # Use custom config file" -ForegroundColor White
Write-Host "  .\select_and_update_table.ps1 -Config 'config/custom.ini'" -ForegroundColor White
Write-Host ""
Write-Host "Script execution completed." -ForegroundColor Green