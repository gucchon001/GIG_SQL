# Test execution script

param(
    [string]$TestPath = "src/tests",
    [switch]$Coverage,
    [switch]$Verbose,
    [string]$Markers = $null
)

Write-Host "========================================" -ForegroundColor Green
Write-Host "Jukuste CSV Download Tool" -ForegroundColor Green
Write-Host "Test Execution" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

# Python path setup
$env:PYTHONPATH = "$PWD;$PWD\src"

# Build test execution options
$TestArgs = @()
$TestArgs += $TestPath

if ($Verbose) {
    $TestArgs += "-v"
}

if ($Coverage) {
    $TestArgs += "--cov=src"
    $TestArgs += "--cov-report=html"
    $TestArgs += "--cov-report=term-missing"
}

if ($Markers) {
    $TestArgs += "-m"
    $TestArgs += $Markers
}

Write-Host "Test configuration:" -ForegroundColor Cyan
Write-Host "  Test path: $TestPath" -ForegroundColor White
Write-Host "  Coverage: $Coverage" -ForegroundColor White
Write-Host "  Verbose: $Verbose" -ForegroundColor White
if ($Markers) {
    Write-Host "  Markers: $Markers" -ForegroundColor White
}
Write-Host "----------------------------------------" -ForegroundColor Green

try {
    Write-Host "Running tests..." -ForegroundColor Yellow
    
    # Execute pytest
    python -m pytest @TestArgs
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "----------------------------------------" -ForegroundColor Green
        Write-Host "All tests completed successfully" -ForegroundColor Green
        
        if ($Coverage) {
            Write-Host "Coverage report generated in htmlcov/" -ForegroundColor Cyan
        }
        
        Write-Host "========================================" -ForegroundColor Green
    } else {
        Write-Host "----------------------------------------" -ForegroundColor Red
        Write-Host "Test execution failed" -ForegroundColor Red
        Write-Host "Exit code: $LASTEXITCODE" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
        exit $LASTEXITCODE
    }
} catch {
    Write-Host "----------------------------------------" -ForegroundColor Red
    Write-Host "Exception occurred during test execution: $_" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Usage examples:" -ForegroundColor Cyan
Write-Host "  .\run_tests.ps1" -ForegroundColor White
Write-Host "  .\run_tests.ps1 -Coverage" -ForegroundColor White
Write-Host "  .\run_tests.ps1 -Verbose" -ForegroundColor White
Write-Host "  .\run_tests.ps1 -Markers unit" -ForegroundColor White
Write-Host "  .\run_tests.ps1 -TestPath src/tests/test_core -Coverage" -ForegroundColor White