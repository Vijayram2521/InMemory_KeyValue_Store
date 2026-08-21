# Configure (if needed), build, and run the kv_tests suite via CTest.
# Usage: .\scripts\run_tests.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

cmake -S . -B build -G "MinGW Makefiles" | Out-Null
cmake --build build
ctest --test-dir build --output-on-failure
