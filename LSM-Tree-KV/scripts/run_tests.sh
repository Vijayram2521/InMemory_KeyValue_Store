#!/usr/bin/env bash
# Configure (if needed), build, and run the kv_tests suite via CTest.
# Usage: ./scripts/run_tests.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# On MSYS2, the interactive shell subsystem (e.g. MINGW64) and the actual
# compiler toolchain (e.g. UCRT64) can differ. When they do, cc1.exe fails to
# resolve its own dependent DLLs unless the toolchain's bin dir is on PATH.
if [ -d /c/msys64/ucrt64/bin ]; then
    export PATH="/c/msys64/ucrt64/bin:$PATH"
fi

cmake -S . -B build -G "MinGW Makefiles" >/dev/null
cmake --build build
ctest --test-dir build --output-on-failure
