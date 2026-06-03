#!/usr/bin/env bash
set -euo pipefail

# Konfiguracja globalna całej serii testowej
export DURATION=60
export STOP_AFTER=1

# Definicja wielkości flot do przetestowania
FLEET_SIZES=(1 5 10 25 50)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "========================================================"
echo " STARTING FULL BENCHMARK SUITE"
echo " Total scenarios: ${#FLEET_SIZES[@]} | Duration per run: ${DURATION}s"
echo "========================================================"

for N in "${FLEET_SIZES[@]}"; do
    echo ""
    echo "########################################################"
    echo "  RUNNING BENCHMARK FOR N = ${N} ROBOTS"
    echo "########################################################"
    echo ""

    # Uruchomienie pojedynczego scenariusza z jawnym przekazaniem zmiennej N
    N="${N}" ./launch_fleet_bench.sh

    echo ""
    echo "=> Finished benchmark for N = ${N}."
    echo "Cooling down host system for 30 seconds"
    echo "########################################################"
    
    # Krótki odpoczynek dla systemu (Docker musi w pełni usunąć sieci i zwolnić RAM)
    sleep 30
done

echo ""
echo "========================================================"
echo " ALL BENCHMARKS COMPLETED SUCCESSFULLY!"
echo " Results available in ./benchmarks/results/ as run_*.txt"
echo "========================================================"