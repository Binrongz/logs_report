#!/bin/bash

# Scalability Testing Script
# Tests performance with different thread counts

echo "========================================"
echo "Scenario D: Scalability Testing"
echo "========================================"
echo "Start time: $(date)"
echo ""

# Make sure program is compiled
make clean
make

# Create output directory
mkdir -p output/scalability

# Test different thread counts
for threads in 1 2 4 8 16 32; do
    echo ""
    echo "----------------------------------------"
    echo "Testing with $threads threads"
    echo "----------------------------------------"
    
    output_dir="output/scalability/${threads}threads/"
    mkdir -p $output_dir
    
    ./scenario_d data/subset_500.csv $output_dir $threads
    
    echo "Results saved to: $output_dir"
    sleep 1
done

echo ""
echo "========================================"
echo "All scalability tests completed"
echo "========================================"
echo "End time: $(date)"
echo ""
echo "Results summary:"
for threads in 1 2 4 8 16 32; do
    perf_file="output/scalability/${threads}threads/scenario_d_performance.json"
    if [ -f "$perf_file" ]; then
        throughput=$(grep "logs_per_second" $perf_file | awk -F': ' '{print $2}' | tr -d ',')
        echo "  ${threads} threads: ${throughput} logs/sec"
    fi
done