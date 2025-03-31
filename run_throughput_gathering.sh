#!/bin/bash

# Check if correct number of arguments is provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <path_to_SimulateManyRequests> <path_to_ParquetRequest>"
    exit 1
fi

# Paths to the executables
simulate_many_requests_path="$1"
parquet_request_path="$2"

# Array of x values
# x_values=(0.0 1.0)
x_values=(0.0 0.01 0.05 0.1 0.5 1.0)

# Array of commands
commands=("select" "range")

# Iterate over each command
for command in "${commands[@]}"; do
    # Iterate over each x value
    for x in "${x_values[@]}"; do
        # Run the command with the current x value and command
        "$simulate_many_requests_path" "$parquet_request_path" "$command" 10 "$x"
    done
done
