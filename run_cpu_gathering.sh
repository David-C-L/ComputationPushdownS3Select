#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <path_to_SimulateManyRequests> <path_to_ParquetRequest> <path_to_ParquetServer>"
    exit 1
fi

# Paths to executables
simulate_many_requests_path="$1"
parquet_request_path="$2"
parquet_server_path="$3"

# Array of x values
x_values=(0.01 0.05 0.1 0.5)
x_values=(0.0 0.001 0.005 0.01 0.05 0.1 0.3 0.5 0.7 0.8 0.9 1.0)

# Array of methods
methods=("select" "range")

# Iterate over each method
for method in "${methods[@]}"; do
    # Iterate over each x value
    for x in "${x_values[@]}"; do
        # Dynamically generate vtune command
        vtune_command="vtune -collect hotspots -duration 120 -result-dir ${method}_10_${x}_vtune -- $parquet_server_path temp.csv"

        # Dynamically generate SimulateManyRequests command
        simulate_command="$simulate_many_requests_path $parquet_request_path ${method} 10 ${x}"

        echo "Starting vtune command: $vtune_command"
        echo "Starting SimulateManyRequests command after 10 seconds: $simulate_command"

        # Start the simulate command as a background process with a 10-second delay
        (
            sleep 10
            eval "$simulate_command"
        ) &

        # Run the vtune command in the foreground
        eval "$vtune_command"

        # Wait for the simulate command to complete, if still running
        wait
        echo "Finished processing method: $method with x value: $x"
    done
done
