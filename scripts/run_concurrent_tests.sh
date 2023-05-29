#!/bin/bash

# Number of test runs
num_runs=10

# Function to run `cargo test` and capture the output and execution time
run_test() {
    output=$(cargo test --quiet 2>&1)
    if [[ $output == *"test result: FAILED"* ]]; then
        # Extract the name of the failed test
        failed_tests=$(echo "$output" | awk '/test .* ... FAILED/ { print $2 }')

        # Split the failed test names into an array
        IFS=$'\n' read -r -d '' -a failed_test_names <<<"$failed_tests"

        # Increment the failure count for each failed test
        for failed_test in "${failed_test_names[@]}"; do
            for ((i = 0; i < num_runs; i++)); do
                if [[ "${failed_test_names[i]}" == "$failed_test" ]]; then
                    ((test_failures[i]++))
                    break
                fi
            done
        done
    fi
}

# Initialize arrays
failed_test_names=()
test_failures=()

# Run `cargo test` for the specified number of runs
for ((i = 0; i < num_runs; i++)); do
    failed_test_names[i]=""
    test_failures[i]=0

    run_test &
done

# Wait for all background tasks to complete
wait

# Print the test summary
echo "Test Summary:"
echo "--------------------------------"
total_tests=$((num_runs * num_runs))
total_failures=0
for ((i = 0; i < num_runs; i++)); do
    failure_count=${test_failures[i]}
    success_count=$((num_runs - failure_count))
    
    if [[ "${failed_test_names[i]}" != "" ]]; then
        echo "${failed_test_names[i]}: $success_count successes, $failure_count failures"
        ((total_failures += failure_count))
    fi
    
    ((total_tests += num_runs))
done

successes=$((total_tests - total_failures))
echo "--------------------------------"
echo "Total Tests: $total_tests"
echo "Successes: $successes"
echo "Failures: $total_failures"
