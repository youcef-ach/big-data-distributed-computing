#!/usr/bin/env python3
import sys

current_store = None
total_sales = 0.0
store = None

# Read input from STDIN
for line in sys.stdin:
    line = line.strip()

    # Parse the input (Store [tab] Cost)
    try:
        store, cost = line.split('\t', 1)
        cost = float(cost)  # Convert string to float for math
    except ValueError:
        continue  # Skip lines that are formatted incorrectly

    # If the store is the same as the previous line, add to the total
    if current_store == store:
        total_sales += cost
    else:
        # If the store name changed, print the result for the previous store
        if current_store:
            print('%s\t%s' % (current_store, total_sales))
        
        # Start counting for the new store
        current_store = store
        total_sales = cost

# Print the final store after the loop finishes
if current_store is not None:
    print('%s\t%s' % (current_store, total_sales))
