#!/usr/bin/env python3
import sys

# Read each line from standard input
for line in sys.stdin:
    line = line.strip()
    
    # Split the line into fields (standard format: date time store product cost payment)
    # Split by tab if present (standard Hadoop TSV streaming), otherwise fallback to whitespace
    data = line.split('\t') if '\t' in line else line.split()

    # Ensure the line has exactly 6 columns before processing
    if len(data) == 6:
        date, time, store, product, cost, payment = data
        
        # Print the Store and the Cost separated by a tab
        print('%s\t%s' % (store, cost))