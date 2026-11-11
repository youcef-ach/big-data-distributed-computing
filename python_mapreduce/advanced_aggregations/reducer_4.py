

import sys

current_key = None
date_list = []

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t', 1)
    
    if len(parts) != 2: continue
    
    key, val = parts
    
    if current_key != key:
        if current_key:
            date_list.sort()
            print("%s: %s" % (current_key, ', '.join(date_list)))
        current_key = key
        date_list = []
    
    date_list.append(val)

if current_key:
    date_list.sort()
    print("%s: %s" % (current_key, ', '.join(date_list)))