

import sys
from datetime import datetime


IDX_LOC  = 0
IDX_DATE = 1
IDX_TYPE = 3

def parse_date(date_str):

    formats = [
        '%m/%d/%y', 
        '%m/%d/%Y',  
        '%d/%m/%Y', 
        '%Y-%m-%d', 
        '%d-%b-%y'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

for line in sys.stdin:
    line = line.strip()
    if not line: continue
    
    parts = line.split(',')
    

    if len(parts) <= 3: continue

    try:

        location = parts[IDX_LOC].replace('"', '')
        date_raw = parts[IDX_DATE].replace('"', '')
        event_type = parts[IDX_TYPE].replace('"', '').lower()
        
    except IndexError:
        continue

    if "regains" in event_type:
        dt = parse_date(date_raw)
        if dt:
            print("%s\t%s" % (location, dt.strftime('%Y-%m-%d')))