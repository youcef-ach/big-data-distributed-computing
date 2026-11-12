#!/usr/bin/env python
import sys

current_word = None
current_count = 0
word = None

# Read input from STDIN
for line in sys.stdin:
    line = line.strip()

    # Parse the input we got from mapper.py
    try:
        word, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue

    # If this word is the same as the one we are processing, sum it
    if current_word == word:
        current_count += count
    else:
        # If the word changed, print the count for the previous word
        if current_word:
            print('%s\t%s' % (current_word, current_count))
        current_count = count
        current_word = word

# Don't forget to output the last word if needed!
if current_word is not None:
    print('%s\t%s' % (current_word, current_count))
