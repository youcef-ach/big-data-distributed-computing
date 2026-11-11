
import sys

current_key = None
anagram_list = []

for line in sys.stdin:
    line = line.strip()
    

    parts = line.split('\t', 1)
    if len(parts) != 2:
        continue
    
    key, word = parts


    if current_key != key:
        if current_key:
     
            print("%s\t%s" % (current_key, str(anagram_list)))
        
        current_key = key
        anagram_list = []

    anagram_list.append(word)


if current_key:
    print("%s\t%s" % (current_key, str(anagram_list)))