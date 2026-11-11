
import sys


for line in sys.stdin:

    line = line.strip()

    words = line.split()
    
    for word in words:

        chars = list(word)
        chars.sort()
        sorted_key = "".join(chars)
        
        print("%s\t%s" % (sorted_key, word))