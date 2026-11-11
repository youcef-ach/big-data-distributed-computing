from __future__ import print_function
from pyspark import SparkContext
import sys


sc = SparkContext(appName="ArbresAnalysis")


if len(sys.argv) != 2:
    print("Usage: spark-submit arbres_analysis.py <input_file_path>")
    sys.exit(1)

input_file = sys.argv[1]

raw_rdd = sc.textFile(input_file)
header = raw_rdd.first()
data_rdd = raw_rdd.filter(lambda line: line != header)

print("--- Task 1 ---")

print("Total number of trees (lines): %s" % data_rdd.count())

def parse_line(line):
    parts = line.split(';') 
    try:
        genus = parts[2].strip()
        if parts[6].strip() == "":
            return None
        height = float(parts[6]) 
        return (genus, height)
    except:
        return None

parsed_rdd = data_rdd.map(parse_line).filter(lambda x: x is not None)
parsed_rdd.cache()


avg_height = parsed_rdd.map(lambda x: x[1]).mean()

print("\n--- Task 2 ---")

print("Average tree height: %.2f" % avg_height)


tallest_tree = parsed_rdd.map(lambda x: (x[1], x[0])) \
                         .sortByKey(ascending=False) \
                         .first()

print("\n--- Task 3 ---")

print("Tallest tree genus: %s (Height: %s)" % (tallest_tree[1], tallest_tree[0]))


genus_counts = parsed_rdd.map(lambda x: (x[0], 1)) \
                         .reduceByKey(lambda a, b: a + b) \
                         .collect()

print("\n--- Task 4 ---")
print("Number of trees per genus:")
for genus, count in sorted(genus_counts):

    print("%s: %s" % (genus, count))

sc.stop()