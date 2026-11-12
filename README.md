# MapReduce and Spark coursework

Small data-engineering exercises covering Python streaming mappers/reducers, Java Hadoop jobs and Spark analyses. These demonstrate grouping, aggregation and distributed-processing concepts; no large-scale throughput benchmark is claimed.

## Review and run

- `python_mapreduce/word_count/`: token counts over a sorted mapper stream.
- `python_mapreduce/sales_analytics/`: store-level revenue aggregation, with a small sample input.
- `python_mapreduce/advanced_aggregations/`: anagram grouping and date/event filtering exercises.
- `java_hadoop_mapreduce/`: Java coursework; the Maven files declare Hadoop 2.7.2, not Hadoop 3.x.
- `spark_analytics/`: Spark exercises requiring a separate Spark environment.

The Python streaming exercises use the standard library. Run their tests from this directory:

```sh
python -m unittest discover -s tests -v
```

The nine tests execute the real mapper/reducer programs as subprocesses. They cover token emission, aggregation, the sample sales pipeline, anagrams, date filtering, empty input and malformed word-count rows. Reducer input must be sorted by key; these local checks do not emulate Hadoop shuffle, cluster failure or Spark execution.

On a shell with `sort`:

```sh
printf 'a b a\n' | python python_mapreduce/word_count/mapper.py | sort | python python_mapreduce/word_count/reducer.py
```

Expected counts are `a: 2` and `b: 1`, emitted as tab-separated rows. Java and Spark components have not been rebuilt in this presentation pass. Their original dependencies should be assessed before use outside coursework.

## Scope and attribution

Academic exercises by Achouri Youcef Islem, ESI-SBA. The sample dataset and framework dependencies retain their original terms. No blanket MIT license is asserted for material whose licensing has not been established.
