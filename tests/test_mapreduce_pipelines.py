import io
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WC_MAPPER = ROOT / "python_mapreduce" / "word_count" / "mapper.py"
WC_REDUCER = ROOT / "python_mapreduce" / "word_count" / "reducer.py"
SALES_MAPPER = ROOT / "python_mapreduce" / "sales_analytics" / "mapper_sales.py"
SALES_REDUCER = ROOT / "python_mapreduce" / "sales_analytics" / "reducer_sales.py"
PURCHASES_SAMPLE = ROOT / "python_mapreduce" / "sales_analytics" / "purchases_sample.txt"
ANAGRAM_MAPPER = ROOT / "python_mapreduce" / "advanced_aggregations" / "mapper_5.py"
ANAGRAM_REDUCER = ROOT / "python_mapreduce" / "advanced_aggregations" / "reducer_5.py"
EVENT_MAPPER = ROOT / "python_mapreduce" / "advanced_aggregations" / "mapper_4.py"
EVENT_REDUCER = ROOT / "python_mapreduce" / "advanced_aggregations" / "reducer_4.py"


class TestBigDataMapReducePipelines(unittest.TestCase):
    def test_wordcount_mapper_output(self):
        input_data = "Distributed computing with Hadoop\nMapReduce streaming with Python"
        proc = subprocess.run(
            [sys.executable, str(WC_MAPPER)],
            input=input_data,
            capture_output=True,
            text=True,
            check=True,
        )
        lines = [line.strip() for line in proc.stdout.strip().splitlines()]
        self.assertIn("Hadoop\t1", lines)
        self.assertIn("streaming\t1", lines)
        self.assertEqual(len(lines), 8)

    def test_wordcount_reducer_aggregation(self):
        # Reducer expects sorted key-value pairs
        sorted_input = "Hadoop\t1\nHadoop\t1\nPython\t1\nstreaming\t1\n"
        proc = subprocess.run(
            [sys.executable, str(WC_REDUCER)],
            input=sorted_input,
            capture_output=True,
            text=True,
            check=True,
        )
        output = dict(line.split("\t") for line in proc.stdout.strip().splitlines())
        self.assertEqual(int(output["Hadoop"]), 2)
        self.assertEqual(int(output["Python"]), 1)
        self.assertEqual(int(output["streaming"]), 1)

    def test_sales_mapper_schema_parsing(self):
        sample_line = "2012-01-01\t09:00\tSan Jose\tMen's Clothing\t214.05\tAmex\n"
        proc = subprocess.run(
            [sys.executable, str(SALES_MAPPER)],
            input=sample_line,
            capture_output=True,
            text=True,
            check=True,
        )
        output = proc.stdout.strip()
        self.assertEqual(output, "San Jose\t214.05")

    def test_sales_reducer_store_totals(self):
        sorted_sales = (
            "Austin\t100.50\n"
            "Austin\t200.25\n"
            "New York\t50.00\n"
            "New York\t150.00\n"
        )
        proc = subprocess.run(
            [sys.executable, str(SALES_REDUCER)],
            input=sorted_sales,
            capture_output=True,
            text=True,
            check=True,
        )
        results = dict(line.split("\t") for line in proc.stdout.strip().splitlines())
        self.assertAlmostEqual(float(results["Austin"]), 300.75, places=2)
        self.assertAlmostEqual(float(results["New York"]), 200.00, places=2)

    def test_end_to_end_sample_sales_pipeline(self):
        if not PURCHASES_SAMPLE.exists():
            self.skipTest("purchases_sample.txt not found")

        with open(PURCHASES_SAMPLE, "r", encoding="utf-8") as f:
            raw_data = f.read()

        # Step 1: Map
        map_proc = subprocess.run(
            [sys.executable, str(SALES_MAPPER)],
            input=raw_data,
            capture_output=True,
            text=True,
            check=True,
        )

        # Step 2: Shuffle / Sort
        mapped_lines = sorted(map_proc.stdout.strip().splitlines())
        sorted_stream = "\n".join(mapped_lines) + "\n"

        # Step 3: Reduce
        red_proc = subprocess.run(
            [sys.executable, str(SALES_REDUCER)],
            input=sorted_stream,
            capture_output=True,
            text=True,
            check=True,
        )

        aggregated = dict(line.split("\t") for line in red_proc.stdout.strip().splitlines())
        self.assertGreater(len(aggregated), 0)
        # Check that numbers are valid positive floats
        for store, total in aggregated.items():
            self.assertGreater(float(total), 0.0)

    def test_anagram_mapreduce_pipeline(self):
        input_words = "pots tops stop spot opt top pot\n"
        # Step 1: Map
        map_proc = subprocess.run(
            [sys.executable, str(ANAGRAM_MAPPER)],
            input=input_words,
            capture_output=True,
            text=True,
            check=True,
        )

        # Step 2: Shuffle / Sort
        mapped_lines = sorted(map_proc.stdout.strip().splitlines())
        sorted_stream = "\n".join(mapped_lines) + "\n"

        # Step 3: Reduce
        red_proc = subprocess.run(
            [sys.executable, str(ANAGRAM_REDUCER)],
            input=sorted_stream,
            capture_output=True,
            text=True,
            check=True,
        )

        lines = [line.strip() for line in red_proc.stdout.strip().splitlines()]
        opst_group = [l for l in lines if l.startswith("opst\t")]
        self.assertEqual(len(opst_group), 1)
        self.assertIn("pots", opst_group[0])
        self.assertIn("tops", opst_group[0])
        self.assertIn("stop", opst_group[0])
        self.assertIn("spot", opst_group[0])

    def test_event_date_filtering_pipeline(self):
        sample_csv = (
            '"Algeria","2023-05-10","ID01","Regains territory"\n'
            '"Morocco","2023-06-15","ID02","Peace treaty"\n'
        )
        map_proc = subprocess.run(
            [sys.executable, str(EVENT_MAPPER)],
            input=sample_csv,
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertIn("Algeria\t2023-05-10", map_proc.stdout)
        self.assertNotIn("Morocco", map_proc.stdout)


if __name__ == "__main__":
    unittest.main()
