import csv
import io
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAPPER = ROOT / "mapreduce" / "mapper.py"
REDUCER = ROOT / "mapreduce" / "reducer.py"


class MapReduceTests(unittest.TestCase):
    maxDiff = None

    def run_script(self, script_path: Path, input_text: str = "") -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(script_path)],
            input=input_text,
            text=True,
            capture_output=True,
            check=False,
        )

    def parse_csv_output(self, text: str):
        rows = list(csv.reader(io.StringIO(text)))
        return rows

    def test_mapper_basic_output(self):
        sample = (
            '{"product_name":"Laptop","units_sold":5}\n'
            '{"product_name":"Mouse","units_sold":10}\n'
            '{"product_name":"Keyboard","units_sold":7}\n'
            '{"product_name":"Laptop","units_sold":8}\n'
        )
        result = self.run_script(MAPPER, sample)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(
            result.stdout.strip().splitlines(),
            ["Laptop\t5", "Mouse\t10", "Keyboard\t7", "Laptop\t8"],
        )

    def test_reducer_aggregates_duplicate_products(self):
        mapper_output = "Laptop\t5\nMouse\t10\nKeyboard\t7\nLaptop\t8\nMouse\t3\n"
        result = self.run_script(REDUCER, mapper_output)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        rows = self.parse_csv_output(result.stdout)
        self.assertEqual(rows[0], ["rank", "product_name", "units_sold"])
        self.assertEqual(rows[1:], [["1", "Laptop", "13"], ["2", "Mouse", "13"], ["3", "Keyboard", "7"]])

    def test_top_ten_ranking(self):
        products = [
            ("P01", 9),
            ("P02", 8),
            ("P03", 7),
            ("P04", 6),
            ("P05", 5),
            ("P06", 4),
            ("P07", 3),
            ("P08", 2),
            ("P09", 1),
            ("P10", 0),
            ("P11", 11),
        ]

        mapper_output = "\n".join(f"{name}\t{qty}" for name, qty in products) + "\n"
        result = self.run_script(REDUCER, mapper_output)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        rows = self.parse_csv_output(result.stdout)
        self.assertEqual(len(rows) - 1, 10)
        top_names = [row[1] for row in rows[1:]]
        self.assertEqual(top_names, ["P11", "P01", "P02", "P03", "P04", "P05", "P06", "P07", "P08", "P09"])

    def test_tie_breaking_is_alphabetical(self):
        mapper_output = "Laptop\t13\nKeyboard\t13\nMouse\t13\nMonitor\t6\n"
        result = self.run_script(REDUCER, mapper_output)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        rows = self.parse_csv_output(result.stdout)
        self.assertEqual(
            [row[1] for row in rows[1:]],
            ["Keyboard", "Laptop", "Mouse", "Monitor"],
        )

    def test_mapper_rejects_malformed_json(self):
        result = self.run_script(MAPPER, '{"product_name": "Bad"\n')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid HDFS transaction", result.stderr)

    def test_mapper_rejects_missing_fields(self):
        result = self.run_script(MAPPER, '{"product_name":"Laptop"}\n')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid HDFS transaction", result.stderr)

    def test_mapper_rejects_negative_quantity(self):
        result = self.run_script(MAPPER, '{"product_name":"Laptop","units_sold":-1}\n')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid HDFS transaction", result.stderr)

    def test_mapper_accepts_zero_quantity(self):
        result = self.run_script(MAPPER, '{"product_name":"Laptop","units_sold":0}\n')
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(result.stdout.strip(), "Laptop\t0")

    def test_mapper_rejects_invalid_quantity_type(self):
        result = self.run_script(MAPPER, '{"product_name":"Laptop","units_sold":"abc"}\n')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid HDFS transaction", result.stderr)

    def test_reducer_invalid_input_line_fails(self):
        result = self.run_script(REDUCER, "Laptop|5\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid mapper output", result.stderr)

    def test_empty_input_works_for_mapper_and_reducer(self):
        mapper_result = self.run_script(MAPPER, "")
        self.assertEqual(mapper_result.returncode, 0)
        self.assertEqual(mapper_result.stdout, "")

        reducer_result = self.run_script(REDUCER, "")
        self.assertEqual(reducer_result.returncode, 0)
        self.assertEqual(reducer_result.stdout.splitlines()[0], "rank,product_name,units_sold")

    def test_complete_pipeline_sample_matches_expected(self):
        sample = (
            '{"product_name":"Laptop","units_sold":5}\n'
            '{"product_name":"Mouse","units_sold":10}\n'
            '{"product_name":"Keyboard","units_sold":7}\n'
            '{"product_name":"Laptop","units_sold":8}\n'
            '{"product_name":"Mouse","units_sold":3}\n'
            '{"product_name":"Monitor","units_sold":6}\n'
            '{"product_name":"Headphones","units_sold":15}\n'
            '{"product_name":"Keyboard","units_sold":6}\n'
        )

        mapper_result = self.run_script(MAPPER, sample)
        self.assertEqual(mapper_result.returncode, 0, msg=mapper_result.stderr)

        reducer_result = self.run_script(REDUCER, mapper_result.stdout)
        self.assertEqual(reducer_result.returncode, 0, msg=reducer_result.stderr)
        rows = self.parse_csv_output(reducer_result.stdout)
        expected = [
            ["rank", "product_name", "units_sold"],
            ["1", "Headphones", "15"],
            ["2", "Keyboard", "13"],
            ["3", "Laptop", "13"],
            ["4", "Mouse", "13"],
            ["5", "Monitor", "6"],
        ]
        self.assertEqual(rows, expected)

    def test_reducer_csv_format_and_order(self):
        mapper_output = "Headphones\t15\nKeyboard\t13\nLaptop\t13\nMouse\t13\nMonitor\t6\n"
        result = self.run_script(REDUCER, mapper_output)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        rows = self.parse_csv_output(result.stdout)
        self.assertEqual(rows[0], ["rank", "product_name", "units_sold"])
        self.assertEqual([row[0] for row in rows[1:]], ["1", "2", "3", "4", "5"])

    def test_reducer_is_repeatable_across_runs(self):
        mapper_output = "Laptop\t5\nKeyboard\t7\nMouse\t10\nLaptop\t8\nKeyboard\t6\n"
        first = self.run_script(REDUCER, mapper_output)
        second = self.run_script(REDUCER, mapper_output)
        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual(first.returncode, second.returncode)


if __name__ == "__main__":
    unittest.main(verbosity=2)
