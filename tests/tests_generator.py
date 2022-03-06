import os.path
import unittest
from waterbear.convertor import *
from waterbear.generator import *
from pyspark.sql import SparkSession


HOME = os.path.join(os.path.dirname(__file__))
SCHEMA_DIR = os.path.join(HOME, 'schema')
TMP_DIR = os.path.join(HOME, '..', 'dist')
DATA_FILE = os.path.join(TMP_DIR, 'test_generate_records.json')


class SparkTest(unittest.TestCase):

    def setUp(self):

        xs = JsonRecordGenerator(SCHEMA_DIR, 0.5).generate("employee", 100)
        if not os.path.exists(TMP_DIR):
            os.makedirs(TMP_DIR)
        with open(DATA_FILE, 'w') as f:
            f.write('\n'.join(xs))

        self.spark = SparkSession \
            .builder \
            .appName("UNIT_TEST") \
            .master("local") \
            .getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_schema_apply(self):
        schema, constraints = JsonSchemaConvertor(SCHEMA_DIR).convert("employee")
        df = self.spark.read.format("json").schema(schema).load(DATA_FILE)
        df.show(truncate=False)
        self.assertEqual(100, df.count())


if __name__ == '__main__':
    unittest.main()
