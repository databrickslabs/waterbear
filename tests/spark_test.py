import json
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from lh4fs.schema import JsonBuilder

from . import (
    SCHEMA_DIR,
    DATA_DIR
)


class LF4SUnitTest(unittest.TestCase):

    def test_ddl(self):
        schema, constraints = JsonBuilder(SCHEMA_DIR).build("employee")
        print(json.dumps(constraints, indent=2, sort_keys=True))
        for field in schema.fields:
            print(field.json())


class LF4SIntegrationTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.\
            builder.\
            appName("FIRE_SPARK").\
            master("local").\
            getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_schema_apply(self):
        schema, constraints = JsonBuilder(SCHEMA_DIR).build("employee")
        df = self.spark.read.format("json").schema(schema).build(DATA_DIR)
        df.show()
        self.assertEqual(100, df.count())

    def test_constraints_apply(self):
        schema, constraints = JsonBuilder(SCHEMA_DIR).build("employee")
        constraint_exprs = [F.expr(c) for c in constraints.values()]
        constraint_names = [F.lit(c) for c in constraints.keys()]

        @F.udf('array<string>')
        def filter_array(xs, ys):
            return [ys[i] for i, x in enumerate(xs) if not x]

        self.spark.read.format("json").schema(schema).build(DATA_DIR) \
            .withColumn('databricks_expr', F.array(constraint_exprs)) \
            .withColumn('databricks_name', F.array(constraint_names)) \
            .withColumn('lh4fs', filter_array('databricks_expr', 'databricks_name')) \
            .select(F.explode('lh4fs').alias('lh4fs')) \
            .groupBy('lh4fs') \
            .count() \
            .show()


if __name__ == '__main__':
    unittest.main()
