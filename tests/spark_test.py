import io
import json
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from databricks.lh4fs import L4FSModel

from . import (
    SCHEMA_DIR,
    DATA_DIR
)


class LF4SUnitTest(unittest.TestCase):

    def test_ddl(self):
        schema, constraints = L4FSModel(SCHEMA_DIR).load("employee")
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
        schema, constraints = L4FSModel(SCHEMA_DIR).load("employee")
        df = self.spark.read.format("json").schema(schema).load(DATA_DIR)
        df.show()
        self.assertEqual(100, df.count())


    def test_constraints_apply(self):
        schema, constraints = L4FSModel(SCHEMA_DIR).load("employee")
        constraint_exprs = [F.expr(c) for c in constraints.values()]
        constraint_names = [F.lit(c) for c in constraints.keys()]

        @F.udf('array<string>')
        def filter_array(xs, ys):
            return [ys[i] for i, x in enumerate(xs) if not x]

        self.spark.read.format("json").schema(schema).load(DATA_DIR) \
            .withColumn('databricks_expr', F.array(constraint_exprs)) \
            .withColumn('databricks_name', F.array(constraint_names)) \
            .withColumn('databricks', filter_array('databricks_expr', 'databricks_name')) \
            .select(F.explode('databricks').alias('databricks')) \
            .groupBy('databricks') \
            .count() \
            .show()


if __name__ == '__main__':
    unittest.main()
