import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from lh4fs.schema import JsonBuilder

from . import (
    SCHEMA_DIR,
    DATA_DIR
)


def dataframe_stub(df, limit=20):
    return "\n".join(["\n".join(f"({', '.join('%r' % f for f in row)})" for row in df.limit(limit).collect())])


class SparkTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession \
            .builder \
            .appName("UNIT_TEST") \
            .master("local") \
            .getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_schema_apply(self):
        schema, constraints = JsonBuilder(SCHEMA_DIR).build("employee")
        df = self.spark.read.format("json").schema(schema).load(DATA_DIR)
        df.show()
        self.assertEqual(100, df.count())

    def test_constraints_apply(self):
        schema, constraints = JsonBuilder(SCHEMA_DIR).build("employee")
        constraint_exprs = [F.expr(c) for c in constraints.values()]
        constraint_names = [F.lit(c) for c in constraints.keys()]

        @F.udf('array<string>')
        def filter_array(xs, ys):
            return [ys[i] for i, x in enumerate(xs) if not x]

        df = self.spark.read.format("json").schema(schema).load(DATA_DIR) \
            .withColumn('databricks_expr', F.array(constraint_exprs)) \
            .withColumn('databricks_name', F.array(constraint_names)) \
            .withColumn('lh4fs', filter_array('databricks_expr', 'databricks_name')) \
            .select(F.explode('lh4fs').alias('lh4fs')) \
            .groupBy('lh4fs') \
            .count()

        df.show()
        actual = dataframe_stub(df)
        expected = "\n".join([
            "('[`high_fives`] VALUE', 1)",
            "('[`person`] NULLABLE', 1)",
            "('[`person`.`username`] MATCH', 1)",
            "('[`role`] VALUE', 1)",
            "('[`skills`] SIZE', 1)",
            "('[`id`] NULLABLE', 1)"
        ])

        self.assertEqual(expected, actual, "Constraints should be violated")


if __name__ == '__main__':
    unittest.main()
