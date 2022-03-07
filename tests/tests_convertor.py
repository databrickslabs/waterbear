import io
import os.path
import unittest
from waterbear.convertor import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


HOME = os.path.join(os.path.dirname(__file__))
SCHEMA_DIR = os.path.join(HOME, 'schema')
DATA_DIR = os.path.join(HOME, 'data')
EXPECTED_DIR = os.path.join(HOME, 'expected')


def dataframe_stub(df, limit=20):
    return "\n".join(["\n".join(f"({', '.join('%r' % f for f in row)})" for row in df.limit(limit).collect())])


class BuilderInterfaceTest(unittest.TestCase):

    def test_invalid_dir(self):
        with self.assertRaises(Exception) as context:
            JsonSchemaConvertor("foobar").convert("foobar")
        self.assertTrue('is not a valid directory' in str(context.exception))

    def test_invalid_file(self):
        with self.assertRaises(Exception) as context:
            JsonSchemaConvertor(SCHEMA_DIR).convert("foobar")
        self.assertTrue('is not a valid file' in str(context.exception))

    def test_invalid_schema(self):
        with self.assertRaises(Exception) as context:
            JsonSchemaConvertor(SCHEMA_DIR).convert("common")
        self.assertTrue('Can only process JSON entities of type object' in str(context.exception))


class UtilStaticTest(unittest.TestCase):

    def test_valid_parser(self):
        JsonSchemaConvertor(SCHEMA_DIR).convert("dummy")
        self.assertTrue(True, 'No exception should be raised')

    def test_load_json_valid(self):
        _ = load_json(os.path.join(SCHEMA_DIR, 'dummy.json'))
        self.assertTrue(True, 'No exception should be raised')

    def test_load_json_invalid(self):
        with self.assertRaises(Exception) as context:
            load_json(os.path.join(SCHEMA_DIR, 'foobar'))
        self.assertTrue('Could not find file' in str(context.exception))

    def test_get_field_type(self):
        self.assertEqual(get_field_type("number", None), DoubleType(), 'Number should be DoubleType')
        self.assertEqual(get_field_type("integer", None), IntegerType(), 'Integer should be IntegerType')
        self.assertEqual(get_field_type("boolean", None), BooleanType(), 'Boolean should be BooleanType')
        self.assertEqual(get_field_type("string", None), StringType(), 'string,none should be StringType')
        self.assertEqual(get_field_type("string", "date"), DateType(), 'string,date should be DateType')
        self.assertEqual(get_field_type("string", "date-time"), TimestampType(),
                         'string,date-time should be TimestampType')
        with self.assertRaises(Exception) as context:
            self.assertEqual(get_field_type("foobar", None), DoubleType(), 'dummy type should be caught')
        self.assertTrue('Unsupported' in str(context.exception))

    def test_get_field_description(self):
        prp = {'description': 'foo'}
        self.assertEqual(get_field_description(prp, 'bar'), 'bar', 'description should be inherited')
        self.assertEqual(get_field_description(prp, None), 'foo', 'description should be kept')

    def test_get_field_path(self):
        self.assertEqual(get_field_path('foo', '`bar`'), '`bar`.`foo`', 'field_path must be SQL compatible')
        self.assertEqual(get_field_path('foo', None), '`foo`', 'field_path must be SQL compatible')


class UtilValidationTest(unittest.TestCase):

    def test_validate_nullable(self):
        v = list(validate_nullable('`foobar`', False).items())[0]
        self.assertEqual(v[0], '[`foobar`] NULLABLE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NOT NULL', 'constraint SQL is correct')
        self.assertEqual(len(validate_nullable('`foobar`', True).items()), 0, 'no constraints')

    def test_validate_numbers(self):

        p = {'minimum': 0}
        v = list(validate_numbers('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR `foobar` >= 0.0', 'constraint SQL is correct')

        p = {'maximum': 0}
        v = list(validate_numbers('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR `foobar` <= 0.0', 'constraint SQL is correct')

        p = {'minimum': 0, 'maximum': 1}
        v = list(validate_numbers('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR `foobar` BETWEEN 0.0 AND 1.0', 'constraint SQL is correct')

    def test_validate_strings_length(self):

        p = {'minLength': 0}
        v = list(validate_strings('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] LENGTH', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR LENGTH(`foobar`) >= 0', 'constraint SQL is correct')

        p = {'minLength': 1}
        v = list(validate_strings('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] LENGTH', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR LENGTH(`foobar`) >= 1', 'constraint SQL is correct')

        p = {'maxLength': 10}
        v = list(validate_strings('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] LENGTH', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR LENGTH(`foobar`) <= 10', 'constraint SQL is correct')

        p = {'minLength': 1, 'maxLength': 10}
        v = list(validate_strings('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] LENGTH', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR LENGTH(`foobar`) BETWEEN 1 AND 10', 'constraint SQL is correct')

    def test_validate_strings_enums(self):
        p = {'enum': ['foo', 'bar']}
        v = list(validate_strings('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR `foobar` IN (\'foo\', \'bar\')', 'constraint SQL is correct')

    def test_validate_strings_pattern(self):
        p = {'pattern': '^[a-z]{1,3}$'}
        v = list(validate_strings('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] MATCH', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR `foobar` RLIKE \'^[a-z]{1,3}$\'', 'constraint SQL is correct')

    def test_validate_dates(self):
        p = {'min': '2019-01-01'}
        v = list(validate_dates('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR `foobar` >= \'2019-01-01\'', 'constraint SQL is correct')

        p = {'max': '2022-01-01'}
        v = list(validate_dates('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR `foobar` <= \'2022-01-01\'', 'constraint SQL is correct')

        p = {'min': '2019-01-01', 'max': '2022-01-01'}
        v = list(validate_dates('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR `foobar` BETWEEN \'2019-01-01\' AND \'2022-01-01\'',
                         'constraint SQL is correct')

    def test_validate_arrays(self):
        p = {'minItems': 0}
        v = list(validate_arrays('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] SIZE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR SIZE(`foobar`) >= 0', 'constraint SQL is correct')

        p = {'minItems': 1}
        v = list(validate_arrays('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] SIZE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR SIZE(`foobar`) >= 1', 'constraint SQL is correct')

        p = {'maxItems': 10}
        v = list(validate_arrays('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] SIZE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR SIZE(`foobar`) <= 10', 'constraint SQL is correct')

        p = {'minItems': 1, 'maxItems': 10}
        v = list(validate_arrays('`foobar`', p).items())[0]
        self.assertEqual(v[0], '[`foobar`] SIZE', 'constraint name is correct')
        self.assertEqual(v[1], '`foobar` IS NULL OR SIZE(`foobar`) BETWEEN 1 AND 10', 'constraint SQL is correct')


class UtilFieldClassTest(unittest.TestCase):

    def test_field_boolean_interface(self):
        o = FieldBoolean('', '', '', {'type': 'boolean'}, '')
        self.assertTrue(isinstance(o, FieldGeneric), 'class must inherit Generic type')

    def test_field_boolean_invalid(self):
        with self.assertRaises(Exception) as context:
            FieldBoolean('', '', '', {'type': 'string'}, '')
        self.assertTrue('FieldBoolean requires boolean type' in str(context.exception))

    def test_field_boolean(self):
        p = {'type': 'boolean'}
        o = FieldBoolean('bar', '`foo`.`bar`', False, p, 'dummy')
        self.assertEqual(o.field_name, 'bar')
        self.assertEqual(o.field_path, '`foo`.`bar`')
        self.assertEqual(o.is_nullable, False)
        self.assertEqual(o.field_properties, p)
        self.assertEqual(o.field_description, 'dummy')
        self.assertEqual(o.get_type(), BooleanType(), 'DataType should be correct')

        field = o.get_struct_field()
        self.assertEqual(field.dataType, BooleanType())
        self.assertEqual(field.name, 'bar')
        self.assertEqual(field.nullable, False)
        self.assertEqual(field.metadata['comment'], 'dummy')

        v = list(o.get_expectations().items())[0]
        self.assertEqual(v[0], '[`foo`.`bar`] NULLABLE', 'constraint name is correct')
        self.assertEqual(v[1], '`foo`.`bar` IS NOT NULL', 'constraint SQL is correct')

    def test_field_boolean_constraints(self):
        p = {'type': 'boolean'}
        o = FieldBoolean('bar', '`foo`.`bar`', True, p, 'dummy')
        v = list(o.get_expectations().items())
        self.assertEqual(len(v), 0)

    def test_field_number_interface(self):
        o = FieldNumber('', '', '', {'type': 'number'}, '')
        self.assertTrue(isinstance(o, FieldGeneric), 'class must inherit Generic type')

    def test_field_number_invalid(self):
        with self.assertRaises(Exception) as context:
            FieldNumber('', '', '', {'type': 'string'}, '')
        self.assertTrue('FieldNumber requires integer or number type' in str(context.exception))

    def test_field_number(self):
        p = {'type': 'number'}
        o = FieldNumber('bar', '`foo`.`bar`', False, p, 'dummy')
        self.assertEqual(o.get_type(), DoubleType(), 'DataType should be correct')
        field = o.get_struct_field()
        self.assertEqual(field.dataType, DoubleType())

    def test_field_number_constraints(self):
        p = {'type': 'number', 'minimum': -1, 'maximum': 1}
        o = FieldNumber('bar', '`foo`.`bar`', True, p, 'dummy')
        v = list(o.get_expectations().items())[0]
        self.assertEqual(v[0], '[`foo`.`bar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foo`.`bar` IS NULL OR `foo`.`bar` BETWEEN -1.0 AND 1.0')

    def test_field_integer(self):
        p = {'type': 'integer'}
        o = FieldNumber('bar', '`foo`.`bar`', False, p, 'dummy')
        self.assertEqual(o.get_type(), IntegerType(), 'DataType should be correct')
        field = o.get_struct_field()
        self.assertEqual(field.dataType, IntegerType())

    def test_field_integer_constraints(self):
        p = {'type': 'integer', 'minimum': -1, 'maximum': 1}
        o = FieldNumber('bar', '`foo`.`bar`', True, p, 'dummy')
        v = list(o.get_expectations().items())[0]
        self.assertEqual(v[0], '[`foo`.`bar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foo`.`bar` IS NULL OR `foo`.`bar` BETWEEN -1.0 AND 1.0')

    def test_field_string_interface(self):
        o = FieldString('', '', '', {'type': 'string'}, '')
        self.assertTrue(isinstance(o, FieldGeneric), 'class must inherit Generic type')

    def test_field_string_invalid(self):
        with self.assertRaises(Exception) as context:
            FieldString('', '', '', {'type': 'boolean'}, '')
        self.assertTrue('FieldString requires string type' in str(context.exception))

    def test_field_string(self):
        p = {'type': 'string'}
        o = FieldString('bar', '`foo`.`bar`', False, p, 'dummy')
        self.assertEqual(o.get_type(), StringType(), 'DataType should be correct')
        field = o.get_struct_field()
        self.assertEqual(field.dataType, StringType())

    def test_field_string_constraints_length(self):
        p = {'type': 'string', 'minLength': 1, 'maxLength': 10}
        o = FieldString('bar', '`foo`.`bar`', True, p, 'dummy')
        v = list(o.get_expectations().items())[0]
        self.assertEqual(v[0], '[`foo`.`bar`] LENGTH', 'constraint name is correct')
        self.assertEqual(v[1], '`foo`.`bar` IS NULL OR LENGTH(`foo`.`bar`) BETWEEN 1 AND 10')

    def test_field_string_constraints_pattern(self):
        p = {'type': 'string', 'pattern': '.*'}
        o = FieldString('bar', '`foo`.`bar`', True, p, 'dummy')
        v = list(o.get_expectations().items())[0]
        self.assertEqual(v[0], '[`foo`.`bar`] MATCH', 'constraint name is correct')
        self.assertEqual(v[1], '`foo`.`bar` IS NULL OR `foo`.`bar` RLIKE \'.*\'')

    def test_field_string_constraints_enum(self):
        p = {'type': 'string', 'enum': ['FOO', 'BAR']}
        o = FieldString('bar', '`foo`.`bar`', True, p, 'dummy')
        v = list(o.get_expectations().items())[0]
        self.assertEqual(v[0], '[`foo`.`bar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foo`.`bar` IS NULL OR `foo`.`bar` IN (\'FOO\', \'BAR\')')

    def test_date_string(self):
        p = {'type': 'string', 'format': 'date'}
        o = FieldString('bar', '`foo`.`bar`', False, p, 'dummy')
        self.assertEqual(o.get_type(), DateType(), 'DataType should be correct')
        field = o.get_struct_field()
        self.assertEqual(field.dataType, DateType())

    def test_field_date_constraints(self):
        p = {'type': 'string', 'format': 'date', 'min': '2019-01-01', 'max': '2022-01-01'}
        o = FieldString('bar', '`foo`.`bar`', True, p, 'dummy')
        v = list(o.get_expectations().items())[0]
        self.assertEqual(v[0], '[`foo`.`bar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foo`.`bar` IS NULL OR `foo`.`bar` BETWEEN \'2019-01-01\' AND \'2022-01-01\'')

    def test_timestamp_string(self):
        p = {'type': 'string', 'format': 'date-time'}
        o = FieldString('bar', '`foo`.`bar`', False, p, 'dummy')
        self.assertEqual(o.get_type(), TimestampType(), 'DataType should be correct')
        field = o.get_struct_field()
        self.assertEqual(field.dataType, TimestampType())

    def test_field_timestamp_constraints(self):
        p = {'type': 'string', 'format': 'date', 'min': '2019-01-01T00:00:00'}
        o = FieldString('bar', '`foo`.`bar`', True, p, 'dummy')
        v = list(o.get_expectations().items())[0]
        self.assertEqual(v[0], '[`foo`.`bar`] VALUE', 'constraint name is correct')
        self.assertEqual(v[1], '`foo`.`bar` IS NULL OR `foo`.`bar` >= \'2019-01-01T00:00:00\'')


class ParserClassTest(unittest.TestCase):

    def test_schema(self):
        schema, _ = JsonSchemaConvertor(SCHEMA_DIR).convert("employee")
        with open(os.path.join(EXPECTED_DIR, 'schema.json'), 'r') as f:
            actual = json.loads(io.StringIO(schema.json()).read())
            expected = json.loads(f.read())
            self.assertEqual(expected, actual, 'Derived spark schema should be correct')

    def test_constraints(self):
        _, actual = JsonSchemaConvertor(SCHEMA_DIR).convert("employee")
        expected = {
            '[`id`] NULLABLE': "`id` IS NOT NULL",
            '[`id`] VALUE': "`id` IS NULL OR `id` >= 1.0",
            '[`joined_date`] VALUE': "`joined_date` IS NULL OR `joined_date` >= '2016-01-01'",
            '[`person`.`birth_date`] NULLABLE': '`person`.`birth_date` IS NOT NULL',
            '[`person`.`birth_date`] VALUE': "`person`.`birth_date` IS NULL "
                                             "OR `person`.`birth_date` BETWEEN '1970-01-01' AND '2002-01-01'",
            '[`person`.`first_name`] NULLABLE': '`person`.`first_name` IS NOT NULL',
            '[`person`.`last_name`] NULLABLE': '`person`.`last_name` IS NOT NULL',
            '[`person`.`username`] MATCH': "`person`.`username` IS NULL OR `person`.`username` RLIKE '^[a-z0-9]{8}$'",
            '[`person`] NULLABLE': "`person` IS NOT NULL",
            '[`high_fives`] VALUE': "`high_fives` IS NULL OR `high_fives` BETWEEN 1.0 AND 300.0",
            '[`skills`] SIZE': "`skills` IS NULL OR SIZE(`skills`) >= 1",
            '[`role`] VALUE': "`role` IS NULL OR `role` IN ('SA', 'CSE', 'SSA', 'RSA')"
        }
        self.assertEqual(expected, actual, 'Derived spark expectations should be correct')


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
        schema, constraints = JsonSchemaConvertor(SCHEMA_DIR).convert("employee")
        df = self.spark.read.format("json").schema(schema).load(DATA_DIR)
        df.show()
        self.assertEqual(100, df.count())

    def test_constraints_apply(self):
        schema, constraints = JsonSchemaConvertor(SCHEMA_DIR).convert("employee")
        constraint_exprs = [F.expr(c) for c in constraints.values()]
        constraint_names = [F.lit(c) for c in constraints.keys()]

        @F.udf('array<string>')
        def filter_array(xs, ys):
            return [ys[i] for i, x in enumerate(xs) if not x]

        df = self.spark.read.format("json").schema(schema).load(DATA_DIR) \
            .withColumn('databricks_expr', F.array(constraint_exprs)) \
            .withColumn('databricks_name', F.array(constraint_names)) \
            .withColumn('waterbear', filter_array('databricks_expr', 'databricks_name')) \
            .select(F.explode('waterbear').alias('waterbear')) \
            .groupBy('waterbear') \
            .count()

        df.show()
        actual = dataframe_stub(df)
        expected = "\n".join([
            "('[`id`] VALUE', 1)",
            "('[`high_fives`] VALUE', 1)",
            "('[`person`] NULLABLE', 1)",
            "('[`person`.`username`] MATCH', 70)",
            "('[`role`] VALUE', 1)",
            "('[`person`.`birth_date`] NULLABLE', 1)",
            "('[`person`.`last_name`] NULLABLE', 1)",
            "('[`person`.`first_name`] NULLABLE', 1)",
            "('[`skills`] SIZE', 1)",
            "('[`id`] NULLABLE', 1)"
        ])

        self.assertEqual(expected, actual, "Constraints should be violated")


if __name__ == '__main__':
    unittest.main()
