import io
import json
import os.path
import unittest
from lh4fs.schema import JsonBuilder

from . import (
    SCHEMA_DIR,
    EXPECTED_DIR
)


class IOTest(unittest.TestCase):

    def test_invalid_file(self):
        with self.assertRaises(Exception) as context:
            JsonBuilder(SCHEMA_DIR).build("foobar")
        self.assertTrue('is not a valid file' in str(context.exception))

    def test_invalid_schema(self):
        with self.assertRaises(Exception) as context:
            JsonBuilder(SCHEMA_DIR).build("common")
        self.assertTrue('Can only process JSON entities of type object' in str(context.exception))


class ClassTest(unittest.TestCase):

    def test_schema(self):
        schema, _ = JsonBuilder(SCHEMA_DIR).build("employee")
        for field in schema.fields:
            print(field)
        with open(os.path.join(EXPECTED_DIR, 'schema.json'), 'r') as f:
            actual = json.loads(io.StringIO(schema.json()).read())
            expected = json.loads(f.read())
            self.assertEqual(expected, actual, 'Derived spark schema should be correct')

    def test_constraints(self):
        _, actual = JsonBuilder(SCHEMA_DIR).build("employee")
        expected = {
            '[`id`] NULLABLE': '`id` IS NOT NULL',
            '[`person`.`username`] MATCH': "`person`.`username` IS NULL OR `person`.`username` RLIKE '^[a-z0-9]{2,}$'",
            '[`person`] NULLABLE': '`person` IS NOT NULL',
            '[`high_fives`] VALUE': '`high_fives` IS NULL OR `high_fives` BETWEEN 1.0 AND 300.0',
            '[`skills`] SIZE': '`skills` IS NULL OR SIZE(`skills`) >= 1.0',
            '[`role`] VALUE': "`role` IS NULL OR `role` IN ('SA','CSE','SSA','RSA')"
        }
        print(json.dumps(actual, indent=2))
        self.assertEqual(expected, actual, 'Derived spark expectations should be correct')


if __name__ == '__main__':
    unittest.main()
