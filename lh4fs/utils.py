import os
import json
from pyspark.sql.types import *


class LH4FSAtomic:
    def __init__(self, field_name, field_path, is_nullable, field_properties, field_description):
        self.field_name = field_name
        self.field_path = field_path
        self.field_properties = field_properties
        self.is_nullable = is_nullable
        self.field_description = field_description


class LH4FSString():
    def __init__(self, field_name, field_path, is_nullable, field_properties, field_description):
        self.field_name = field_name
        self.field_path = field_path
        self.field_description = field_description
        self.field_properties = field_properties
        self.is_nullable = is_nullable
        self.field_format = field_properties.get('format', None)

    def type(self):
        if self.field_format == "date":
            return DateType()
        elif self.field_format == 'date-time':
            return TimestampType()
        else:
            return StringType()

    def convert(self):
        return StructField(self.field_name, self.type(), self.is_nullable, {'desc': self.field_description})

    def validate(self):
        field_format = self.field_properties.get('format', None)
        if field_format == "date" or field_format == 'date-time':
            return validate_dates(self.field_path, self.field_properties)
        else:
            return validate_strings(self.field_path, self.field_properties)


class LH4FSNumber:
    def __init__(self, field_name, field_path, is_nullable, field_properties, field_description):
        self.field_name = field_name
        self.field_path = field_path
        self.is_nullable = is_nullable
        self.field_properties = field_properties
        self.field_description = field_description

    def type(self):
        return DoubleType()

    def validate(self):
        return validate_numbers(self.field_path, self.field_properties)

    def convert(self):
        return StructField(self.field_name, self.type(), self.is_nullable, {'desc': self.field_description})


class LH4FSInteger:
    def __init__(self, field_name, field_path, is_nullable, field_properties, field_description):
        self.field_name = field_name
        self.field_path = field_path
        self.is_nullable = is_nullable
        self.field_properties = field_properties
        self.field_description = field_description

    def type(self):
        return IntegerType()

    def validate(self):
        return validate_numbers(self.field_path, self.field_properties)

    def convert(self):
        return StructField(self.field_name, self.type(), self.is_nullable, {'desc': self.field_description})


class LH4FSBoolean:
    def __init__(self, field_name, field_path, is_nullable, field_properties, field_description):
        self.field_name = field_name
        self.is_nullable = is_nullable
        self.field_path = field_path
        self.field_description = field_description

    def type(self):
        return BooleanType()

    def validate(self):
        return {}

    def convert(self):
        return StructField(self.field_name, self.type(), self.is_nullable, {'desc': self.field_description})


class LH4FSEntity:
    def __init__(self, schema, constraints):
        self.schema = schema
        self.constraints = constraints


def build_field_desc(field_properties, parent_description):
    field_description = field_properties.get('description', None)
    if parent_description:
        field_description = parent_description
    return field_description


def build_field_path(field_name, parent_path):
    if parent_path:
        return '{}.`{}`'.format(parent_path, field_name)
    else:
        return '`{}`'.format(field_name)


def load_json(json_file):
    """

    :param json_file:
    :return:
    """
    if not os.path.exists(json_file):
        raise Exception("Could not find file {}".format(json_file))
    with open(json_file) as f:
        return json.loads(f.read())


def validate_nullable(field_path, is_nullable):
    """
    As part of a JSON model, some fields may be marked as mandatory. We leverage that info to define expectations
    testing for null. These constraints will be evaluated as SQL expressions that can be used within DLT as-is
    Each generated constraint will have a name and an expression as a form of dictionary
    Note that we cannot validate the consistency of values within an array without complex UDF
    :param field_path:
    :param is_nullable:
    :return:
    """
    constraints = {}
    if not is_nullable:
        nme = "[{field}] NULLABLE".format(field=field_path)
        exp = "{field} IS NOT NULL".format(field=field_path)
        constraints[nme] = exp
    return constraints


def validate_numbers(field_path, field_properties):
    """
    We may extract additional constraints from our Numeric objects, such as minimum or maximum
    These constraints will be evaluated as SQL expressions that can be used within Delta Live Tables as-is
    Each generated constraint will have a name and an expression as a form of dictionary
    :param field_path:
    :param field_properties:
    :return:
    """
    constraints = {}
    minimum = field_properties.get('minimum', None)
    maximum = field_properties.get('maximum', None)
    nme = "[{field}] VALUE".format(field=field_path)
    if minimum and maximum:
        exp = "{field} IS NULL OR {field} BETWEEN {minimum} AND {maximum}".format(
            field=field_path,
            minimum=float(minimum),
            maximum=float(maximum)
        )
        constraints[nme] = exp
    elif minimum:
        exp = "{field} IS NULL OR {field} >= {minimum}".format(field=field_path, minimum=float(minimum))
        constraints[nme] = exp
    elif maximum:
        exp = "{field} IS NULL OR {field} <= {maximum}".format(field=field_path, maximum=float(maximum))
        constraints[nme] = exp
    return constraints


def validate_strings(field_path, field_properties):
    """
    We may extract additional constraints from our String objects, such as minimum or maximum length, or even regexes
    These constraints will be evaluated as SQL expressions that can be used within Delta Live Tables as-is
    Each generated constraint will have a name and an expression as a form of dictionary
    :param field_path:
    :param field_properties:
    :return:
    """
    constraints = {}
    minimum = field_properties.get('minLength', None)
    maximum = field_properties.get('maxLength', None)
    pattern = field_properties.get('pattern', None)
    enum = field_properties.get('enum', None)

    if enum:
        nme = "[{field}] VALUE".format(field=field_path)
        enums = ','.join(["'{}'".format(e) for e in enum])
        exp = "{field} IS NULL OR {field} IN ({enums})".format(field=field_path, enums=enums)
        constraints[nme] = exp

    if pattern:
        nme = "[{field}] MATCH".format(field=field_path)
        # regexes would certainly get curly brackets that breaks our string formatting
        exp = "{field} IS NULL OR {field} RLIKE '{pattern}'".format(field=field_path, pattern=pattern)
        constraints[nme] = exp

    nme = "[{field}] LENGTH".format(field=field_path)
    if minimum and maximum:
        exp = "{field} IS NULL OR LENGTH({field}) BETWEEN {minimum} AND {maximum}".format(
            field=field_path,
            minimum=int(minimum),
            maximum=int(maximum)
        )
        constraints[nme] = exp
    elif minimum:
        exp = "{field} IS NULL OR LENGTH({field}) >= {minimum}".format(field=field_path, minimum=int(minimum))
        constraints[nme] = exp
    elif maximum:
        exp = "{field} IS NULL OR LENGTH({field}) <= {maximum}".format(field=field_path, maximum=int(maximum))
        constraints[nme] = exp

    return constraints


def validate_dates(field_path, field_properties):
    """
    We may extract additional constraints from our Date or Timestamp objects, such as minimum or maximum
    These constraints will be evaluated as SQL expressions that can be used within Delta Live Tables as-is
    Each generated constraint will have a name and an expression as a form of dictionary
    :param field_path:
    :param field_properties:
    :return:
    """
    constraints = {}
    minimum = field_properties.get('minimum', None)
    maximum = field_properties.get('maximum', None)
    nme = "[{field}] VALUE".format(field=field_path)
    if minimum and maximum:
        exp = "{field} IS NULL OR {field} BETWEEN '{minimum}' AND '{maximum}'".format(
            field=field_path,
            minimum=str(minimum),
            maximum=str(maximum)
        )
        constraints[nme] = exp
    elif minimum:
        exp = "{field} IS NULL OR {field} >= '{minimum}'".format(field=field_path, minimum=str(minimum))
        constraints[nme] = exp
    elif maximum:
        exp = "{field} IS NULL OR {field} <= '{maximum}'".format(field=field_path, maximum=str(maximum))
        constraints[nme] = exp
    return constraints


def validate_arrays(field_path, field_properties):
    """
    We may extract additional constraints from our Array objects, such as minimum or maximum number of items
    These constraints will be evaluated as SQL expressions that can be used within Delta Live Tables as-is
    Each generated constraint will have a name and an expression as a form of dictionary
    Note that we cannot validate the consistency of values within an array without complex UDF
    :param field_path:
    :param field_properties:
    :return:
    """

    constraints = {}
    # we cannot validate the integrity of each field
    # without exploding array or running complex UDFs
    # we simply check for array size for now
    minimum = field_properties.get('minItems', None)
    maximum = field_properties.get('maxItems', None)
    nme = "[{field}] SIZE".format(field=field_path)
    if minimum and maximum:
        exp = "{field} IS NULL OR SIZE({field}) BETWEEN {minimum} AND {maximum}".format(
            field=field_path,
            minimum=float(minimum),
            maximum=float(maximum)
        )

        constraints[nme] = exp
    elif minimum:
        exp = "{field} IS NULL OR SIZE({field}) >= {minimum}".format(field=field_path, minimum=float(minimum))
        constraints[nme] = exp
    elif maximum:
        exp = "{field} IS NULL OR SIZE({field}) <= {maximum}".format(field=field_path, maximum=float(maximum))
        constraints[nme] = exp
    return constraints
