import os
import json
from pyspark.sql.types import *


class Schema:
    def __init__(self, schema, constraints):
        self.schema = schema
        self.constraints = constraints


def load_json(json_file):
    """
    Load a JSON object for a given file available on DISK
    :param json_file: the absolute path of the file to load
    :return: the JSON object
    """
    if not os.path.exists(json_file):
        raise Exception("Could not find file {}".format(json_file))
    with open(json_file) as f:
        return json.loads(f.read())


class FieldGeneric:
    def __init__(self, args):
        self.field_name = args[0]
        self.field_path = args[1]
        self.is_nullable = args[2]
        self.field_properties = args[3]
        self.field_description = args[4]

    def get_type(self) -> DataType:
        field_format = self.field_properties.get('format', None)
        field_type = self.field_properties.get('type', None)
        return get_field_type(field_type, field_format)

    def get_expectations(self) -> {}:
        return validate_nullable(self.field_path, self.is_nullable)

    def get_struct_field(self) -> StructField:
        return StructField(self.field_name, self.get_type(), self.is_nullable, {'desc': self.field_description})


class FieldString(FieldGeneric):
    def __init__(self, *args):
        super().__init__(args)
        self.field_format = self.field_properties.get('format', None)

    def get_expectations(self):
        constraints = super().get_expectations()
        if self.field_format == "date" or self.field_format == 'date-time':
            constraints.update(validate_dates(self.field_path, self.field_properties))
        else:
            constraints.update(validate_strings(self.field_path, self.field_properties))
        return constraints


class FieldNumber(FieldGeneric):
    def __init__(self, *args):
        super().__init__(args)

    def get_expectations(self):
        constraints = super().get_expectations()
        constraints.update(validate_numbers(self.field_path, self.field_properties))
        return constraints


class FieldBoolean(FieldGeneric):
    def __init__(self, *args):
        super().__init__(args)

    def get_expectations(self):
        return super().get_expectations()


def get_field_type(field_type, field_format=None) -> DataType:
    """
    Retrieve the spark DataType for a given JSON field. We should have processed complex types recursively to
    their lowest hierarchy already, hence only processing Atomic types here.
    Note that exotic JSON types such as hostname, ip address, etc. will be treated as string
    :param field_type: the JSON type of the field (number, integer, etc.)
    :param field_format: the JSON format of the field (date, date-time, etc.)
    :return: the corresponding spark [pyspark.sql.types.DataType]
    """
    if field_type == 'number':
        return DoubleType()
    elif field_type == 'integer':
        return IntegerType()
    elif field_type == 'boolean':
        return BooleanType()
    elif field_type == 'string':
        if field_format == 'date':
            return DateType()
        elif field_format == 'date-time':
            return TimestampType()
        else:
            return StringType()
    else:
        raise Exception('Unsupported type {}'.format(field_type))


def get_field_description(field_properties, parent_description=None) -> str:
    """
    Retrieve the description of a JSON field. As we support supertypes and references,
    a parent description takes precedence over its children (more specific description)
    :param field_properties: the JSON object containing field properties (and possibly its description)
    :param parent_description: the description of the parent object, if any
    :return: the most specific description of a field that we can add to spark schema
    """
    field_description = field_properties.get('description', None)
    if parent_description:
        field_description = parent_description
    return field_description


def get_field_path(field_name, parent_path=None) -> str:
    """
    Since we cover supertypes and nested entity, the name of the field may need to be preceded by its
    higher level schema. Although the name of the field must be relative in a [pyspark.sql.types.StructField],
    it must be absolute in the constraints we may generate as SQL. Also, we ensure exotic column names are
    enclosed with backticks
    :param field_name: the name of the field (relative)
    :param parent_path: the higher level hierarchy (parent absolute path), None if root entity.
    :return: the absolute path of our field that can be used in SQL expression
    """
    if parent_path:
        return '{}.`{}`'.format(parent_path, field_name)
    else:
        return '`{}`'.format(field_name)


def validate_nullable(field_path, is_nullable) -> {}:
    """
    Some fields of a JSON entity may be marked as mandatory (`required` parameter).
    We define expectations testing for null.
    :param field_path: the absolute path of a field that can be used in a SQL expression
    :param is_nullable: whether this field is defined as optional in our JSON schema
    :return: name and SQL expression defining our constraint
    """
    constraints = {}
    if not is_nullable:
        nme = "[{field}] NULLABLE".format(field=field_path)
        exp = "{field} IS NOT NULL".format(field=field_path)
        constraints[nme] = exp
    return constraints


def validate_numbers(field_path, field_properties) -> {}:
    """
    We may extract additional constraints from our Numeric objects
    We define expectations testing for minimum (inclusive) / maximum (inclusive)
    :param field_path: the absolute path of a field that can be used in a SQL expression
    :param field_properties: the JSON object containing field properties
    :return: name and SQL expression defining our constraint
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
    String are of special types in the JSON world. Marked as String (hence enclosed with quotes), these might
    be of different formats (date, date-time, ipv4, hostname, etc.). This method is used for actual string type.
    For date types, please refer to [validate_dates()] method.
    For any other string types (ip, email, etc.) we may implement common regexes in future project releases
     - we define expectations testing for minimum or maximum length (indicated with JSON field as `minLength`)
     - we define expectations testing for specific regex patterns (indicated with JSON field as `pattern`)
     - we define expectations testing for specific enums (indicated with JSON field as `enums`)
    :param field_path: the absolute path of a field that can be used in a SQL expression
    :param field_properties: the JSON object containing field properties
    :return: name and SQL expression defining our constraint
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
    String are of special types in the JSON world. Marked as String (hence enclosed with quotes), these might
    be of different formats (date, date-time, ipv4, etc.). This method is used for actual date or timestamp types.
    We define expectations testing for minimum (inclusive) / maximum (inclusive) dates
    :param field_path: the absolute path of a field that can be used in a SQL expression
    :param field_properties: the JSON object containing field properties
    :return: name and SQL expression defining our constraint
    """
    constraints = {}
    minimum = field_properties.get('min', None)
    maximum = field_properties.get('max', None)
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
    We may extract additional constraints from our Array objects
    We define expectations testing for minimum (inclusive) / maximum (inclusive) array size
    Note that we could not find any way to validate consistency of values without having to explode its values
    :param field_path: the absolute path of a field that can be used in a SQL expression
    :param field_properties: the JSON object containing field properties
    :return: name and SQL expression defining our constraint
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
