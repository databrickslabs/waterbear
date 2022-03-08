import os
import json
import uuid

from pyspark.sql.types import *
import random
import datetime
import string


class Schema:
    def __init__(self, schema, constraints):
        self.schema = schema
        self.constraints = constraints


def generate_integer(minimum, maximum):
    if not minimum and minimum != 0:
        minimum = 0
    if not maximum and maximum != 0:
        maximum = 9999
    return random.randint(minimum, maximum)


def generate_number(minimum, maximum):
    if not minimum and minimum != 0:
        minimum = 0
    if not maximum and maximum != 0:
        maximum = 1
    return random.uniform(minimum, maximum)


def generate_date(minimum, maximum):
    if not minimum:
        minimum = datetime.datetime(1970, 1, 1)
    if not maximum:
        maximum = datetime.datetime(2025, 1, 1)
    time_between_dates = maximum - minimum
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    r = minimum + datetime.timedelta(days=random_number_of_days)
    return r.strftime("%Y-%m-%d")


def generate_timestamp(minimum, maximum):
    if not minimum:
        minimum = datetime.datetime(1970, 1, 1, 0, 0, 0)
    if not maximum:
        maximum = datetime.datetime(2025, 1, 1, 0, 0, 0)
    time_between_dates = maximum - minimum
    seconds_between_dates = time_between_dates.total_seconds()
    random_number_of_seconds = random.randrange(int(seconds_between_dates))
    r = minimum + datetime.timedelta(seconds=random_number_of_seconds)
    return r.strftime('%Y-%m-%dT%H:%M:%S')


def generate_uuid():
    return str(uuid.uuid4())


def generate_duration():
    n = random.randint(1, 10)
    if random.choice(['P', 'T']) == 'P':
        return "P{}{}".format(n, random.choice(['Y', 'M', 'D', 'W']))
    else:
        return "T{}{}".format(n, random.choice(['H', 'M', 'S']))


def generate_string(minimum, maximum):
    if not minimum:
        minimum = 1
    else:
        minimum = max(minimum, 1)
    if not maximum:
        maximum = 25
    else:
        maximum = max(maximum, 1)
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(minimum, random.randint(minimum + 1, maximum + 1)))


def generate_enum(values):
    return random.choice(values)


def load_json(json_file):
    """
    Load a JSON object for a given file available on DISK
    :param json_file: the absolute path of the file to load
    :return: the JSON object
    """
    if not os.path.isfile(json_file):
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

    def generate(self):
        raise Exception("Not implemented")

    def get_expectations(self) -> {}:
        return validate_nullable(self.field_path, self.is_nullable)

    def get_struct_field(self) -> StructField:
        return StructField(self.field_name, self.get_type(), self.is_nullable, {'comment': self.field_description})


class FieldString(FieldGeneric):
    def __init__(self, *args):
        super().__init__(args)
        self.field_format = self.field_properties.get('format', None)
        if self.field_properties['type'] != 'string':
            raise Exception('FieldString requires string type')

    def generate(self):
        fmt = self.field_format
        prp = self.field_properties
        if fmt == 'date':
            minimum = prp.get('min', None)
            maximum = prp.get('max', None)
            if minimum:
                minimum = datetime.datetime.strptime(minimum, "%Y-%m-%d")
            if maximum:
                maximum = datetime.datetime.strptime(maximum, "%Y-%m-%d")
            return generate_date(minimum, maximum)
        elif fmt == 'date-time':
            minimum = prp.get('min', None)
            maximum = prp.get('max', None)
            if minimum:
                minimum = datetime.datetime.strptime(minimum, "%Y-%m-%d")
            if maximum:
                maximum = datetime.datetime.strptime(maximum, "%Y-%m-%d")
            return generate_timestamp(minimum, maximum)
        elif fmt == 'duration':
            return generate_duration()
        elif fmt == 'uuid':
            return generate_uuid()
        else:
            if prp.get('enum', None):
                return generate_enum(prp['enum'])
            else:
                return generate_string(prp.get('minLength', None), prp.get('maxLength', None))

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
        if self.field_properties['type'] != 'integer' and self.field_properties['type'] != 'number':
            raise Exception('FieldNumber requires integer or number type')

    def generate(self):
        minimum = self.field_properties.get('minimum', None)
        maximum = self.field_properties.get('maximum', None)
        if self.field_properties['type'] == 'integer':
            return generate_integer(minimum, maximum)
        else:
            return generate_number(minimum, maximum)

    def get_expectations(self):
        constraints = super().get_expectations()
        constraints.update(validate_numbers(self.field_path, self.field_properties))
        return constraints


class FieldBoolean(FieldGeneric):
    def __init__(self, *args):
        super().__init__(args)
        if self.field_properties['type'] != 'boolean':
            raise Exception('FieldBoolean requires boolean type')

    def generate(self):
        return bool(random.getrandbits(1))

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
    if (minimum and maximum) or (minimum == 0 and maximum) or (minimum and maximum == 0):
        exp = "{field} IS NULL OR {field} BETWEEN {minimum} AND {maximum}".format(
            field=field_path,
            minimum=float(minimum),
            maximum=float(maximum)
        )
        constraints[nme] = exp
    elif minimum or minimum == 0:
        exp = "{field} IS NULL OR {field} >= {minimum}".format(field=field_path, minimum=float(minimum))
        constraints[nme] = exp
    elif maximum or maximum == 0:
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
    fmt = field_properties.get('format', None)

    if fmt:
        nme = "[{field}] MATCH".format(field=field_path)
        if fmt == 'uuid':
            pattern = '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
            exp = "{field} IS NULL OR {field} RLIKE '{pattern}'".format(field=field_path, pattern=pattern)
            constraints[nme] = exp
        elif fmt == 'duration':
            p1 = '^P\\d{+}[YMWD]$'
            p2 = '^T\\d{+}[HMS]$'
            exp = "{field} IS NULL OR {field} RLIKE '{p1}' OR {field} RLIKE '{p2}'".format(
                field=field_path, p1=p1, p2=p2)
            constraints[nme] = exp
        elif fmt == 'ipv4':
            pattern = '^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$'
            exp = "{field} IS NULL OR {field} RLIKE '{p1}'".format(field=field_path, p1=pattern)
            constraints[nme] = exp
        elif fmt == 'ipv6':
            pattern = '^(?:[A-F0-9]{1,4}:){7}[A-F0-9]{1,4}$'
            exp = "{field} IS NULL OR {field} RLIKE '{p1}'".format(field=field_path, p1=pattern)
            constraints[nme] = exp
        elif fmt == 'hostname':
            pattern = '^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|' \
                      '[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])$'
            exp = "{field} IS NULL OR {field} RLIKE '{p1}'".format(field=field_path, p1=pattern)
            constraints[nme] = exp
        elif fmt == 'uuid':
            pattern = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
            exp = "{field} IS NULL OR {field} RLIKE '{p1}'".format(field=field_path, p1=pattern)
            constraints[nme] = exp
        elif fmt == 'time':
            pattern = '^(0?[1-9]|1[012]):[0-5][0-9]:[0-5][0-9]$'
            exp = "{field} IS NULL OR {field} RLIKE '{pattern}'".format(field=field_path, pattern=pattern)
            constraints[nme] = exp

    if pattern:
        nme = "[{field}] MATCH".format(field=field_path)
        # regexes would certainly get curly brackets that breaks our string formatting
        exp = "{field} IS NULL OR {field} RLIKE '{pattern}'".format(field=field_path, pattern=pattern)
        constraints[nme] = exp

    if enum:
        nme = "[{field}] VALUE".format(field=field_path)
        enums = ', '.join(["'{}'".format(e) for e in enum])
        exp = "{field} IS NULL OR {field} IN ({enums})".format(field=field_path, enums=enums)
        constraints[nme] = exp

    nme = "[{field}] LENGTH".format(field=field_path)
    if (minimum and maximum) or (minimum and maximum == 0) or (minimum == 0 and maximum):
        exp = "{field} IS NULL OR LENGTH({field}) BETWEEN {minimum} AND {maximum}".format(
            field=field_path,
            minimum=int(minimum),
            maximum=int(maximum)
        )
        constraints[nme] = exp
    elif minimum or minimum == 0:
        exp = "{field} IS NULL OR LENGTH({field}) >= {minimum}".format(field=field_path, minimum=int(minimum))
        constraints[nme] = exp
    elif maximum or maximum == 0:
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
    if (minimum and maximum) or (minimum == 0 and maximum) or (minimum and maximum == 0):
        exp = "{field} IS NULL OR SIZE({field}) BETWEEN {minimum} AND {maximum}".format(
            field=field_path,
            minimum=int(minimum),
            maximum=int(maximum)
        )

        constraints[nme] = exp
    elif minimum or minimum == 0:
        exp = "{field} IS NULL OR SIZE({field}) >= {minimum}".format(field=field_path, minimum=int(minimum))
        constraints[nme] = exp
    elif maximum or maximum == 0:
        exp = "{field} IS NULL OR SIZE({field}) <= {maximum}".format(field=field_path, maximum=int(maximum))
        constraints[nme] = exp
    return constraints
