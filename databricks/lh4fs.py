import os
import re
import json
from pyspark.sql.types import *


def load_json(json_file):
    if not os.path.exists(json_file):
        raise Exception("Could not find file {}".format(json_file))
    with open(json_file) as f:
        return json.loads(f.read())


class L4FSEntity:

    def __init__(self, schema, constraints):
        self.schema = schema
        self.constraints = constraints


class L4FSModel:

    def __init__(self, schema_directory=None):
        if not schema_directory:
            raise Exception("Could not load directory [{}]".format(schema_directory))
        self.schema_directory = schema_directory
        self.constraints = {}

    def __get_array_type(self, tpe, fmt, prp):
        if tpe == "object":
            nested_prp = prp['properties']
            nested_fields = nested_prp.keys()
            nested_required = set(prp['required'])
            nested_structs = []
            for nested_field in nested_fields:
                nested_field_nullable = nested_field not in nested_required
                nested_property = nested_prp[nested_field]
                nested_struct = self.__process_property(
                    nested_field, nested_field_nullable, nested_property, None, None)
                nested_structs.append(nested_struct)
            return StructType(nested_structs)
        elif tpe == "number":
            return DoubleType()
        elif tpe == "integer":
            return IntegerType()
        elif tpe == "boolean":
            return BooleanType()
        elif tpe == "string":
            if not fmt:
                return StringType()
            elif fmt == "date":
                return DateType()
            elif fmt == "date-time":
                return TimestampType()
        raise Exception("Unsupported type {}".format(tpe))

    '''
    Converting a JSON field into a Spark type
    Simple mapping exercise for atomic types (number, string, etc),
    this process becomes complex for nested entities
    For entities of type object, we recursively parse object
    and map their respective types into StructTypes
    For list, we recursively call that function to extract entity types
    '''
    def __process_property_type(self, fqn, name, tpe, nullable, fmt, prp, dsc):

        if tpe == "object":
            # Nested field, we must read its underlying properties
            # Return a complex struct type
            struct = StructType(self.__load_object(prp, fqn))
            struct = StructField(name, struct, nullable, metadata={"desc": dsc})
            self.constraints.update(self.__validate(fqn, nullable))
            return struct

        if tpe == "array":
            # Array type, we need to recursively read its underlying properties
            nested_prp = prp['items']
            nested_tpe = nested_prp['type']
            nested_fmt = nested_prp.get('format', None)
            struct = ArrayType(self.__get_array_type(nested_tpe, nested_fmt, nested_prp))
            self.constraints.update(self.__validate_arrays(fqn, prp, nullable))
            struct = StructField(name, struct, nullable, metadata={"desc": dsc})
            return struct

        if tpe == "number":
            self.constraints.update(self.__validate_numbers(fqn, prp, nullable))
            struct = StructField(name, DoubleType(), nullable, metadata={"desc": dsc})
            return struct

        if tpe == "integer":
            self.constraints.update(self.__validate_numbers(fqn, prp, nullable))
            struct = StructField(name, IntegerType(), nullable, metadata={"desc": dsc})
            return struct

        if tpe == "boolean":
            self.constraints.update(self.__validate(fqn, prp))
            struct = StructField(name, BooleanType(), nullable, metadata={"desc": dsc})
            return struct

        if tpe == "string":
            if not fmt:
                self.constraints.update(self.__validate_strings(fqn, prp, nullable))
                struct = StructField(name, StringType(), nullable, metadata={"desc": dsc})
                return struct

            if fmt == "date-time":
                self.constraints.update(self.__validate_dates(fqn, prp, nullable))
                struct = StructField(name, TimestampType(), nullable, metadata={"desc": dsc})
                return struct

            if fmt == "date":
                self.constraints.update(self.__validate_dates(fqn, prp, nullable))
                struct = StructField(name, DateType(), nullable, metadata={"desc": dsc})
                return struct

        raise Exception("Unsupported type {} for field `{}`".format(tpe, fqn))

    def __validate_strings(self, name, prp, nullable):
        constraints = self.__validate(name, nullable)
        minimum = prp.get('minLength', None)
        maximum = prp.get('maxLength', None)
        pattern = prp.get('pattern', None)
        enum = prp.get('enum', None)

        if enum:
            nme = "[{field}] VALUE".format(field=name)
            enums = ','.join(["'{}'".format(e) for e in enum])
            exp = "{field} IS NULL OR {field} IN ({enums})".format(field=name, enums=enums)
            constraints[nme] = exp

        if pattern:
            nme = "[{field}] MATCH".format(field=name)
            # regexes would certainly get curly brackets that breaks our string formatting
            exp = "{field} IS NULL OR {field} RLIKE '{pattern}'".format(field=name, pattern=pattern)
            constraints[nme] = exp

        nme = "[{field}] LENGTH".format(field=name)
        if minimum and maximum:
            exp = "{field} IS NULL OR LENGTH({field}) BETWEEN {minimum} AND {maximum}".format(
                field=name,
                minimum=int(minimum),
                maximum=int(maximum)
            )
            constraints[nme] = exp
        elif minimum:
            exp = "{field} IS NULL OR LENGTH({field}) >= {minimum}".format(field=name, minimum=int(minimum))
            constraints[nme] = exp
        elif maximum:
            exp = "{field} IS NULL OR LENGTH({field}) <= {maximum}".format(field=name, maximum=int(maximum))
            constraints[nme] = exp

        return constraints

    def __validate_dates(self, name, prp, nullable):
        constraints = self.__validate(name, nullable)
        minimum = prp.get('minimum', None)
        maximum = prp.get('maximum', None)
        nme = "[{field}] VALUE".format(field=name)
        if minimum and maximum:
            exp = "{field} IS NULL OR {field} BETWEEN '{minimum}' AND '{maximum}'".format(
                field=name,
                minimum=str(minimum),
                maximum=str(maximum)
            )
            constraints[nme] = exp
        elif minimum:
            exp = "{field} IS NULL OR {field} >= '{minimum}'".format(field=name, minimum=str(minimum))
            constraints[nme] = exp
        elif maximum:
            exp = "{field} IS NULL OR {field} <= '{maximum}'".format(field=name, maximum=str(maximum))
            constraints[nme] = exp
        return constraints

    def __validate_numbers(self, name, prp, nullable):
        constraints = self.__validate(name, nullable)
        minimum = prp.get('minimum', None)
        maximum = prp.get('maximum', None)
        nme = "[{field}] VALUE".format(field=name)
        if minimum and maximum:
            exp = "{field} IS NULL OR {field} BETWEEN {minimum} AND {maximum}".format(
                field=name,
                minimum=float(minimum),
                maximum=float(maximum)
            )
            constraints[nme] = exp
        elif minimum:
            exp = "{field} IS NULL OR {field} >= {minimum}".format(field=name, minimum=float(minimum))
            constraints[nme] = exp
        elif maximum:
            exp = "{field} IS NULL OR {field} <= {maximum}".format(field=name, maximum=float(maximum))
            constraints[nme] = exp
        return constraints

    def __validate_arrays(self, name, prp, nullable):
        constraints = self.__validate(name, nullable)
        # we cannot validate the integrity of each field
        # without exploding array or running complex UDFs
        # we simply check for array size for now
        minimum = prp.get('minItems', None)
        maximum = prp.get('maxItems', None)
        nme = "[{field}] SIZE".format(field=name)
        if minimum and maximum:
            exp = "{field} IS NULL OR SIZE({field}) BETWEEN {minimum} AND {maximum}".format(
                field=name,
                minimum=float(minimum),
                maximum=float(maximum)
            )

            constraints[nme] = exp
        elif minimum:
            exp = "{field} IS NULL OR SIZE({field}) >= {minimum}".format(field=name, minimum=float(minimum))
            constraints[nme] = exp
        elif maximum:
            exp = "{field} IS NULL OR SIZE({field}) <= {maximum}".format(field=name, maximum=float(maximum))
            constraints[nme] = exp
        return constraints

    @staticmethod
    def __validate(name, nullable):
        constraints = {}
        if not nullable:
            nme = "[{field}] NULLABLE".format(field=name)
            exp = "{field} IS NOT NULL".format(field=name)
            constraints[nme] = exp
        return constraints

    '''
    Process a databricks property (i.e. a field) given a name and a property object
    A field may be a reference to a common object such as currency code,
    so recursive call may be required
    We look at field description
    '''
    def __process_property(self, name, nullable, prp, parent, parent_dsc):

        if parent:
            fqn = "{}.`{}`".format(parent, name)
        else:
            fqn = "`{}`".format(name)

        dsc = prp.get('description', None)
        if parent_dsc:
            # we prefer entity specific description if any rather than generic
            # parent takes precedence
            dsc = parent_dsc

        tpe = prp.get('type', None)
        fmt = prp.get('format', None)
        ref = prp.get('$ref', None)

        # processing referenced property
        if ref:

            # retrieve the name of the Json file and
            # the name of the entity to load
            ref_object = ref.split('/')[-1]
            ref_json = ref.split('#')[0].split('/')[-1]

            # parsing json
            ref_json_file = os.path.join(self.schema_directory, ref_json)
            ref_json_model = load_json(ref_json_file)
            if ref_object not in ref_json_model.keys():
                raise Exception("Referencing non existing property {}".format(ref_object))

            # processing inline property
            ref_property = ref_json_model[ref_object]
            return self.__process_property(name, nullable, ref_property, parent, dsc)

        # processing property
        struct = self.__process_property_type(fqn, name, tpe, nullable, fmt, prp, dsc)
        return struct

    '''
    Some entities may be built as a supertype to other entities
    Example: A customer is a supertype to a person entity
    We load that entire referenced entity as we would
    be loading any object, parsing json file into spark schema
    '''
    def __load_reference(self, ref, parent):
        ref_object = ref.split('/')[-1]
        ref_json_file = os.path.join(self.schema_directory, ref_object)
        ref_json_model = load_json(ref_json_file)
        return self.__load_object(ref_json_model, parent)

    '''
    Core business logic, we process a given entity from a json object
    An entity contains metadata (e.g. description),
    required field definition and property value
    We extract each property, map them to their spark
    type and return the spark schema for that given entity
    '''
    def __load_object(self, model, parent=None):

        schema = []

        # Adding referenced entities
        if "allOf" in model.keys():
            for ref in model['allOf']:
                schema.extend(self.__load_reference(ref['$ref'], parent))
            return schema

        required = model['required']
        fields = model['properties']

        # Processing fields
        for field in fields.keys():
            prp = fields[field]
            nullable = field not in required
            struct = self.__process_property(field, nullable, prp, parent, None)
            schema.append(struct)

        return schema

    '''
    Entry point, given a name of an entity,
    we access and parse underlying json object
    We retrieve all fields, referenced entities,
    metadata as a spark schema
    '''
    def load(self, model):
        json_file = os.path.join(self.schema_directory, "{}.json".format(model))
        json_model = load_json(json_file)
        tpe = json_model.get('type', None)
        if not tpe or tpe != "object":
            raise Exception("Can only process entities of type object")

        struct = self.__load_object(json_model)
        schema = StructType(struct)
        return L4FSEntity(schema, self.constraints)
