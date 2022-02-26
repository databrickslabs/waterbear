from lh4fs.utils import *
from pyspark.sql.types import *


class LegendBuilder:
    def __init__(self, schema_directory=None):
        raise Exception("Legend is not yet supported")

    def build(self, model):
        raise Exception("Legend is not yet supported")


class JsonBuilder:

    def __init__(self, schema_directory=None):
        if not schema_directory:
            raise Exception("Could not load directory [{}]".format(schema_directory))
        self.schema_directory = schema_directory
        self.constraints = {}

    def build(self, entity_name):
        """
        Entry point, given a name of an entity,
        we access and parse underlying json object
        We retrieve all fields, referenced entities,
        metadata as a spark schema
        :param entity_name: The name of the JSON entity to extract Spark Schema from
        :return: The corresponding Spark Schema that embeds our entity and all its underlying objects
        """
        if ".json" in entity_name:
            json_file_name = entity_name
        else:
            json_file_name = "{}.json".format(entity_name)
        json_file = os.path.join(self.schema_directory, json_file_name)
        entity = load_json(json_file)
        entity_type = entity.get('type', None)
        if not entity_type or entity_type != "object":
            raise Exception("Can only process entities of type object")

        # We retrieve all spark fields, nested entities and references
        struct_fields = self.__load_object(entity)

        # And create a spark schema accordingly
        schema = StructType(struct_fields)

        # In addition to the schema, we also return all expectations
        return schema, self.constraints

    def __load_object(self, entity, parent_path=None):
        """
        Core business logic, we process a given entity from a json object
        An entity contains metadata (e.g. description),
        required field definition and property value
        We extract each property, map them to their spark
        type and return the spark schema for that given entity
        :param entity: The JSON entity we want to extract Spark Schema from
        :param parent_path: For nested object, we need to know the full hierarchy to attach that entity to
        :return: the list of Spark StructField defining our entity
        """

        struct_fields = []

        # Adding supertype entities (employee is derived from person)
        # We do not support anyOf, oneOf as we cannot guarantee valid schema with optional entities
        if "allOf" in entity.keys():
            for ref in entity['allOf']:
                struct_fields.extend(self.__load_supertype_object(ref['$ref'], parent_path))
            return struct_fields

        required = entity['required']
        fields = entity['properties']

        # Processing fields
        for field_name in fields.keys():
            field_properties = fields[field_name]
            is_nullable = field_name not in required
            field_struct = self.__process_field(field_name, is_nullable, field_properties, parent_path, None)
            struct_fields.append(field_struct)

        return struct_fields

    def __load_supertype_object(self, ref_link, parent_path):
        """
        Some entities may be built as a supertype to other entities
        Example: A customer is a supertype to a person entity
        We load that entire referenced entity as we would
        be loading any object, parsing json file into spark schema
        :param ref_link: the link pointing to a JSON file
        :param parent_path: the fully qualified name of our parent
        :return: the spark StructType that defines our schema at that parent level
        """
        ref_object = ref_link.split('/')[-1]
        ref_json_file = os.path.join(self.schema_directory, ref_object)
        ref_json_model = load_json(ref_json_file)
        return self.__load_object(ref_json_model, parent_path)

    def __process_field(self, field_name, is_nullable, field_properties, parent_path, parent_description):
        """
        Process a JSON property (i.e. a field) given a name and a property object
        A field may be a reference to a common object such as currency code, so recursion may be required
        :param field_name: field name
        :param is_nullable: whether field is not mandatory
        :param field_properties: json properties of the field
        :param parent_path: fully qualified name of the parent object
        :param parent_description: description of the parent object
        :return: The Spark DataType embedding our JSON field and its possible underlying objects
        """
        # as we go through different level of nested values, we must remember the fully qualified name of our field
        # this will be used for evaluating our constraints
        field_path = build_field_path(field_name, parent_path)

        # as we support supertypes (employee extends person), we may have carried metadata over
        # whether a field is described or not, parent reference always takes precedence (more specific)
        field_desc = build_field_desc(field_properties, parent_description)

        # fields may be described as a reference to different JSON models.
        if field_properties.get('$ref', None):

            # retrieve the name of the Json file and
            # the name of the entity to load
            field_ref = field_properties['$ref']
            ref_object = field_ref.split('/')[-1]
            ref_json = field_ref.split('#')[0].split('/')[-1]

            # parsing json
            ref_json_file = os.path.join(self.schema_directory, ref_json)
            ref_json_model = load_json(ref_json_file)
            if ref_object not in ref_json_model.keys():
                raise Exception("Referencing non existing property {}".format(ref_object))

            # processing inline property
            ref_property = ref_json_model[ref_object]
            return self.__process_field(field_name, is_nullable, ref_property, parent_path,
                                        field_desc)

        # Specify default nullable constraints
        field_constraints = validate_nullable(field_path, is_nullable)
        self.constraints.update(field_constraints)

        # processing property
        field_type = field_properties.get('type', None)
        if field_type == 'object':
            # Nested field, we must read its underlying properties
            # Return a complex struct type
            struct = StructType(self.__load_object(field_properties, field_path))
            struct = StructField(field_name, struct, is_nullable, metadata={"desc": field_desc})
            return struct
        elif field_type == "array":
            # Array type, we need to recursively read its underlying properties
            nested_prp = field_properties['items']
            nested_tpe = nested_prp['type']
            nested_fmt = nested_prp.get('format', None)
            self.constraints.update(validate_arrays(field_path, field_properties))
            struct = ArrayType(self.__convert_type_array(nested_tpe, nested_fmt, nested_prp))
            struct = StructField(field_name, struct, is_nullable, metadata={"desc": field_desc})
            return struct
        else:
            # lower level hierarchy, fields are atomic fields
            return self.__process_atomic(field_name, field_path, is_nullable, field_type, field_properties, field_desc)

    def __process_atomic(self, field_name, field_path, is_nullable, field_type, field_properties, field_description):
        """
        Converting a JSON field into a Spark type and update our validation rules
        Simple mapping exercise for atomic types (number, string, etc), this process becomes complex for nested entities
        For entities of type object, we recursively parse object and map their respective types into StructTypes
        For list, we recursively extract its underlying entities into an ArrayType
        :param field_name: field name
        :param field_path: fully qualified name
        :param is_nullable: whether field is optional
        :param field_type: field type
        :param field_properties: json properties of the field
        :param field_description: field description
        :return: The Spark DataType embedding our highest granularity JSON field
        """

        if field_type == "number":
            o = LH4FSNumber(field_name, field_path, is_nullable, field_properties, field_description)
            self.constraints.update(o.validate())
            return o.convert()

        if field_type == "integer":
            o = LH4FSInteger(field_name, field_path, is_nullable, field_properties, field_description)
            self.constraints.update(o.validate())
            return o.convert()

        if field_type == "boolean":
            o = LH4FSBoolean(field_name, field_path, is_nullable, field_properties, field_description)
            self.constraints.update(o.validate())
            return o.convert()

        if field_type == "string":
            o = LH4FSString(field_name, field_path, is_nullable, field_properties, field_description)
            self.constraints.update(o.validate())
            return o.convert()

        raise Exception("Unsupported type {} for field {}".format(field_type, field_path))

    def __convert_type_array(self, field_type, field_format, field_properties):
        """
        Arrays may be of simple or complex types. We treat those as standard object property and retrieve the
        equivalent spark data type. For complex types (i.e. object), we recursively access the underlying properties
        and references (if not defined inline). Note that values of Arrays cannot be validated using expectations
        :param field_type: the field JSON type
        :param field_format: the field JSON format for entities of type string (e.g. date, date-time)
        :param field_properties: the JSON properties defining our field
        :return: The underlying spark StructType defining our ArrayType
        """

        if field_type == "object":
            # Field is of a complex type, we need to access its underlying properties
            nested_properties = field_properties['properties']
            nested_fields = nested_properties.keys()
            nested_required = set(field_properties['required'])
            nested_struct_fields = []
            for nested_field in nested_fields:
                nested_field_nullable = nested_field not in nested_required
                nested_property = nested_properties[nested_field]
                nested_struct_field = self.__process_field(nested_field, nested_field_nullable, nested_property, None,
                                                           None)
                nested_struct_fields.append(nested_struct_field)
            return StructType(nested_struct_fields)
        else:
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
                raise Exception('Unsupported array of type {}'.format(field_type))
