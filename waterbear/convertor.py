from waterbear.utils.util import *


class JsonSchemaConvertor:
    """
    This implementation follows JSON schema [standards](https://json-schema.org/).
    Please refer to JSON schema specs for more information about supported data types
    """
    def __init__(self, schema_directory):
        self.schema_directory = schema_directory
        self.constraints = {}
        if not os.path.exists(schema_directory) or not os.path.isdir(schema_directory):
            raise Exception('path {} is not a valid directory'.format(schema_directory))

    def convert(self, entity_name):
        """
        Entry point, given a name of an entity, we access and parse its JSON object
        We retrieve all fields, referenced entities, supertypes and metadata as a spark schema
        We also derive all SQL constraints from the JSON schema, such as mandatory field, string type or minimum bounds
        :param entity_name: The name of the JSON entity to extract Spark Schema from
        :return: The corresponding Spark Schema that embeds our entity and all its underlying objects
        """

        if entity_name.endswith('.json'):
            json_file_name = entity_name
        else:
            json_file_name = "{}.json".format(entity_name)

        json_file = os.path.join(self.schema_directory, json_file_name)
        if not os.path.exists(json_file) or not os.path.isfile(json_file):
            raise Exception('File {} is not a valid file'.format(json_file))

        entity = load_json(json_file)
        entity_type = entity.get('type', None)
        if not entity_type or entity_type != "object":
            raise Exception("Can only process JSON entities of type object")

        # We retrieve all spark fields, nested entities and references
        struct_fields = self.__process_object(entity)

        # And create a spark schema accordingly
        schema = StructType(struct_fields)

        # In addition to the schema, we also return all expectations
        return schema, self.constraints

    def __process_object(self, entity, parent_path=None, process_constraints=True) -> [StructField]:
        """
        Core business logic of a JSON entity, we process a given entity from a JSON object
        An entity contains metadata (e.g. description), required field definition and property value
        We extract each property, map them to their spark type
        :param entity: The JSON entity we want to extract Spark schema from
        :param parent_path: For nested object, we need to know the full hierarchy to attach that entity to (if any)
        :param process_constraints: We may decide to skip constraints stage, especially for array elements
        :return: the list of [pyspark.sql.types.StructField] defining our JSON entity and all its underlying objects
        """

        struct_fields = []
        # Adding supertype entities (`employee` is derived from `person`)
        # We do not support `anyOf`, `oneOf` as we cannot guarantee schema consistency with optional attributes
        # loading this supertype, we will process it just like another JSON entity
        if "allOf" in entity.keys():
            for ref in entity['allOf']:
                struct_fields.extend(self.__process_supertype(ref['$ref'], parent_path, process_constraints))
            return struct_fields

        # we get the list of mandatory fields for our JSON entity
        required = entity.get('required', None)

        # we get all top level fields describing our JSON entity
        fields = entity['properties']

        # we process each field sequentially
        for field_name in fields.keys():
            field_properties = fields[field_name]
            is_nullable = field_name not in required
            # nested object will not inherit from the parent description, hence a None parent description
            # we process each field in a different function to understand complex / simple types
            field_struct = self.__process_field(field_name, is_nullable, field_properties, parent_path, None,
                                                process_constraints)

            # we append each field as spark [pyspark.sql.types.StructField] to our entity schema
            struct_fields.append(field_struct)

        # fully processed that level (the same function may be called for nested elements)
        # returning all of our [pyspark.sql.types.StructField] defining our Spark entity
        return struct_fields

    def __process_supertype(self, ref_link, parent_path=None, process_constraints=True) -> [StructField]:
        """
        Some entities may be built as a supertype to other entities. e.g. A `customer` is a supertype to a `person`.
        We load our referenced entity as we would be loading any object, parsing json file into spark schema
        :param ref_link: the link pointing to a JSON file (must be in our same model directory)
        :param parent_path: the fully qualified name of our parent that we can attach that supertype to (if any)
        :param process_constraints: we may decide to skip constraint stages, especially for array elements
        :return: the list of [pyspark.sql.types.StructField] defining our JSON supertype and all its underlying objects
        """

        ref_object = ref_link.split('/')[-1]
        ref_json_file = os.path.join(self.schema_directory, ref_object)
        ref_json_model = load_json(ref_json_file)
        return self.__process_object(ref_json_model, parent_path, process_constraints)

    def __process_field(self, field_name, is_nullable, field_properties, parent_path,
                        parent_description, process_constraints=True) -> StructField:
        """
        Core business logic of a JSON field, we convert this field as spark schema and SQL constraints
        A field may be a reference to different object, possibly in a different file.
        A field may also be of complex type (i.e. object), requiring recursion.
        :param field_name: field name (relative name)
        :param is_nullable: whether field is not mandatory
        :param field_properties: json properties of the field
        :param parent_path: fully qualified name of the parent object
        :param parent_description: description of the parent object
        :param process_constraints: we may decide to skip constraint stage, especially for array elements
        :return: The Spark [pyspark.sql.types.StructField] embedding our JSON field and its underlying objects
        """

        # as we go through different level of nested values, we must remember the fully qualified name of our field
        # this will be used for evaluating our constraints
        field_path = get_field_path(field_name, parent_path)

        # as we support supertypes (employee extends person), we may have carried metadata over
        # whether a field is described or not, parent reference always takes precedence (more specific)
        field_desc = get_field_description(field_properties, parent_description)

        # fields may be described as a reference to different JSON models.
        if field_properties.get('$ref', None):
            return self.__process_field_reference(field_name, is_nullable, field_properties, parent_path, field_desc,
                                                  process_constraints)

        # processing property of type object (nested element)
        if field_properties['type'] == 'object':
            return self.__process_field_object(field_name, field_path, is_nullable, field_properties, field_desc,
                                               process_constraints)

        # processing property of type array (could be of complex type)
        if field_properties['type'] == "array":
            return self.__process_field_array(field_name, field_path, is_nullable, field_properties, field_desc)

        # processing property at lowest granularity (simple type, no nested elements)
        return self.__process_field_atomic(field_name, field_path, is_nullable, field_properties, field_desc,
                                           process_constraints)

    def __process_field_reference(self, field_name, is_nullable, field_properties, parent_path,
                                  field_desc, process_constraints=True) -> StructField:
        """
        A JSON element might be a reference to another field, eventually to a different JSON file
        :param field_name: the name of the JSON entity
        :param is_nullable: whether this field is optional
        :param field_properties: the JSON properties defining this object
        :param parent_path: the absolute path of that field for accessing via SQL
        :param field_desc: the description of the field
        :param process_constraints: we may decide to skip constraint stages, especially for array elements
        :return: The Spark [pyspark.sql.types.StructField] embedding our referenced JSON entity
        """

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
        return self.__process_field(field_name, is_nullable, ref_property, parent_path, field_desc, process_constraints)

    def __process_field_object(self, field_name, field_path, is_nullable, field_properties, field_desc,
                               process_constraints=True) -> StructField:
        """
        JSON type objects are complex types and must be processed recursively to access each of their nested entities
        :param field_name: the name of our field
        :param field_path: the absolute path of our field that can be used for SQL expressions
        :param is_nullable: whether the array is optional or not
        :param field_properties: the JSON properties defining our array element
        :param field_desc: the description of our field to attach to our metadata (may be inherited from parent)
        :param process_constraints: we may decide to skip constraints stage, especially for Array elements
        :return: The Spark [pyspark.sql.types.StructField] embedding our JSON array
        """

        # Specify default nullable constraints
        if process_constraints:
            field_constraints = validate_nullable(field_path, is_nullable)
            self.constraints.update(field_constraints)

        # Parsing our object nested schema
        struct = StructType(self.__process_object(field_properties, field_path, process_constraints))
        struct = StructField(field_name, struct, is_nullable, metadata={"comment": field_desc})
        return struct

    def __process_field_atomic(self, field_name, field_path, is_nullable, field_properties, field_desc,
                               process_constraints=True) -> StructField:
        """
        Lowest level hierarchy, our field is of simple type that can be converted to a spark object as-is
        :param field_name: the name of our field
        :param field_path: the absolute path of our field that can be used for SQL expressions
        :param is_nullable: whether the array is optional or not
        :param field_properties: the JSON properties defining our array element
        :param field_desc: the description of our field to attach to our metadata (may be inherited from parent)
        :param process_constraints: We may decide to skip constraint stage, especially for array elements
        :return: The Spark [pyspark.sql.types.StructField] embedding our JSON element
        """
        field_type = field_properties['type']
        if field_type == "number" or field_type == "integer":
            o = FieldNumber(field_name, field_path, is_nullable, field_properties, field_desc)
        elif field_type == "boolean":
            o = FieldBoolean(field_name, field_path, is_nullable, field_properties, field_desc)
        elif field_type == "string":
            o = FieldString(field_name, field_path, is_nullable, field_properties, field_desc)
        else:
            raise Exception("Unsupported type {} for field {}".format(field_type, field_path))

        # update constraints
        if process_constraints:
            self.constraints.update(o.get_expectations())
        # and return our structField
        return o.get_struct_field()

    def __process_field_array(self, field_name, field_path, is_nullable, field_properties, field_desc) -> StructField:
        """
        Arrays are slightly more complex as we need to understand their types and process each of its
        underlying entities (if any). As some arrays may be of complex types, we may have to load their referenced
        JSON files and parse each of their nested entities
        :param field_name: the name of our field
        :param field_path: the absolute path of our field that can be used for SQL expressions
        :param is_nullable: whether the array is optional or not
        :param field_properties: the JSON properties defining our array element
        :param field_desc: the description of our field to attach to our metadata (may be inherited from parent)
        :return: The Spark [pyspark.sql.types.StructField] embedding our JSON array
        """

        # Specify default nullable constraints
        field_constraints = validate_nullable(field_path, is_nullable)
        self.constraints.update(field_constraints)

        # Specify array specific constraints (minSize, maxSize, etc)
        self.constraints.update(validate_arrays(field_path, field_properties))

        # Parsing our array and its possible nested schema (for arrays of complex type)
        struct = ArrayType(self.__get_array_type(field_properties['items']))
        struct = StructField(field_name, struct, is_nullable, metadata={"comment": field_desc})
        return struct

    def __get_array_type(self, field_properties) -> DataType:
        """
        Arrays may be of simple or complex types. We treat those as standard object property and retrieve the
        equivalent spark data type and references (if not defined inline).
        :param field_properties: the JSON properties defining our field
        :return: The underlying spark [pyspark.sql.types.DataType] defining our [pyspark.sql.types.ArrayType]
        """

        if field_properties['type'] == "object":
            # Array is of a complex type, we need to access its underlying properties, processing each of them
            # as standard JSON field
            a_properties = field_properties['properties']
            a_fields = a_properties.keys()
            a_fields_required = set(field_properties['required'])
            a_fields_struct = []
            for field in a_fields:
                is_nullable = field not in a_fields_required
                field_struct = self.__process_field(field, is_nullable, a_properties[field], None, None, False)
                a_fields_struct.append(field_struct)
            return StructType(a_fields_struct)
        else:
            # Array is of a simple type, let's find the corresponding spark [pyspark.sql.types.DataType]
            field_format = field_properties.get('format', None)
            field_type = field_properties.get('type', None)
            return get_field_type(field_type, field_format)
