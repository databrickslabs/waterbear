from waterbear.utils.util import *
import random


# EXPERIMENTAL, proceed at your own risk
class JsonRecordGenerator:
    def __init__(self, schema_directory, nullable_rate=0.0):
        self.schema_directory = schema_directory
        self.nullable_rate = nullable_rate
        if not os.path.exists(schema_directory) or not os.path.isdir(schema_directory):
            raise Exception('path {} is not a valid directory'.format(schema_directory))

    def generate(self, entity_name, num) -> [str]:
        entity = self.__load_object(entity_name)
        return [json.dumps(self.__generate_object(entity)) for _ in range(num)]

    def __it_happens(self):
        return random.uniform(0, 1) <= self.nullable_rate

    def __load_object(self, entity_name):
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
        return entity

    def __generate_object(self, entity):
        struct = {}
        if "allOf" in entity.keys():
            for ref in entity['allOf']:
                struct.update(self.__generate_supertype(ref['$ref']))
        else:
            required = entity.get('required', None)
            fields = entity['properties']
            for field_name in fields.keys():
                field_properties = fields[field_name]
                is_nullable = field_name not in required
                if is_nullable and self.__it_happens():
                    continue
                else:
                    struct[field_name] = self.__generate_field(is_nullable, field_properties)
        return struct

    def __generate_supertype(self, ref_link):
        ref_object = ref_link.split('/')[-1]
        ref_json_file = os.path.join(self.schema_directory, ref_object)
        ref_json_model = load_json(ref_json_file)
        return self.__generate_object(ref_json_model)

    def __generate_field(self, is_nullable, field_properties):
        if field_properties.get('$ref', None):
            return self.__generate_field_reference(is_nullable, field_properties)
        elif field_properties['type'] == 'object':
            return self.__generate_object(field_properties)
        elif field_properties['type'] == "array":
            minimum = field_properties.get('minItems', 0)
            maximum = field_properties.get('maxItems', 1)
            items = range(max(minimum, 0), max(maximum, 0) + 1)
            return [self.__generate_array_item(is_nullable, field_properties) for _ in items]
        else:
            return self.__generate_field_atomic(is_nullable, field_properties)

    def __generate_field_reference(self,  is_nullable, field_properties):
        field_ref = field_properties['$ref']
        ref_object = field_ref.split('/')[-1]
        ref_json = field_ref.split('#')[0].split('/')[-1]
        ref_json_file = os.path.join(self.schema_directory, ref_json)
        ref_json_model = load_json(ref_json_file)
        if ref_object not in ref_json_model.keys():
            raise Exception("Referencing non existing property {}".format(ref_object))
        ref_property = ref_json_model[ref_object]
        return self.__generate_field(is_nullable, ref_property)

    @staticmethod
    def __generate_field_atomic(is_nullable, field_properties):
        field_type = field_properties['type']
        if field_type == "number" or field_type == "integer":
            o = FieldNumber(None, None, is_nullable, field_properties, None)
        elif field_type == "boolean":
            o = FieldBoolean(None, None, is_nullable, field_properties, None)
        elif field_type == "string":
            field_properties['type'] = 'string'
            o = FieldString(None, None, is_nullable, field_properties, None)
        else:
            raise Exception("Unsupported type {} for field {}".format(field_type, None))
        return o.generate()

    def __generate_array_item(self, is_nullable, field_properties):
        if field_properties['type'] == "object":
            return self.__generate_object(field_properties['properties'])
        if field_properties.get('items', None):
            return self.__generate_field(False, field_properties['items'])
        else:
            return self.__generate_field_atomic(is_nullable, field_properties)
