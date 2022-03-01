from waterbear.utils.util import *


class Builder:
    """
    Common interface for our project
    User will have to specify a directory on disk where models might be stored
    """
    def __init__(self, schema_directory):
        self.schema_directory = schema_directory
        self.constraints = {}
        if not os.path.exists(schema_directory) or not os.path.isdir(schema_directory):
            raise Exception('path {} is not a valid directory'.format(schema_directory))

    def build(self, entity_name) -> Schema:
        raise Exception('Not implemented')

