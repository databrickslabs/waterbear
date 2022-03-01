from waterbear.builder import Builder


class LegendBuilder(Builder):
    """
    We plan to extend our framework to support multiple data model formats, including Legend
    given our recent contribution to [FINOS LABS](https://github.com/finos-labs/legend-delta)
    """
    def __init__(self, schema_directory=None):
        super().__init__(schema_directory)
