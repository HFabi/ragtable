
class App:
    DATA_DIR_PATH = "../../data"
    SOURCE_DIR_NAME = "source"
    RUN_DIR_NAME = "run"
    #RUN_SET_DIR_NAME = "runset"


class ConfigYML:
    USE_CASE_NAME = 'name'
    FLAVOR_DEFAULT = "default"

    TRANSFORM = "transform"
    TRANSFORM_TYPE = 'type'
    TRANSFORM_FROM = 'from'
    TRANSFORM_REPLACE_CONTENT = 'replace_content'
    TRANSFORM_REPLACE_CONTENT_FIND = 'find'
    TRANSFORM_REPLACE_CONTENT_REPLACE = 'replacement'

    METADATA = "metadata"
    METADATA_AVERAGE = "average"
    METADATA_RATE = "rate"
    METADATA_METRICS_CUSTOM = 'metrics_custom'
    METADATA_METRICS_DISCRETE = 'metrics_discrete'
    METADATA_METRICS_RATE = 'metrics_rate'
