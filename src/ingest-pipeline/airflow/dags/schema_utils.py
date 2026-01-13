from os.path import dirname, join, realpath
from typing import Any, Literal, Union

from hubmap_commons.schema_tools import assert_json_matches_schema, set_schema_base_path

JSONType = Union[str, int, float, bool, None, dict[str, Any], list[Any]]

SCHEMA_BASE_PATH = join(dirname(dirname(dirname(realpath(__file__)))), "schemata")
SCHEMA_BASE_URI = "http://schemata.hubmapconsortium.org/"

set_schema_base_path(SCHEMA_BASE_PATH, SCHEMA_BASE_URI)


def localized_assert_json_matches_schema(jsn: JSONType, schemafile: str) -> Literal[True]:
    """
    This version of assert_json_matches_schema knows where to find schemata used by this module
    """
    try:
        return assert_json_matches_schema(jsn, schemafile)  # localized by set_schema_base_path
    except AssertionError as e:
        print("ASSERTION FAILED: {}".format(e))
        raise
