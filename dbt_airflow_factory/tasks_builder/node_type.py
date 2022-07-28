from enum import Enum


class NodeType(Enum):
    RUN_TEST = 1
    MULTIPLE_DEPS_TEST = 2
    EPHEMERAL = 3
    SOURCE_SENSOR = 4
    MOCK_GATEWAY = 5
