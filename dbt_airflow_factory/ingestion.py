from typing import List

from airflow.models import BaseOperator


class IngestionTasksBuilder:

    def build(self) -> List[BaseOperator]:
        return NotImplementedError("Ingestion task builder")


class AirbyteIngestionTasksBuilder(IngestionTasksBuilder):

    def __init__(self, config: dict):
        self.ingestion_config = config

    def build(self) -> List[BaseOperator]:
        return []

    def __validate_config(self):
        pass