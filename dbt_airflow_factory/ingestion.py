from enum import Enum
from typing import List

from airflow.models import BaseOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator


class IngestionEngine(Enum):
    AIRBYTE = "airbyte"

    @classmethod
    def value_of(cls, value: str) -> "IngestionEngine":
        return IngestionEngine(value)


class IngestionTasksBuilder:
    def build(self) -> List[BaseOperator]:
        raise NotImplementedError("Should implement build method")


class AirbyteIngestionTasksBuilder(IngestionTasksBuilder):
    def __init__(self, config: dict):
        self.ingestion_config = config

    def build(self) -> List[BaseOperator]:
        airflow_tasks = []
        tasks = self.ingestion_config["tasks"]
        for task in tasks:
            airflow_tasks.append(
                AirbyteTriggerSyncOperator(
                    task_id=task["task_id"],
                    airbyte_conn_id=self.ingestion_config["airbyte_connection_id"],
                    connection_id=task["connection_id"],
                    asynchronous=task["asyncrounous"],
                    api_version=task["api_version"],
                    wait_seconds=task["wait_seconds"],
                    timeout=task["timeout"],
                )
            )

        return airflow_tasks


class IngestionFactory:
    def __init__(self, ingestion_config: dict, name: IngestionEngine):
        self.ingestion_config = ingestion_config
        self.name = name

    def create(
        self,
    ) -> IngestionTasksBuilder:
        if self.name == IngestionEngine.AIRBYTE:
            return AirbyteIngestionTasksBuilder(self.ingestion_config)
        raise NotImplementedError(f"{self.name} is not supported !")
