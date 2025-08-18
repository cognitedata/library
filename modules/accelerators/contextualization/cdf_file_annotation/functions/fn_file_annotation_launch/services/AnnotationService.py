import abc
from typing import Any
from cognite.client import CogniteClient
from services.ConfigService import Config

from cognite.client.data_classes.contextualization import (
    DiagramDetectResults,
    DiagramDetectConfig,
    FileReference,
)

from services.LoggerService import CogniteFunctionLogger


class IAnnotationService(abc.ABC):
    """
    Interface for interacting with the diagram detect and other contextualization endpoints
    """

    @abc.abstractmethod
    def run_diagram_detect(self, files: list[FileReference], entities: list[dict[str, Any]]) -> int:
        pass

    @abc.abstractmethod
    def run_pattern_mode_detect(self, files: list[FileReference], pattern_samples: list[dict[str, Any]]) -> int:
        pass


# maybe a different class for debug mode and run mode?
class GeneralAnnotationService(IAnnotationService):
    """
    Build a queue of files that are in the annotation process and return the jobId
    """

    def __init__(self, config: Config, client: CogniteClient, logger: CogniteFunctionLogger):
        self.client: CogniteClient = client
        self.config: Config = config
        self.logger: CogniteFunctionLogger = logger

        self.annotation_config = config.launch_function.annotation_service
        self.diagram_detect_config: DiagramDetectConfig | None = None
        if config.launch_function.annotation_service.diagram_detect_config:
            self.diagram_detect_config = config.launch_function.annotation_service.diagram_detect_config.as_config()

    def run_diagram_detect(self, files: list[FileReference], entities: list[dict[str, Any]]) -> int:
        detect_job: DiagramDetectResults = self.client.diagrams.detect(
            file_references=files,
            entities=entities,
            partial_match=self.annotation_config.partial_match,
            min_tokens=self.annotation_config.min_tokens,
            search_field="search_property",
            configuration=self.diagram_detect_config,
        )
        if detect_job.job_id:
            return detect_job.job_id
        else:
            raise Exception(f"API call to diagram/detect in pattern mode did not return a job ID")

    def run_pattern_mode_detect(self, files: list, pattern_samples: list[dict[str, Any]]) -> int:
        """Generates patterns and runs the diagram detection job in pattern mode."""
        self.logger.info(f"Generated {len(pattern_samples)} pattern samples for detection.")

        detect_job: DiagramDetectResults = self.client.diagrams.detect(
            file_references=files,
            entities=pattern_samples,  # Use the generated patterns
            partial_match=self.annotation_config.partial_match,
            min_tokens=self.annotation_config.min_tokens,
            search_field="sample",  # The key in your generated samples
            configuration=self.diagram_detect_config,
            pattern_mode=True,  # The crucial flag
        )
        if detect_job.job_id:
            return detect_job.job_id
        else:
            raise Exception("API call to diagram/detect in pattern mode did not return a job ID")
