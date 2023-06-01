from __future__ import annotations

import logging
import os
from collections import OrderedDict
from enum import Enum
from typing import Optional, Sequence, Tuple

import yaml
from dbt_semantic_interfaces.implementations.base import FrozenBaseModel

logger = logging.getLogger(__name__)

DOCUMENT_KEY = "integration_test"


class IntegrationTestModel(Enum):
    """Names of the models that are possible to specify in the test case files."""

    SIMPLE_MODEL = "SIMPLE_MODEL"
    SIMPLE_MODEL_NON_DS = "SIMPLE_MODEL_NON_DS"
    UNPARTITIONED_MULTI_HOP_JOIN_MODEL = "UNPARTITIONED_MULTI_HOP_JOIN_MODEL"
    PARTITIONED_MULTI_HOP_JOIN_MODEL = "PARTITIONED_MULTI_HOP_JOIN_MODEL"
    EXTENDED_DATE_MODEL = "EXTENDED_DATE_MODEL"
    SCD_MODEL = "SCD_MODEL"


class RequiredDwEngineFeatures(Enum):
    """Required features that are needed in the DW engine for the test to run."""

    DATE_TRUNC = "DATE_TRUNC"
    FULL_OUTER_JOIN = "FULL_OUTER_JOIN"
    CONTINUOUS_PERCENTILE_AGGREGATION = "CONTINUOUS_PERCENTILE_AGGREGATION"
    DISCRETE_PERCENTILE_AGGREGATION = "DISCRETE_PERCENTILE_AGGREGATION"
    APPROXIMATE_CONTINUOUS_PERCENTILE_AGGREGATION = "APPROXIMATE_CONTINUOUS_PERCENTILE_AGGREGATION"
    APPROXIMATE_DISCRETE_PERCENTILE_AGGREGATION = "APPROXIMATE_DISCRETE_PERCENTILE_AGGREGATION"

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}.{self.name}"


class ConfiguredIntegrationTestCase(FrozenBaseModel):
    """Integration test case parsed from YAML files."""

    # Pydantic feature to throw errors on extra fields.
    class Config:  # noqa: D
        extra = "forbid"

    # Name of the test.
    name: str
    # Name of the semantic model to use.
    model: IntegrationTestModel
    metrics: Tuple[str, ...]
    # The SQL query that can be run to obtain the expected results.
    check_query: str
    file_path: str
    group_bys: Tuple[str, ...] = ()
    order_bys: Tuple[str, ...] = ()
    # The required features in the DW engine for the test to complete.
    required_features: Tuple[RequiredDwEngineFeatures, ...] = ()
    # Whether to check the order of the rows / columns.
    check_order: bool = False
    allow_empty: bool = False
    time_constraint: Optional[Tuple[str, str]] = None
    where_filter: Optional[str] = None
    limit: Optional[int] = None
    description: Optional[str] = None


class TestCaseParseException(Exception):
    """Exception thrown when there is an error parsing the YAML test configuration."""

    pass


class ConfiguredIntegrationTestCaseRepository:
    """Stores integration test cases generated by parsing YAML files."""

    def __init__(self, config_directory: str) -> None:
        """Constructor.

        Args:
            config_directory: directory that should be searched for YAML files containing test cases.
        """
        self._config_directory = config_directory
        self._test_case_file_paths = ConfiguredIntegrationTestCaseRepository._find_all_yaml_file_paths(
            self._config_directory
        )
        self._test_cases: OrderedDict = OrderedDict()

        for file_path in self._test_case_file_paths:
            test_cases = ConfiguredIntegrationTestCaseRepository._parse_config_yaml(file_path)
            for test_case in test_cases:
                qualified_name = f"{os.path.basename(file_path)}/{test_case.name}"
                if qualified_name in self._test_cases:
                    raise ValueError(f"Test with a duplicate test name found: {test_case.name}")
                self._test_cases[qualified_name] = test_case

    @staticmethod
    def _parse_config_yaml(file_path: str) -> Sequence[ConfiguredIntegrationTestCase]:
        """Parse the YAML file at the given path into test cases."""
        results = []

        with open(file_path) as f:
            file_contents = f.read()
            for config_document in yaml.load_all(stream=file_contents, Loader=yaml.SafeLoader):
                # The config document can be None if there is nothing but white space between two `---`
                # this isn't really an issue, so lets just swallow it
                if config_document is None:
                    continue
                if not isinstance(config_document, dict):
                    raise TestCaseParseException(
                        f"Test query object YAML must be a dict. Got `{type(config_document)}`: {config_document}"
                    )

                keys = tuple(x for x in config_document.keys())
                if len(keys) != 1:
                    raise TestCaseParseException(
                        f"Test case document should have one type of key, but this has {len(keys)}. "
                        f"Found keys: {keys} in {file_path}",
                    )

                # retrieve last top-level key as type
                document_type = next(iter(config_document.keys()))
                object_cfg = config_document[document_type]
                if document_type == DOCUMENT_KEY:
                    try:
                        results.append(ConfiguredIntegrationTestCase(**object_cfg, file_path=file_path))
                    except Exception as e:
                        raise TestCaseParseException(f"Error while parsing: {file_path}") from e
                else:
                    raise TestCaseParseException(f"Expected {DOCUMENT_KEY}, but got {document_type}")
        return results

    @staticmethod
    def _find_all_yaml_file_paths(directory: str) -> Sequence[str]:  # noqa: D
        """Recursively search through the given directory for YAML files."""
        test_case_file_paths = []

        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(".yaml"):
                    test_case_file_paths.append(os.path.join(root, file))

        return sorted(test_case_file_paths)

    @property
    def all_test_case_names(self) -> Sequence[str]:
        """Return test case names as specified in the YAML files."""
        return tuple(self._test_cases.keys())

    def get_test_case(self, test_case_name: str) -> ConfiguredIntegrationTestCase:
        """Get a specific test case by name. Throws an error if the given test case does not exist."""
        if test_case_name not in self._test_cases:
            raise ValueError(f"Unknown test case: {test_case_name}")
        return self._test_cases[test_case_name]


CONFIGURED_INTEGRATION_TESTS_REPOSITORY = ConfiguredIntegrationTestCaseRepository(
    os.path.join(os.path.dirname(__file__), "test_cases"),
)
