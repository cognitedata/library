#!/usr/bin/env python3
"""
Comprehensive Test Suite for Key Extraction Pipelines
Author: Jonluca Biagini
Version: 1.0.0
"""

import logging
import unittest
from pathlib import Path
from typing import List
from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.logger import CogniteFunctionLogger
import yaml

# Configure logging for tests
logging.basicConfig(level=logging.WARNING)  # Reduce noise during tests

# Add modules to path for imports
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    ExtractionResult,
    KeyExtractionEngine,
)

class TestKeyExtractionpipeline(unittest.TestCase):
    def setUp(self):
        self.logger = CogniteFunctionLogger("DEBUG", True)
        self.sample_assets = [
            {   # Equipment
                "id": "R-130-ABCD-R-K300",
                "name": "R-130-ABCD-R-K300",
                "description": "Sample equipment for testing 300K",
                "equipmentType": "Pump",
                "manufacturer": "Santa Claus",
                "serialNumber": "HO-HO-HO-M3RRY-CHR1$TM@$",
                "type": "Equipment",
                "source_id": "003K-R-DCBA-031-R",
                "metadata": {
                    "site": "Plant_A",
                    "file_subcategory": "PID",
                    "created_at": "2023-01-01T00:00:00Z",
                    "updated_at": "2023-01-01T00:00:00Z",
                    "tags": [
                        "test", 
                        "sample"
                    ]
                }
            },
            {   # File
                "id": "P4LELFMAXDISTRIBUTION",
                "name": "P_4L_ELF_MAX_DISTRIBITUION",
                "description": "Sample timeseries for testing",
                "mimeType": "application/pdf",
                "type": "File",
                "metadata": {
                    "site": "Plant_A",
                    "file_subcategory": "PID",
                    "created_at": "2023-01-01T00:00:00Z",
                    "updated_at": "2023-01-01T00:00:00Z",
                    "tags": [
                        "test", 
                        "sample"
                    ]
                }
            },
            {   # TImeseries
                "id": "25.12.CANDY-FEEDER.001.OUTPUT",
                "name": "OUPUT 001ELF-CF 25.12",
                "description": "The candy feeder's ouput into Santa's sack on 12/25 (Christmas Day!)",
                "unit": "CHRISTMAS",
                "type": "Timeseries",
                "metadata": {
                    "site": "Plant_A",
                    "created_at": "2023-01-01T00:00:00Z",
                    "updated_at": "2023-01-01T00:00:00Z",
                    "tags": [
                        "test", 
                        "sample"
                    ]
                }
            },
            {
                "id": "CNDY_CNVR_BLT_001-140L-ELF",
                "name": "001ELF CANDY CONVEYER",
                "type": "Equipment",
                "labels": [
                    "CANDY",
                    "SWEET",
                    "NO_COAL"
                ],
                "metadata": {
                    "site": "Plant_A",
                    "tags":[
                        "test",
                        "sample"
                    ]
                }
            },
            {
                "id": "CNDY_CNVR_BLT_001-140L-ELF.APPVL.2024",
                "name": "CANDY BELT APPROVAL 2024-001-140L",
                "type": "File",
                "content":"""I, Santa Claus, hereby declare that this candy conveyer belt is fit for the night of CHristmas on this year 2024.
                                In the trust of ELF 140L and the cleanliness of section 001 of Plant_A Site 1 I deem this belt APPROVED FOR USE
                                on CHRISTMAS EVE 2024. This motion is non-veteoable unless pre-approved by misses claus and reviewed by a grand elf
                                jury containing no more than a half majority of candy specialized elf, the star wtiness elf 140L-001 in question, and 
                                a yeti as the stenographer.
                                
                                -Santa""",
                "metadata": {
                    "site": "Plant_A",
                    "tags": [
                        "test",
                        "sample",
                        "approval_document"
                    ]
                }
            }
        ]
    def test_field_selection_demo(self):
        """Test field selection demo pipeline"""
        sample_config = {
            "extraction_rules": [
                {
                    "name": "FIELD SELECTION DEMO",
                    "extraction_type": "candidate_key",
                    "method": "regex",
                    "priority": 80,
                    "source_fields": [
                        {
                            "field_name": "name",
                            "field_type": "string",
                            "required": False,
                            "priority": 1,
                            "role": "target",
                            "max_length": 500,
                            "preprocessing": 
                            [
                                "trim"
                            ]
                        },
                        {
                            "field_name": "description",
                            "field_type": "string",
                            "required": False,
                            "priority": 2,
                            "role": "target",
                            "max_length": 500,
                            "preprocessing": 
                            [
                                "trim"
                            ]
                        },
                        {
                            "field_name": "unit",
                            "field_type": "string",
                            "required": False,
                            "priority": 3,
                            "role": "target",
                            "max_length": 500,
                            "preprocessing":
                            [
                                "trim"
                            ]
                        }
                    ],
                    "config": {
                        "pattern": "\\d+[A-Z]+",
                        "regex_options": {
                            "multiline": False,
                            "dotall": False
                        },
                        "ignore_case": False,
                        "unicode": True,
                        "reassemble_format": None,
                        "max_matches_per_field": 5,
                        "early_termination": True
                    }
                }
            ]
        }

        engine = KeyExtractionEngine(sample_config, self.logger)
        
        results: List[ExtractionResult] = []
        
        for entity in self.sample_assets:
            result = engine.extract_keys(entity, entity.get("type", "unknown"))
            results.append(result)

        self.assertTrue(all((len(result.candidate_keys) >= 1) for result in results))