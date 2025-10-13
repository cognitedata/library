from cognite.client import CogniteClient
from services.ConfigService import Config, load_config_parameters
from services.LoggerService import CogniteFunctionLogger
from services.RepairService import GeneralRepairService

def create_services(data: dict, client: CogniteClient):
    """Factory function to create all necessary services."""
    config, client = load_config_parameters(client=client, function_data=data)
    logger = CogniteFunctionLogger(log_level=data.get("logLevel", "INFO"))
    
    repair_service = GeneralRepairService(
        client=client,
        config=config,
        logger=logger,
    )
    
    return repair_service, logger