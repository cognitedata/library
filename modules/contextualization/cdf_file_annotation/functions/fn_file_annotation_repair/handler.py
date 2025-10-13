import time
from datetime import datetime, timezone, timedelta
from cognite.client import CogniteClient
from dependencies import create_services

def handle(data: dict, function_call_info: dict, client: CogniteClient):
    """Main entry point for the Cognite Function."""
    start_time = datetime.now(timezone.utc)
    repair_service, logger = create_services(data, client)

    try:
        # Run in a loop for a maximum of 7 minutes
        while datetime.now(timezone.utc) - start_time < timedelta(minutes=7):
            result = repair_service.run()
            if result == "Done":
                logger.info("No more candidates to process. Exiting.", section="END")
                break
            time.sleep(10) # Pause between batches
        
        return {"status": "success", "message": "Repair function completed a cycle."}
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", section="BOTH")
        return {"status": "failure", "message": str(e)}

def run_locally(config_file: dict, log_path: str | None = None):
    """Entry point for local execution and debugging."""
    from dependencies import create_client, get_env_variables

    env_vars = get_env_variables()
    client = create_client(env_vars)
    
    # Mock function_call_info for local runs
    function_call_info = {"function_id": "local", "call_id": "local"}
    
    handle(config_file, function_call_info, client)

if __name__ == "__main__":
    # Example for running locally
    config = {
        "ExtractionPipelineExtId": "ep_file_annotation", # Replace with your pipeline ID
        "logLevel": "DEBUG"
    }
    run_locally(config)