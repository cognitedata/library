"""Local entry point."""


from generated_file_loader.handler import handle
from local_run_helpers import get_local_client

if __name__ == "__main__":
    client = get_local_client()

    handle(client=client)
