from typing import Any, Optional

import streamlit as st
from client_factory import CogniteClientFactory
from dotenv import load_dotenv
from ui import PatternManagementUI

st.set_page_config(page_title="Pattern Management", page_icon="🧩", layout="wide")


def main() -> None:
    load_dotenv()
    client: Optional[Any] = CogniteClientFactory.create_from_env()
    ui = PatternManagementUI(client=client)
    ui.render()


if __name__ == "__main__":
    main()