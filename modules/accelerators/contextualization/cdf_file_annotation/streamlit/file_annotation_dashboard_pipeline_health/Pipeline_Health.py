import streamlit as st
from ui import PipelineHealthUI
from typing import Any, Optional
from dotenv import load_dotenv
from client_factory import CogniteClientFactory

st.set_page_config(page_title="Pipeline Health", page_icon="🩺", layout="wide")


def main() -> None:
    load_dotenv()
    client: Optional[Any] = CogniteClientFactory.create_from_env()
    ui = PipelineHealthUI(client)
    ui.render()

if __name__ == "__main__":
	main()

