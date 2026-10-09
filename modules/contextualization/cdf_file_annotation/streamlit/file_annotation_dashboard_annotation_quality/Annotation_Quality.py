import streamlit as st
from client_factory import CogniteClientFactory
from cognite.client import CogniteClient
from dotenv import load_dotenv
from ui import AnnotationQualityUI
from usage_tracking import report_usage

st.set_page_config(page_title="Annotation Quality", page_icon="🎯", layout="wide")


def main() -> None:
    load_dotenv()
    client: CogniteClient | None = CogniteClientFactory.create_from_env()
    report_usage(client)
    ui = AnnotationQualityUI(client=client)
    ui.render()


if __name__ == "__main__":
    main()
