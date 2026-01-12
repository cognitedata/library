import streamlit as st
from typing import Callable, Optional
from constants import FieldNames


class FactoryHandler:
    @staticmethod
    def make_reset_selection_handler(selected_state_key: str) -> Callable[[], None]:
        def _handler() -> None:
            st.session_state[selected_state_key] = None

        return _handler

    @staticmethod
    def make_single_selection_handler(
        editor_key: str,
        selected_index_state_key: Optional[str] = None,
    ) -> Callable[[], None]:
        def _handler() -> None:
            if selected_index_state_key is None:
                return

            payload = st.session_state.get(editor_key)
            if not isinstance(payload, dict):
                st.session_state[selected_index_state_key] = None
                return

            edited_rows = payload.get("edited_rows") or {}
            data_rows = payload.get("data") if isinstance(payload.get("data"), list) else None

            selected_row_index = None

            if isinstance(edited_rows, dict) and edited_rows:
                try:
                    first_key = next(iter(edited_rows))
                    selected_row_index = int(first_key)
                except Exception:
                    selected_row_index = None

            if isinstance(data_rows, list):
                for row in data_rows:
                    if isinstance(row, dict):
                        row[FieldNames.SELECT_TITLE_CASE] = False
                        row[FieldNames.SELECT_TITLE_CASE.lower()] = False
                        row["Select"] = False

                for raw_idx, changes in (edited_rows.items() if isinstance(edited_rows, dict) else []):
                    try:
                        idx = int(raw_idx)
                    except Exception:
                        continue
                    if 0 <= idx < len(data_rows) and isinstance(changes, dict):
                        for k, v in changes.items():
                            data_rows[idx][k] = v

                if isinstance(selected_row_index, int) and 0 <= selected_row_index < len(data_rows):
                    data_rows[selected_row_index][FieldNames.SELECT_TITLE_CASE] = True
                    data_rows[selected_row_index][FieldNames.SELECT_TITLE_CASE.lower()] = True
                    data_rows[selected_row_index]["Select"] = True

                payload["data"] = data_rows
                st.session_state[editor_key] = payload

            try:
                if isinstance(selected_row_index, int):
                    st.session_state[selected_index_state_key] = int(selected_row_index)
                else:
                    st.session_state[selected_index_state_key] = selected_row_index
            except Exception:
                st.session_state[selected_index_state_key] = None

        return _handler

    @staticmethod
    def make_clear_meta_values_handler(key: str) -> Callable:
        def _handler():
            st.session_state[key] = []

        return _handler