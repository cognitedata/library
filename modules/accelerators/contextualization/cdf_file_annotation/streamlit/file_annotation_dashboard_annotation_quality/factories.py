import streamlit as st
from typing import Callable, Optional
from constants import FieldNames


class SingleSelectionHandlerFactory:
    @staticmethod
    def make_single_selection_handler(editor_key: str, df_state_key: str, selected_index_state_key: Optional[str] = None, selected_tag_suffix: str = "_selected_tag") -> Callable:
        def _handler(_editor_key: str = editor_key, _df_state_key: str = df_state_key):
            # TODO: Implement SingleSelectionHandler logic
            pass

        return _handler

class DataEditorChangeCaptureFactory:
    @staticmethod
    def make_change_capture_handler(df, editor_key: str, key_name: str, changed_keys_state_key: str) -> Callable:
        def _handler():
            payload = st.session_state.get(editor_key)

            if not isinstance(payload, dict):
                return

            edited_rows = payload.get(FieldNames.EDITED_ROWS_SNAKE_CASE, [])
            added_rows = payload.get(FieldNames.ADDED_ROWS_SNAKE_CASE, [])
            deleted_rows = payload.get(FieldNames.DELETED_ROWS_SNAKE_CASE, [])

            for row_idx in edited_rows:
                row = df.iloc[row_idx]
                old_value = row.get(key_name, None)

                if old_value:
                    st.session_state[changed_keys_state_key].add(old_value)

                new_value = edited_rows[row_idx].get(key_name, None)

                if new_value:
                    st.session_state[changed_keys_state_key].add(new_value)

            for row_idx in deleted_rows:
                row = df.iloc[row_idx]
                row_key = row.get(key_name, None)

                if row_key:
                    st.session_state[changed_keys_state_key].add(row_key)

            for row in added_rows:
                row_key = row.get(key_name, None)

                if row_key:
                    st.session_state[changed_keys_state_key].add(row_key)
        return _handler