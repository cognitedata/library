# Adding a discovery submodule

1. Create `submodules/<id>/` with Python package code, optional `functions/`, and Toolkit YAML as needed.
2. Register in [`submodules/_registry.py`](../submodules/_registry.py): `id`, `tree_root_id`, `cli_commands`, `toolkit_roots`.
3. Add a UI plugin in `ui/src/modules/<id>/module.tsx` implementing `DiscoveryModule`; append to [`ui/src/modules/discoveryModules.ts`](../ui/src/modules/discoveryModules.ts).
4. Add tree node ids in `ui/server/tree_node_ids.py` and `ui/src/utils/treeNodeIds.ts`.
5. Wire FastAPI routes under `ui/server/` and include in `ui/server/main.py`.
6. Add CLI dispatch in `module.py` (or delegate from a submodule `cli.py`).
7. Register Toolkit paths in `module.toml` `[[extra_resources]]` when the submodule ships deployables.
8. Use function ids `fn_discovery_<module>_*` for new Cognite Functions.
