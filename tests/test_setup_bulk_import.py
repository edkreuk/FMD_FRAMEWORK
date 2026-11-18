import ast
import base64
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]
UTILITY = ROOT / "src" / "NB_UTILITIES_SETUP_FMD.Notebook" / "notebook-content.py"


class Response:
    def __init__(self, status_code, body, headers=None):
        self.status_code = status_code
        self.body = body
        self.text = json.dumps(body)
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self.body


class SetupBulkImport(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.namespace = {
            "os": os, "json": json, "base64": base64,
            "time": Mock(return_value=0), "sleep": Mock(),
            "get_workspace_id_by_name": Mock(return_value="target-workspace"),
            "get_existing_connections_by_id": Mock(return_value=["target-connection"]),
            "invoke_fabric_api_request": Mock(),
            "get_existing_workspace_items": Mock(return_value={}),
            "copy_to_tmp": self.copy_item, "print": Mock(),
        }
        names = {
            "mapping_table_composite_key", "upsert_mapping", "deploy_items_bulk",
            "replace_ids_in_folder", "replace_ids_and_mark_inactive",
        }
        tree = ast.parse(UTILITY.read_text(encoding="utf-8"))
        functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                     and node.name in names]
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(UTILITY), "exec"),
             self.namespace)
        self.items = json.loads((ROOT / "config" / "item_deployment.json").read_text())
        self.items = [item for item in self.items
                      if item["type"] in ("Notebook", "DataPipeline")]
        self.mappings = [
            {"Description": "workspace", "environment": "dev", "ItemType": "Workspace",
             "old_id": "00000000-0000-0000-0000-000000000000", "new_id": "target-workspace"},
            {"Description": "connection", "environment": "config", "ItemType": "connection",
             "old_id": "source-connection", "new_id": "target-connection"},
        ]
        self.tasks = []

    def copy_item(self, name):
        source = ROOT / "src" / name
        target = Path(self.temp.name) / name
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(source, target)
        return str(target)

    def result(self):
        return {"importItemDefinitionsDetails": [
            {"itemLogicalId": self.logical_id(item), "itemId": f"target-{index}",
             "operationStatus": "Succeeded", "itemDisplayName": item["name"],
             "itemType": item["type"]}
            for index, item in enumerate(self.items)
        ]}

    def logical_id(self, item):
        path = ROOT / "src" / item["name"] / ".platform"
        if not path.exists():
            path = Path(self.temp.name) / item["name"] / ".platform"
        return json.loads(path.read_text())["config"]["logicalId"]

    def deploy(self):
        self.namespace["deploy_items_bulk"](
            "CODE", self.items, self.mappings, "dev", self.tasks)

    def test_real_definitions_imported_once_with_folders_and_logical_dependencies(self):
        self.namespace["invoke_fabric_api_request"].return_value = Response(200, self.result())
        self.deploy()
        api = self.namespace["invoke_fabric_api_request"]
        api.assert_called_once()
        method, path, payload = api.call_args.args
        self.assertEqual((method, path),
                         ("post", "workspaces/target-workspace/items/bulkImportDefinitions"))
        self.assertTrue(payload["options"]["allowPairingByName"])
        parts = {part["path"]: base64.b64decode(part["payload"]).decode("utf-8")
                 for part in payload["definitionParts"]}
        batch_logical_ids = {self.logical_id(item) for item in self.items}
        references = []

        def check_dependencies(value):
            if isinstance(value, dict):
                for key in ("pipelineId", "notebookId"):
                    dependency = value.get(key)
                    if isinstance(dependency, str) and not dependency.startswith("@"):
                        self.assertIn(dependency, batch_logical_ids)
                        self.assertEqual(value["workspaceId"],
                                         "00000000-0000-0000-0000-000000000000")
                        references.append(dependency)
                for child in value.values():
                    check_dependencies(child)
            elif isinstance(value, list):
                for child in value:
                    check_dependencies(child)

        for item in self.items:
            folder = "Notebooks" if item["type"] == "Notebook" else "DataPipelines"
            prefix = f"/{folder}/{item['name']}/"
            item_parts = [part for part in payload["definitionParts"]
                          if f"/{item['name']}/" in part["path"]]
            self.assertTrue(item_parts)
            self.assertTrue(all(part["path"].startswith(prefix) for part in item_parts))
            platform = json.loads(parts[prefix + ".platform"])
            self.assertEqual(platform["config"]["logicalId"], self.logical_id(item))
            self.assertEqual(platform["metadata"]["type"], item["type"])
            self.assertTrue(platform["metadata"].get("description"))
            content = "notebook-content.py" if item["type"] == "Notebook" else "pipeline-content.json"
            self.assertIn(prefix + content, parts)
            if item["type"] == "DataPipeline":
                check_dependencies(json.loads(parts[prefix + content]))
        self.assertTrue(references)
        self.assertEqual(len(self.mappings), len(self.items) + 2)
        self.assertEqual(self.tasks[0]["status"], "success")
        self.namespace["get_existing_connections_by_id"].assert_called_once()

    def test_existing_items_are_updated_in_place_and_only_new_ones_bulk_imported(self):
        notebooks = [item for item in self.items if item["type"] == "Notebook"]
        existing = {}
        for index, item in enumerate(notebooks):
            platform = json.loads((ROOT / "src" / item["name"] / ".platform").read_text())
            existing[(item["type"], platform["metadata"]["displayName"].lower())] = f"existing-{index}"
        self.namespace["get_existing_workspace_items"].return_value = existing
        new_items = [item for item in self.items if item["type"] != "Notebook"]
        self.items_all, self.items = self.items, new_items
        result = self.result()
        self.items = self.items_all
        api = self.namespace["invoke_fabric_api_request"]
        api.side_effect = [Response(200, result)] + [Response(200, {})] * len(notebooks)
        self.deploy()
        calls = api.call_args_list
        self.assertEqual(calls[0].args[1], "workspaces/target-workspace/items/bulkImportDefinitions")
        bulk_paths = [part["path"] for part in calls[0].args[2]["definitionParts"]]
        self.assertFalse(any(path.startswith("/Notebooks/") for path in bulk_paths))
        self.assertEqual([call.args[1] for call in calls[1:]], [
            f"workspaces/target-workspace/items/existing-{i}/updateDefinition"
            for i in range(len(notebooks))])
        for call in calls[1:]:
            paths = [part["path"] for part in call.args[2]["definition"]["parts"]]
            self.assertIn("notebook-content.py", paths)
            self.assertIn(".platform", paths)
        updated = {row["new_id"] for row in self.mappings if row["ItemType"] == "Notebook"}
        self.assertEqual(updated, {f"existing-{i}" for i in range(len(notebooks))})
        self.assertEqual(self.tasks[0]["status"], "success")

    def test_connections_replaced_and_unknown_connections_inactive_before_post(self):
        self.items = [{"name": "TEST.DataPipeline", "id": "source-item", "type": "DataPipeline"}]
        folder = Path(self.temp.name) / "TEST.DataPipeline"
        folder.mkdir()
        (folder / ".platform").write_text(json.dumps({
            "metadata": {"type": "DataPipeline", "displayName": "TEST"},
            "config": {"logicalId": "source-item"},
        }))
        activities = [
            {"name": name, "externalReferences": {"connection": connection}}
            for name, connection in [
                ("mapped", "source-connection"), ("missing", "unknown-connection"),
                ("dynamic", "@item().ConnectionGuid"),
            ]
        ]
        (folder / "pipeline-content.json").write_text(json.dumps({
            "properties": {"activities": activities, "workspaceId":
                           "00000000-0000-0000-0000-000000000000"},
        }))
        self.namespace["copy_to_tmp"] = Mock(return_value=str(folder))
        # A rerun must not replace source batch IDs with stale physical IDs.
        self.mappings.append({"Description": "TEST.DataPipeline", "environment": "dev",
                              "ItemType": "DataPipeline", "old_id": "source-item",
                              "new_id": "stale-item"})
        self.namespace["invoke_fabric_api_request"].return_value = Response(200, self.result())
        self.deploy()
        payload = self.namespace["invoke_fabric_api_request"].call_args.args[2]
        part = next(part for part in payload["definitionParts"]
                    if part["path"].endswith("pipeline-content.json"))
        content = json.loads(base64.b64decode(part["payload"]))
        self.assertEqual(content["properties"]["workspaceId"],
                         "00000000-0000-0000-0000-000000000000")
        mapped, missing, dynamic = content["properties"]["activities"]
        self.assertEqual(mapped["externalReferences"]["connection"], "target-connection")
        self.assertNotIn("state", mapped)
        self.assertEqual((missing["state"], missing["onInactiveMarkAs"]),
                         ("Inactive", "Succeeded"))
        self.assertNotIn("state", dynamic)
        self.assertEqual(self.mappings[-1]["new_id"], "target-0")

    def test_http_failure_preserves_fabric_dependency_details(self):
        body = {"errorCode": "DependenciesCouldNotBeResolved", "requestId": "request-id",
                "moreDetails": [{"message": "Missing dependency for PL_FMD_LOAD_ALL"}]}
        self.namespace["invoke_fabric_api_request"].return_value = Response(400, body)
        with self.assertRaisesRegex(RuntimeError, "DependenciesCouldNotBeResolved") as error:
            self.deploy()
        self.assertIn("request-id", str(error.exception))
        self.assertIn("PL_FMD_LOAD_ALL", str(error.exception))
        self.assertEqual(self.tasks, [])

    def test_async_import_polls_and_fetches_result_without_resubmitting(self):
        self.namespace["invoke_fabric_api_request"].side_effect = [
            Response(202, {}, {"x-ms-operation-id": "operation", "Retry-After": "30"}),
            Response(200, {"status": "Running"}, {"Retry-After": "20"}),
            Response(200, {"status": "Succeeded"}),
            Response(200, self.result()),
        ]
        self.deploy()
        calls = self.namespace["invoke_fabric_api_request"].call_args_list
        self.assertEqual([call.args[:2] for call in calls[1:]], [
            ("get", "operations/operation"), ("get", "operations/operation"),
            ("get", "operations/operation/result"),
        ])
        self.assertEqual([call.args[0] for call in calls].count("post"), 1)
        self.assertEqual([call.args[0] for call in self.namespace["sleep"].call_args_list],
                         [30, 20])

    def test_partial_or_missing_results_do_not_record_success(self):
        for status in ("Failed", "SucceededDespiteFailures", "missing"):
            with self.subTest(status=status):
                result = self.result()
                if status == "missing":
                    result["importItemDefinitionsDetails"].pop()
                else:
                    result["importItemDefinitionsDetails"][0]["operationStatus"] = status
                self.namespace["invoke_fabric_api_request"].return_value = Response(200, result)
                with self.assertRaisesRegex(RuntimeError, "Bulk import failed"):
                    self.deploy()
                self.assertEqual(len(self.mappings), 2)
                self.assertEqual(self.tasks, [])

    def test_failed_operation_and_timeout_stop_without_recording_success(self):
        for terminal in ("Failed", "Canceled", "timeout"):
            with self.subTest(terminal=terminal):
                self.namespace["time"].return_value = 0
                self.namespace["invoke_fabric_api_request"].side_effect = [
                    Response(202, {}, {"x-ms-operation-id": "operation"}),
                    Response(200, {"status": terminal}),
                ]
                if terminal == "timeout":
                    self.namespace["time"].side_effect = [0, 0, 1800]
                    error = TimeoutError
                else:
                    error = RuntimeError
                with self.assertRaises(error):
                    self.deploy()
                self.assertEqual(len(self.mappings), 2)
                self.assertEqual(self.tasks, [])

    def test_setup_deploys_prerequisites_before_one_combined_batch(self):
        notebook = json.loads((ROOT / "setup" / "NB_SETUP_FMD.ipynb").read_text())
        cells = ["".join(cell["source"]) for cell in notebook["cells"]
                 if cell["cell_type"] == "code"]
        prerequisite = next(index for index, cell in enumerate(cells)
                            if "('VariableLibrary','Environment')" in cell)
        batch = next(index for index, cell in enumerate(cells) if "deploy_items_bulk(" in cell)
        self.assertLess(prerequisite, batch)
        self.assertIn("('Notebook', 'DataPipeline')", cells[batch])
        self.assertFalse(any("('Notebook','VariableLibrary','Environment')" in cell
                             for cell in cells))


if __name__ == "__main__":
    unittest.main()
