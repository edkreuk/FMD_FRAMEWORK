import json
import re
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VIEW = "[execution].[vw_LoadSourceToLandingzone]"
WORKSPACE = "schedule-group-test-workspace"


def lookup_queries(value):
    if isinstance(value, dict):
        if value.get("type") == "Lookup":
            query = value["typeProperties"].get("source", {}).get("sqlReaderQuery")
            sql = query.get("value") if isinstance(query, dict) else query
            if sql and VIEW in sql:
                yield value["name"], sql
        for child in value.values():
            yield from lookup_queries(child)
    elif isinstance(value, list):
        for child in value:
            yield from lookup_queries(child)


class ScheduleGroupLookups(unittest.TestCase):
    def test_every_landingzone_lookup_selects_only_the_requested_group(self):
        for path in sorted((ROOT / "src").rglob("pipeline-content.json")):
            pipeline = json.loads(path.read_text(encoding="utf-8-sig"))
            for name, sql in lookup_queries(pipeline):
                source_type = re.search(r"DataSourceType='([^']+)'", sql, re.I)
                connection_type = re.search(r"ConnectionType='([^']+)'", sql, re.I)
                source_type = source_type[1] if source_type else "ASQL_01"
                connection_type = connection_type[1] if connection_type else "SQL"
                with closing(sqlite3.connect(":memory:")) as db:
                    db.execute(
                        "CREATE TABLE entities (EntityId INT, DataSourceType TEXT, "
                        "ConnectionType TEXT, WorkspaceGuid TEXT, ScheduleGroupName TEXT)"
                    )
                    db.executemany(
                        "INSERT INTO entities VALUES (?, ?, ?, ?, ?)",
                        [
                            (1, source_type, connection_type, WORKSPACE, "Default"),
                            (2, source_type, connection_type, WORKSPACE, "Default"),
                            (1, source_type, connection_type, WORKSPACE, "Ops'Hourly"),
                            (2, source_type, connection_type, WORKSPACE, "Nightly"),
                            (3, source_type, connection_type, "another-workspace", "Default"),
                            (3, source_type, connection_type, "another-workspace", "Ops'Hourly"),
                        ],
                    )
                    for group in ("Default", "Ops'Hourly", "Missing"):
                        with self.subTest(pipeline=path.parent.name, lookup=name, group=group):
                            query = self.render(sql, group)
                            rows = db.execute(query).fetchall()
                            if group == "Missing":
                                self.assertEqual(rows, [])
                            elif re.match(r"SELECT\s+\*", sql, re.I):
                                expected = [1, 2] if group == "Default" else [1]
                                self.assertEqual(sorted(row[0] for row in rows), expected)
                                self.assertTrue(all(row[3:] == (WORKSPACE, group) for row in rows))
                            else:
                                projected = connection_type if "distinct" in sql.lower() else source_type
                                self.assertEqual(rows, [(projected,)])

    def render(self, sql, group):
        sql = sql.replace(
            "@{pipeline().parameters.Data_WorkspaceGuid}", WORKSPACE
        )
        expression = (
            "@{replace(pipeline().parameters.ScheduleGroupName, '''', '''''')}"
        )
        sql = sql.replace(expression, group.replace("'", "''"))
        self.assertNotIn("@{", sql, "Unexpected or unescaped pipeline expression")
        # SQLite executes the lookup SELECTs, not the Fabric view's T-SQL body.
        return sql.replace(VIEW, "entities").replace("=N'", "='")


if __name__ == "__main__":
    unittest.main()
