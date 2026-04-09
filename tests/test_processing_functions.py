"""
Tests for functions defined in NB_FMD_PROCESSING_PARALLEL_MAIN and
NB_FMD_PROCESSING_LANDINGZONE_MAIN notebooks.

Tested functions:
  - format_guid
  - is_valid_guid
  - extract_ts_from_name
  - group_key
  - batched
"""

import ast
import pathlib
import uuid
import pytest
from datetime import datetime


# ---------------------------------------------------------------------------
# Helpers: load functions from notebook source without executing Fabric cells
# ---------------------------------------------------------------------------

def _extract_functions(nb_path: pathlib.Path, func_names: list[str]) -> dict:
    """
    Parse the notebook source and exec only the requested function definitions.
    Returns a namespace dict containing the requested functions.
    """
    src = nb_path.read_text()

    # Strip magic lines
    lines = [ln for ln in src.splitlines() if not ln.strip().startswith("%")]
    src = "\n".join(lines)

    tree = ast.parse(src)
    ns: dict = {"uuid": uuid, "datetime": datetime, "re": __import__("re")}

    # Collect function nodes by name
    wanted = set(func_names)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            func_lines = src.splitlines()[node.lineno - 1 : node.end_lineno]
            func_code = "\n".join(func_lines)
            exec(compile(func_code, str(nb_path), "exec"), ns)

    return ns


_PARALLEL_PATH = pathlib.Path(
    "src/NB_FMD_PROCESSING_PARALLEL_MAIN.Notebook/notebook-content.py"
)
_LANDINGZONE_PATH = pathlib.Path(
    "src/NB_FMD_PROCESSING_LANDINGZONE_MAIN.Notebook/notebook-content.py"
)

_parallel_ns = _extract_functions(
    _PARALLEL_PATH,
    ["format_guid", "is_valid_guid", "extract_ts_from_name", "group_key", "batched"],
)
_lz_ns = _extract_functions(
    _LANDINGZONE_PATH,
    ["format_guid", "is_valid_guid"],
)

format_guid = _parallel_ns["format_guid"]
is_valid_guid = _parallel_ns["is_valid_guid"]
extract_ts_from_name = _parallel_ns["extract_ts_from_name"]
group_key = _parallel_ns["group_key"]
batched = _parallel_ns["batched"]

# Verify landing-zone versions are identical in behaviour
format_guid_lz = _lz_ns["format_guid"]
is_valid_guid_lz = _lz_ns["is_valid_guid"]


# ---------------------------------------------------------------------------
# Tests: format_guid
# ---------------------------------------------------------------------------


class TestFormatGuid:

    def test_32_char_hex_gets_hyphens_inserted(self):
        raw = "12345678123412341234123456789012"
        expected = "12345678-1234-1234-1234-123456789012"
        assert format_guid(raw) == expected

    def test_already_hyphenated_returned_unchanged(self):
        guid = "12345678-1234-1234-1234-123456789012"
        assert format_guid(guid) == guid

    def test_short_string_returned_unchanged(self):
        short = "abc123"
        assert format_guid(short) == short

    def test_31_char_string_returned_unchanged(self):
        s = "a" * 31
        assert format_guid(s) == s

    def test_33_char_string_returned_unchanged(self):
        s = "a" * 33
        assert format_guid(s) == s

    def test_hyphen_positions_correct(self):
        raw = "aabbccdd" + "eeff" + "1122" + "3344" + "556677889900"
        result = format_guid(raw)
        parts = result.split("-")
        assert len(parts) == 5
        assert len(parts[0]) == 8
        assert len(parts[1]) == 4
        assert len(parts[2]) == 4
        assert len(parts[3]) == 4
        assert len(parts[4]) == 12

    def test_empty_string_returned_unchanged(self):
        assert format_guid("") == ""

    def test_parallel_and_landingzone_implementations_agree(self):
        raw = "12345678123412341234123456789012"
        assert format_guid(raw) == format_guid_lz(raw)


# ---------------------------------------------------------------------------
# Tests: is_valid_guid
# ---------------------------------------------------------------------------


class TestIsValidGuid:

    def test_valid_lowercase_uuid_returns_true(self):
        valid = str(uuid.uuid4())
        assert is_valid_guid(valid) is True

    def test_uppercase_uuid_returns_false(self):
        # str(uuid.UUID(...)) is always lowercase; uppercase won't match
        upper = str(uuid.uuid4()).upper()
        assert is_valid_guid(upper) is False

    def test_all_zeros_uuid_returns_true(self):
        assert is_valid_guid("00000000-0000-0000-0000-000000000000") is True

    def test_invalid_string_returns_false(self):
        assert is_valid_guid("not-a-guid") is False

    def test_empty_string_returns_false(self):
        assert is_valid_guid("") is False

    def test_32_char_hex_without_hyphens_returns_false(self):
        # uuid.UUID can parse it but str() will differ (adds hyphens)
        raw = uuid.uuid4().hex  # 32 lowercase hex chars, no hyphens
        assert is_valid_guid(raw) is False

    def test_random_text_returns_false(self):
        assert is_valid_guid("hello-world") is False

    def test_parallel_and_landingzone_implementations_agree(self):
        valid = str(uuid.uuid4())
        assert is_valid_guid(valid) == is_valid_guid_lz(valid)
        assert is_valid_guid("bad") == is_valid_guid_lz("bad")


# ---------------------------------------------------------------------------
# Tests: extract_ts_from_name
# ---------------------------------------------------------------------------


class TestExtractTsFromName:

    def test_valid_filename_extracts_datetime(self):
        name = "Sales_Invoices_202601311402.parquet"
        result = extract_ts_from_name(name)
        assert result == datetime(2026, 1, 31, 14, 2)

    def test_another_valid_filename(self):
        name = "Customers_Data_202312011230.parquet"
        result = extract_ts_from_name(name)
        assert result == datetime(2023, 12, 1, 12, 30)

    def test_filename_without_timestamp_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid filename timestamp"):
            extract_ts_from_name("plain_file.parquet")

    def test_filename_with_wrong_extension_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid filename timestamp"):
            extract_ts_from_name("Sales_202601311402.csv")

    def test_filename_with_11_digit_ts_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid filename timestamp"):
            extract_ts_from_name("Sales_20260131140.parquet")

    def test_filename_with_13_digit_ts_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid filename timestamp"):
            extract_ts_from_name("Sales_2026013114020.parquet")

    def test_exact_12_digit_ts_at_end_required(self):
        # Digits not immediately before .parquet → no match
        with pytest.raises(ValueError, match="Invalid filename timestamp"):
            extract_ts_from_name("202601311402_Sales.parquet")


# ---------------------------------------------------------------------------
# Tests: group_key
# ---------------------------------------------------------------------------


class TestGroupKey:

    def _make_item(self, namespace, schema, name):
        return {
            "params": {
                "DataSourceNamespace": namespace,
                "TargetSchema": schema,
                "TargetName": name,
            }
        }

    def test_returns_tuple_of_three(self):
        item = self._make_item("NS", "dbo", "MyTable")
        result = group_key(item)
        assert result == ("NS", "dbo", "MyTable")

    def test_same_group_key_for_same_params(self):
        item1 = self._make_item("NS", "dbo", "MyTable")
        item2 = self._make_item("NS", "dbo", "MyTable")
        assert group_key(item1) == group_key(item2)

    def test_different_namespace_gives_different_key(self):
        item1 = self._make_item("NS1", "dbo", "MyTable")
        item2 = self._make_item("NS2", "dbo", "MyTable")
        assert group_key(item1) != group_key(item2)

    def test_different_schema_gives_different_key(self):
        item1 = self._make_item("NS", "schema_a", "MyTable")
        item2 = self._make_item("NS", "schema_b", "MyTable")
        assert group_key(item1) != group_key(item2)

    def test_missing_params_returns_none_values(self):
        item = {"params": {}}
        result = group_key(item)
        assert result == (None, None, None)


# ---------------------------------------------------------------------------
# Tests: batched
# ---------------------------------------------------------------------------


class TestBatched:

    def test_empty_list_yields_nothing(self):
        assert list(batched([], 5, 10)) == []

    def test_list_smaller_than_first_size_yields_one_batch(self):
        result = list(batched([1, 2, 3], 10, 5))
        assert result == [[1, 2, 3]]

    def test_first_batch_uses_first_size(self):
        items = list(range(15))
        batches = list(batched(items, 5, 3))
        assert batches[0] == list(range(5))

    def test_subsequent_batches_use_default_size(self):
        items = list(range(11))
        batches = list(batched(items, 5, 3))
        assert batches[0] == [0, 1, 2, 3, 4]
        assert batches[1] == [5, 6, 7]
        assert batches[2] == [8, 9, 10]

    def test_all_items_covered(self):
        items = list(range(20))
        batches = list(batched(items, 7, 4))
        flat = [x for batch in batches for x in batch]
        assert flat == items

    def test_single_item_list(self):
        result = list(batched([42], 5, 3))
        assert result == [[42]]

    def test_exactly_first_size_items(self):
        items = list(range(5))
        batches = list(batched(items, 5, 3))
        assert batches == [list(range(5))]

    def test_first_plus_one_items_gives_two_batches(self):
        items = list(range(6))
        batches = list(batched(items, 5, 3))
        assert len(batches) == 2
        assert batches[0] == [0, 1, 2, 3, 4]
        assert batches[1] == [5]
