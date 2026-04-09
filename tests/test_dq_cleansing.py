"""
Tests for NB_FMD_DQ_CLEANSING notebook functions.

Tested functions:
  - register_cleansing_function
  - normalize_cleansing_rules
  - dynamic_call_cleansing_function
  - handle_cleansing_functions
  - normalize_text  (requires PySpark)
  - fill_nulls      (requires PySpark)
  - parse_datetime  (requires PySpark)
"""

import ast
import json
import pathlib
import types
import pytest

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)


# ---------------------------------------------------------------------------
# Helpers: load notebook source, stripping Fabric-only cells
# ---------------------------------------------------------------------------

_NB_PATH = pathlib.Path(
    "src/NB_FMD_DQ_CLEANSING.Notebook/notebook-content.py"
)


def _strip_magic_lines(src: str) -> str:
    """Remove IPython magic lines (e.g. %run ...) from source."""
    lines = [ln for ln in src.splitlines() if not ln.strip().startswith("%")]
    return "\n".join(lines)


def _build_cleansing_module():
    """
    Execute the DQ cleansing notebook source in an isolated namespace,
    providing minimal stubs for Fabric dependencies.
    """
    src = _NB_PATH.read_text()
    src = _strip_magic_lines(src)

    # Provide stubs so the module-level code executes without errors
    ns: dict = {
        # pyspark imports used at module level
        "DataFrame": DataFrame,
        # json is used in normalize_cleansing_rules
        "json": json,
    }

    # Import PySpark functions that the notebook uses at module level
    from pyspark.sql.functions import (
        col, trim, regexp_replace, lower, upper, initcap,
        when, length, lit, coalesce, to_date, to_timestamp,
    )
    ns.update({
        "col": col, "trim": trim, "regexp_replace": regexp_replace,
        "lower": lower, "upper": upper, "initcap": initcap,
        "when": when, "length": length, "lit": lit,
        "coalesce": coalesce, "to_date": to_date, "to_timestamp": to_timestamp,
    })

    exec(compile(src, str(_NB_PATH), "exec"), ns)
    return ns


_MODULE = _build_cleansing_module()

register_cleansing_function = _MODULE["register_cleansing_function"]
_REGISTRY = _MODULE["_CLEANSING_FUNCTION_REGISTRY"]
normalize_cleansing_rules = _MODULE["normalize_cleansing_rules"]
dynamic_call_cleansing_function = _MODULE["dynamic_call_cleansing_function"]
handle_cleansing_functions = _MODULE["handle_cleansing_functions"]
normalize_text = _MODULE["normalize_text"]
fill_nulls = _MODULE["fill_nulls"]
parse_datetime = _MODULE["parse_datetime"]


# ---------------------------------------------------------------------------
# Tests: register_cleansing_function
# ---------------------------------------------------------------------------


class TestRegisterCleansingFunction:

    def setup_method(self):
        """Back up and restore the registry around each test."""
        self._backup = dict(_REGISTRY)

    def teardown_method(self):
        _REGISTRY.clear()
        _REGISTRY.update(self._backup)

    def test_register_valid_function(self):
        def my_fn(df, cols, args):
            return df
        register_cleansing_function("my_fn", my_fn)
        assert "my_fn" in _REGISTRY

    def test_register_strips_whitespace_in_name(self):
        def my_fn2(df, cols, args):
            return df
        register_cleansing_function("  my_fn2  ", my_fn2)
        assert "my_fn2" in _REGISTRY
        assert "  my_fn2  " not in _REGISTRY

    def test_non_string_name_raises_type_error(self):
        with pytest.raises(TypeError, match="name must be a string"):
            register_cleansing_function(123, lambda df, c, a: df)

    def test_empty_string_name_raises_value_error(self):
        with pytest.raises(ValueError, match="non-empty"):
            register_cleansing_function("   ", lambda df, c, a: df)

    def test_non_callable_raises_type_error(self):
        with pytest.raises(TypeError, match="must be callable"):
            register_cleansing_function("bad_fn", "not_a_function")

    def test_duplicate_registration_raises_value_error(self):
        def fn(df, cols, args):
            return df
        register_cleansing_function("dup_fn", fn)
        with pytest.raises(ValueError, match="already registered"):
            register_cleansing_function("dup_fn", fn)

    def test_overwrite_true_replaces_function(self):
        def fn_v1(df, cols, args):
            return df
        def fn_v2(df, cols, args):
            return df
        register_cleansing_function("overwrite_fn", fn_v1)
        register_cleansing_function("overwrite_fn", fn_v2, overwrite=True)
        assert _REGISTRY["overwrite_fn"] is fn_v2

    def test_builtin_functions_registered(self):
        """normalize_text, fill_nulls, parse_datetime are pre-registered."""
        assert "normalize_text" in _REGISTRY
        assert "fill_nulls" in _REGISTRY
        assert "parse_datetime" in _REGISTRY


# ---------------------------------------------------------------------------
# Tests: normalize_cleansing_rules
# ---------------------------------------------------------------------------


class TestNormalizeCleansingRules:

    def test_none_returns_empty_list(self):
        assert normalize_cleansing_rules(None) == []

    def test_empty_string_returns_empty_list(self):
        assert normalize_cleansing_rules("") == []
        assert normalize_cleansing_rules("   ") == []

    def test_json_string_list_of_dicts(self):
        rules = [{"function": "to_upper", "columns": "name"}]
        result = normalize_cleansing_rules(json.dumps(rules))
        assert result == rules

    def test_json_string_single_dict(self):
        rule = {"function": "to_upper", "columns": "name"}
        result = normalize_cleansing_rules(json.dumps(rule))
        assert result == [rule]

    def test_single_dict_wrapped_in_list(self):
        rule = {"function": "to_upper", "columns": "name"}
        result = normalize_cleansing_rules(rule)
        assert result == [rule]

    def test_list_returned_unchanged(self):
        rules = [{"function": "a"}, {"function": "b"}]
        result = normalize_cleansing_rules(rules)
        assert result == rules

    def test_invalid_type_raises_type_error(self):
        with pytest.raises(TypeError, match="must be a list"):
            normalize_cleansing_rules(42)

    def test_list_with_non_dict_raises_type_error(self):
        with pytest.raises(TypeError, match="not a dict"):
            normalize_cleansing_rules(["not_a_dict"])

    def test_empty_list_returns_empty_list(self):
        assert normalize_cleansing_rules([]) == []


# ---------------------------------------------------------------------------
# Tests: dynamic_call_cleansing_function
# ---------------------------------------------------------------------------


class TestDynamicCallCleansingFunction:

    def setup_method(self):
        self._backup = dict(_REGISTRY)

    def teardown_method(self):
        _REGISTRY.clear()
        _REGISTRY.update(self._backup)

    def test_calls_registered_function(self, spark):
        sentinel = []

        def my_fn(df, cols, args):
            sentinel.append((cols, args))
            return df

        _REGISTRY["sentinel_fn"] = my_fn
        df = spark.createDataFrame([("a",)], ["col1"])
        dynamic_call_cleansing_function(df, "sentinel_fn", ["col1"], {"x": 1})
        assert sentinel == [(["col1"], {"x": 1})]

    def test_unregistered_function_raises_value_error(self, spark):
        df = spark.createDataFrame([("a",)], ["col1"])
        with pytest.raises(ValueError, match="not a registered"):
            dynamic_call_cleansing_function(df, "nonexistent_fn", ["col1"], {})

    def test_error_message_lists_available_functions(self, spark):
        df = spark.createDataFrame([("a",)], ["col1"])
        with pytest.raises(ValueError) as exc_info:
            dynamic_call_cleansing_function(df, "nonexistent_fn", ["col1"], {})
        msg = str(exc_info.value)
        assert "normalize_text" in msg or "Available functions" in msg

    def test_function_exception_wrapped_in_value_error(self, spark):
        def bad_fn(df, cols, args):
            raise RuntimeError("boom")

        _REGISTRY["bad_fn"] = bad_fn
        df = spark.createDataFrame([("a",)], ["col1"])
        with pytest.raises(ValueError, match="boom"):
            dynamic_call_cleansing_function(df, "bad_fn", ["col1"], {})


# ---------------------------------------------------------------------------
# Tests: handle_cleansing_functions
# ---------------------------------------------------------------------------


class TestHandleCleansingFunctions:

    def test_empty_rules_returns_df_unchanged(self, spark):
        df = spark.createDataFrame([("hello",)], ["name"])
        result = handle_cleansing_functions(df, [])
        assert result.collect() == df.collect()

    def test_none_rules_returns_df_unchanged(self, spark):
        df = spark.createDataFrame([("hello",)], ["name"])
        result = handle_cleansing_functions(df, None)
        assert result.collect() == df.collect()

    def test_rule_missing_function_key_skips_gracefully(self, spark, capsys):
        df = spark.createDataFrame([("hello",)], ["name"])
        result = handle_cleansing_functions(df, [{"columns": "name"}])
        assert result.collect() == df.collect()

    def test_applies_registered_cleansing_function(self, spark):
        df = spark.createDataFrame([("  hello  ",)], ["name"])
        rules = [{"function": "normalize_text", "columns": "name", "parameters": {}}]
        result = handle_cleansing_functions(df, rules)
        rows = result.collect()
        assert rows[0]["name"] == "hello"

    def test_applies_multiple_rules_in_order(self, spark):
        call_order = []

        def fn_a(df, cols, args):
            call_order.append("a")
            return df

        def fn_b(df, cols, args):
            call_order.append("b")
            return df

        _REGISTRY["fn_a"] = fn_a
        _REGISTRY["fn_b"] = fn_b
        try:
            df = spark.createDataFrame([("x",)], ["col1"])
            rules = [
                {"function": "fn_a", "columns": "col1"},
                {"function": "fn_b", "columns": "col1"},
            ]
            handle_cleansing_functions(df, rules)
            assert call_order == ["a", "b"]
        finally:
            _REGISTRY.pop("fn_a", None)
            _REGISTRY.pop("fn_b", None)

    def test_json_string_rules_parsed_and_applied(self, spark):
        df = spark.createDataFrame([("  world  ",)], ["val"])
        rules_json = json.dumps([{"function": "normalize_text", "columns": "val", "parameters": {}}])
        result = handle_cleansing_functions(df, rules_json)
        assert result.collect()[0]["val"] == "world"


# ---------------------------------------------------------------------------
# Tests: normalize_text (PySpark)
# ---------------------------------------------------------------------------


class TestNormalizeText:

    def _make_df(self, spark, values, col_name="text"):
        return spark.createDataFrame([(v,) for v in values], [col_name])

    def test_trims_leading_trailing_whitespace(self, spark):
        df = self._make_df(spark, ["  hello  "])
        result = normalize_text(df, ["text"], {})
        assert result.collect()[0]["text"] == "hello"

    def test_collapses_multiple_spaces(self, spark):
        df = self._make_df(spark, ["hello   world"])
        result = normalize_text(df, ["text"], {"collapse_spaces": True})
        assert result.collect()[0]["text"] == "hello world"

    def test_case_lower(self, spark):
        df = self._make_df(spark, ["HELLO"])
        result = normalize_text(df, ["text"], {"case": "lower"})
        assert result.collect()[0]["text"] == "hello"

    def test_case_upper(self, spark):
        df = self._make_df(spark, ["hello"])
        result = normalize_text(df, ["text"], {"case": "upper"})
        assert result.collect()[0]["text"] == "HELLO"

    def test_case_title(self, spark):
        df = self._make_df(spark, ["hello world"])
        result = normalize_text(df, ["text"], {"case": "title"})
        assert result.collect()[0]["text"] == "Hello World"

    def test_empty_string_becomes_null_by_default(self, spark):
        df = self._make_df(spark, [""])
        result = normalize_text(df, ["text"], {})
        assert result.collect()[0]["text"] is None

    def test_whitespace_only_becomes_null(self, spark):
        df = self._make_df(spark, ["   "])
        result = normalize_text(df, ["text"], {})
        assert result.collect()[0]["text"] is None

    def test_empty_as_null_false_preserves_empty(self, spark):
        df = self._make_df(spark, [""])
        result = normalize_text(df, ["text"], {"empty_as_null": False})
        assert result.collect()[0]["text"] == ""

    def test_multiple_columns(self, spark):
        schema = StructType([
            StructField("a", StringType()),
            StructField("b", StringType()),
        ])
        df = spark.createDataFrame([("  foo  ", "  BAR  ")], schema)
        result = normalize_text(df, ["a", "b"], {"case": "lower"})
        row = result.collect()[0]
        assert row["a"] == "foo"
        assert row["b"] == "bar"

    def test_none_case_does_not_change_case(self, spark):
        df = self._make_df(spark, ["Hello World"])
        result = normalize_text(df, ["text"], {"case": None})
        assert result.collect()[0]["text"] == "Hello World"


# ---------------------------------------------------------------------------
# Tests: fill_nulls (PySpark)
# ---------------------------------------------------------------------------


class TestFillNulls:

    def test_fill_null_string_with_default_string(self, spark):
        schema = StructType([StructField("name", StringType())])
        df = spark.createDataFrame([(None,)], schema)
        result = fill_nulls(df, ["name"], {"default_string": "N/A"})
        assert result.collect()[0]["name"] == "N/A"

    def test_non_null_string_unchanged(self, spark):
        schema = StructType([StructField("name", StringType())])
        df = spark.createDataFrame([("Alice",)], schema)
        result = fill_nulls(df, ["name"], {"default_string": "N/A"})
        assert result.collect()[0]["name"] == "Alice"

    def test_fill_null_integer_with_default_numeric(self, spark):
        schema = StructType([StructField("score", IntegerType())])
        df = spark.createDataFrame([(None,)], schema)
        result = fill_nulls(df, ["score"], {"default_numeric": 0})
        assert result.collect()[0]["score"] == 0

    def test_per_column_default_overrides_type_default(self, spark):
        schema = StructType([StructField("name", StringType())])
        df = spark.createDataFrame([(None,)], schema)
        result = fill_nulls(df, ["name"], {"defaults": {"name": "specific"}, "default_string": "generic"})
        assert result.collect()[0]["name"] == "specific"

    def test_unknown_column_skipped_gracefully(self, spark):
        schema = StructType([StructField("name", StringType())])
        df = spark.createDataFrame([("Alice",)], schema)
        result = fill_nulls(df, ["nonexistent"], {"default_string": "N/A"})
        # Should not raise; df unchanged
        assert result.collect()[0]["name"] == "Alice"

    def test_fill_null_double_with_default_numeric(self, spark):
        schema = StructType([StructField("val", DoubleType())])
        df = spark.createDataFrame([(None,)], schema)
        result = fill_nulls(df, ["val"], {"default_numeric": 0.0})
        assert result.collect()[0]["val"] == 0.0

    def test_no_defaults_provided_leaves_nulls(self, spark):
        schema = StructType([StructField("name", StringType())])
        df = spark.createDataFrame([(None,)], schema)
        result = fill_nulls(df, ["name"], {})
        assert result.collect()[0]["name"] is None


# ---------------------------------------------------------------------------
# Tests: parse_datetime (PySpark)
# ---------------------------------------------------------------------------


class TestParseDatetime:

    def test_parse_date_default_format(self, spark):
        schema = StructType([StructField("dt", StringType())])
        df = spark.createDataFrame([("2024-01-15",)], schema)
        result = parse_datetime(df, ["dt"], {"formats": ["yyyy-MM-dd"]})
        row = result.collect()[0]
        import datetime
        assert row["dt"] == datetime.date(2024, 1, 15)

    def test_parse_date_multiple_formats_first_match(self, spark):
        schema = StructType([StructField("dt", StringType())])
        df = spark.createDataFrame([("15/01/2024",)], schema)
        result = parse_datetime(
            df, ["dt"], {"formats": ["yyyy-MM-dd", "dd/MM/yyyy"]}
        )
        row = result.collect()[0]
        import datetime
        assert row["dt"] == datetime.date(2024, 1, 15)

    def test_parse_timestamp(self, spark):
        schema = StructType([StructField("ts", StringType())])
        df = spark.createDataFrame([("2024-01-15 10:30:00",)], schema)
        result = parse_datetime(
            df, ["ts"],
            {"target_type": "timestamp", "formats": ["yyyy-MM-dd HH:mm:ss"]}
        )
        row = result.collect()[0]
        assert row["ts"] is not None

    def test_into_writes_to_new_column(self, spark):
        schema = StructType([StructField("raw_date", StringType())])
        df = spark.createDataFrame([("2024-01-15",)], schema)
        result = parse_datetime(
            df, ["raw_date"],
            {"formats": ["yyyy-MM-dd"], "into": "parsed_date"}
        )
        assert "parsed_date" in result.columns

    def test_keep_original_false_drops_source_column(self, spark):
        schema = StructType([StructField("raw_date", StringType())])
        df = spark.createDataFrame([("2024-01-15",)], schema)
        result = parse_datetime(
            df, ["raw_date"],
            {"formats": ["yyyy-MM-dd"], "into": "parsed_date", "keep_original": False}
        )
        assert "raw_date" not in result.columns
        assert "parsed_date" in result.columns

    def test_invalid_date_produces_null(self, spark):
        schema = StructType([StructField("dt", StringType())])
        df = spark.createDataFrame([("not-a-date",)], schema)
        result = parse_datetime(df, ["dt"], {"formats": ["yyyy-MM-dd"]})
        assert result.collect()[0]["dt"] is None

    def test_multiple_columns_parsed(self, spark):
        schema = StructType([
            StructField("d1", StringType()),
            StructField("d2", StringType()),
        ])
        df = spark.createDataFrame([("2024-01-15", "2024-06-30")], schema)
        result = parse_datetime(df, ["d1", "d2"], {"formats": ["yyyy-MM-dd"]})
        import datetime
        row = result.collect()[0]
        assert row["d1"] == datetime.date(2024, 1, 15)
        assert row["d2"] == datetime.date(2024, 6, 30)
