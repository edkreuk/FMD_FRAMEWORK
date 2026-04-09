"""
Tests for NB_FMD_UTILITY_FUNCTIONS notebook functions.

Tested functions:
  - build_exec_statement
"""

import pytest

# ---------------------------------------------------------------------------
# Import helpers: extract functions from the notebook source without executing
# the Fabric-specific cells (import struct/pyodbc, notebookutils, etc.)
# ---------------------------------------------------------------------------

def _load_build_exec_statement():
    """Parse and exec only the build_exec_statement cell."""
    import ast, pathlib

    src = pathlib.Path(
        "src/NB_FMD_UTILITY_FUNCTIONS.Notebook/notebook-content.py"
    ).read_text()

    # Find the function definition block
    tree = ast.parse(src)
    func_src_lines = []
    in_func = False
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "build_exec_statement":
            # Extract source lines for this function
            func_lines = src.splitlines()[node.lineno - 1 : node.end_lineno]
            func_src_lines = func_lines
            break

    func_code = "\n".join(func_src_lines)
    ns = {}
    exec(func_code, ns)
    return ns["build_exec_statement"]


build_exec_statement = _load_build_exec_statement()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBuildExecStatement:
    """Tests for build_exec_statement()."""

    def test_no_params_returns_bare_exec(self):
        result = build_exec_statement("myproc")
        assert result == "EXEC myproc"

    def test_single_string_param(self):
        result = build_exec_statement("myproc", name="Alice")
        assert result == "EXEC myproc, @name='Alice'"

    def test_single_integer_param(self):
        result = build_exec_statement("myproc", count=42)
        assert result == "EXEC myproc, @count=42"

    def test_single_float_param(self):
        result = build_exec_statement("myproc", ratio=3.14)
        assert result == "EXEC myproc, @ratio=3.14"

    def test_none_param_is_excluded(self):
        result = build_exec_statement("myproc", name="Alice", missing=None)
        assert "@missing" not in result
        assert "@name='Alice'" in result

    def test_all_none_params_gives_bare_exec(self):
        result = build_exec_statement("myproc", a=None, b=None)
        assert result == "EXEC myproc"

    def test_multiple_params(self):
        result = build_exec_statement("myproc", first="x", second=10)
        assert result.startswith("EXEC myproc, ")
        assert "@first='x'" in result
        assert "@second=10" in result

    def test_string_param_uses_single_quotes(self):
        result = build_exec_statement("myproc", status="active")
        assert "@status='active'" in result
        assert '"' not in result.split("@status=")[1].split(",")[0]

    def test_mixed_none_and_non_none(self):
        result = build_exec_statement("myproc", present="yes", absent=None)
        assert "@present='yes'" in result
        assert "@absent" not in result

    def test_boolean_param(self):
        # Booleans are not strings, so they use the non-string path
        result = build_exec_statement("myproc", flag=True)
        assert "@flag=True" in result

    def test_proc_name_preserved(self):
        proc = "[dbo].[my_stored_procedure]"
        result = build_exec_statement(proc)
        assert proc in result
