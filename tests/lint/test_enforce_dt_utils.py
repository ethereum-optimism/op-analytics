import os
import ast
import fnmatch
import pytest

# List of forbidden imports
FORBIDDEN_IMPORTS = ["datetime"]  # Add any other libraries here

INCLUDED_FILES = [
    "packages/op_datasets/**/*.py",
    "src/op_analytics/**/*.py",
]  # Include specific files or patterns to be checked

EXCLUDED_FILES = [
    "src/op_analytics/cli/subcommands/pulls/github_analytics.py"  # Todo: Remove this once we start using the new utils
]  # Exclude specific files or patterns from being checked


def get_all_python_files(directory):
    """Recursively get all Python files in a directory."""
    python_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                python_files.append(os.path.join(root, file))
    return python_files


def is_included(file_path):
    """Check if the given file path is included based on patterns or folder rules."""
    for pattern in INCLUDED_FILES:
        if fnmatch.fnmatch(file_path, pattern):
            return True
        if os.path.isdir(pattern) and file_path.startswith(pattern):
            return True
    return False


def is_excluded(file_path):
    """Check if the given file path is excluded based on patterns or folder rules."""
    for pattern in EXCLUDED_FILES:
        if fnmatch.fnmatch(file_path, pattern):
            return True
        if os.path.isdir(pattern) and file_path.startswith(pattern):
            return True
    return False


def check_for_forbidden_imports(file_path):
    """Check if the given file contains forbidden imports."""
    with open(file_path, "r", encoding="utf-8") as file:
        tree = ast.parse(file.read(), filename=file_path)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in FORBIDDEN_IMPORTS:
                    return alias.name
        elif isinstance(node, ast.ImportFrom):
            if node.module in FORBIDDEN_IMPORTS:
                return node.module
    return None


@pytest.mark.parametrize("file_path", get_all_python_files("src/op_analytics"))
def test_forbidden_imports(file_path):
    """Test to ensure forbidden imports are not used in the codebase, except in excluded files."""
    if not is_included(file_path) or is_excluded(file_path):
        return
        # pytest.skip(f"{file_path} is not in the included files list or is in the excluded files list.")

    forbidden_import = check_for_forbidden_imports(file_path)
    if forbidden_import:
        pytest.fail(f"Forbidden import '{forbidden_import}' found in {file_path}")
