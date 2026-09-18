# Copyright 2026 Query Farm LLC - https://query.farm

"""Importing vgi, and defining functions, must not touch the filesystem.

``Function.storage`` resolves the default SQLite store lazily, on first access.
``TableFunctionBase.__init_subclass__`` used to defeat that: it detected
abstract classes with a plain ``getattr`` over ``dir(cls)``, which invoked the
storage descriptor, which created ``vgi_storage.db`` under the user's state
directory. Because vgi defines subclasses of its own, a bare ``import vgi`` did
it — and failed outright wherever that directory was not writable, such as a
container running as a non-root user whose home belongs to root.

The import checks run in a subprocess with ``HOME`` and the XDG directories
pointed at an empty temporary directory, because the storage descriptor caches
its backend per process and the test process has usually resolved it already.
"""

import os
import subprocess
import sys
from abc import abstractmethod
from pathlib import Path

import pytest

from vgi.table_function import TableFunctionGenerator

#: Imports every function base class and defines a concrete table function, so
#: both vgi's own class definitions and a worker's go through __init_subclass__.
_IMPORT_AND_DEFINE = """
import pyarrow as pa
import vgi
import vgi.aggregate_function
import vgi.scalar_function
import vgi.table_in_out_function
from vgi.invocation import BindResponse
from vgi.table_function import TableFunctionGenerator


class Concrete(TableFunctionGenerator[None, None]):
    @classmethod
    def on_bind(cls, params):
        return BindResponse(output_schema=pa.schema([("n", pa.int64())]))

    @classmethod
    def process(cls, params, state, out):
        out.finish()


# The direct check, independent of where a platform keeps its state directory:
# reading through __dict__ does not invoke the descriptor.
from vgi.function import Function

assert Function.__dict__["storage"]._resolved is None, "storage was resolved during import"
"""


def _isolated_env(home: Path) -> dict[str, str]:
    """Environment whose every per-user directory lives under ``home``.

    ``VGI_*`` variables are dropped so the default file-backed SQLite store is
    the one in play; a test run configured with ``VGI_WORKER_SHARED_STORAGE``
    set to ``memory`` would otherwise pass without the fix.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith(("VGI_", "XDG_"))}
    env["HOME"] = str(home)
    for name in ("XDG_STATE_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_CONFIG_HOME"):
        env[name] = str(home / name.lower())
    # platformdirs asks Windows for its folders directly and ignores HOME and
    # XDG there, but honors these overrides.
    for name in ("LOCAL_APPDATA", "APPDATA"):
        env[f"WIN_PD_OVERRIDE_{name}"] = str(home / name.lower())
    return env


def _run(code: str, home: Path) -> subprocess.CompletedProcess[str]:
    """Run ``code`` in a fresh interpreter with per-user directories under ``home``."""
    return subprocess.run(
        [sys.executable, "-c", code],
        env=_isolated_env(home),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _files_under(root: Path) -> list[str]:
    """Every file below ``root``, relative to it."""
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


class TestImportWritesNothing:
    """The storage store is created when it is used, not when vgi is imported."""

    def test_import_and_define_create_no_files(self, tmp_path: Path) -> None:
        """Importing vgi and defining a function leaves the home directory empty."""
        result = _run(_IMPORT_AND_DEFINE, tmp_path)
        assert result.returncode == 0, result.stderr
        assert _files_under(tmp_path) == []

    @pytest.mark.skipif(
        sys.platform == "win32" or os.geteuid() == 0,
        reason="a read-only directory does not stop root, and POSIX modes do not apply on Windows",
    )
    def test_import_succeeds_when_home_is_not_writable(self, tmp_path: Path) -> None:
        """A read-only home — a container's non-root user, say — does not stop the import."""
        home = tmp_path / "home"
        home.mkdir()
        home.chmod(0o555)
        try:
            result = _run(_IMPORT_AND_DEFINE, home)
        finally:
            home.chmod(0o755)
        assert result.returncode == 0, result.stderr

    def test_storage_is_still_created_on_first_use(self, tmp_path: Path) -> None:
        """Laziness, not removal: touching Function.storage still creates the store."""
        result = _run("from vgi.function import Function\nFunction.storage", tmp_path)
        assert result.returncode == 0, result.stderr
        assert any(name.endswith("vgi_storage.db") for name in _files_under(tmp_path))


class _NotADataclass:
    """An argument type a concrete function would be rejected for."""


class TestAbstractDetection:
    """Reading attributes statically must still recognize every kind of abstract member.

    An abstract class skips validation of its argument type; a concrete one is
    rejected at class creation when that type is not a dataclass. So a class
    with an invalid argument type is defined successfully only if it is seen as
    abstract.
    """

    def test_a_concrete_class_is_validated(self) -> None:
        """Control: without an abstract member, the invalid argument type is rejected."""
        with pytest.raises(TypeError, match="must be a dataclass"):

            class Concrete(TableFunctionGenerator[_NotADataclass, None]):
                @classmethod
                def on_bind(cls, params):  # type: ignore[no-untyped-def]
                    raise NotImplementedError

                @classmethod
                def process(cls, params, state, out):  # type: ignore[no-untyped-def]
                    raise NotImplementedError

    def test_an_abstract_classmethod_marks_the_class_abstract(self) -> None:
        """An inherited-concrete class that adds an abstract classmethod skips validation."""

        class WithAbstractClassmethod(TableFunctionGenerator[_NotADataclass, None]):
            @classmethod
            def on_bind(cls, params):  # type: ignore[no-untyped-def]
                raise NotImplementedError

            @classmethod
            def process(cls, params, state, out):  # type: ignore[no-untyped-def]
                raise NotImplementedError

            @classmethod
            @abstractmethod
            def extra(cls) -> None:
                """Left for a subclass to implement."""

        assert WithAbstractClassmethod._setting_params == {}

    def test_an_abstract_staticmethod_marks_the_class_abstract(self) -> None:
        """An abstract staticmethod is recognized through its raw attribute too."""

        class WithAbstractStaticmethod(TableFunctionGenerator[_NotADataclass, None]):
            @classmethod
            def on_bind(cls, params):  # type: ignore[no-untyped-def]
                raise NotImplementedError

            @classmethod
            def process(cls, params, state, out):  # type: ignore[no-untyped-def]
                raise NotImplementedError

            @staticmethod
            @abstractmethod
            def extra() -> None:
                """Left for a subclass to implement."""

        assert WithAbstractStaticmethod._setting_params == {}

    def test_an_abstract_property_marks_the_class_abstract(self) -> None:
        """An abstract property is recognized through its raw attribute too."""

        class WithAbstractProperty(TableFunctionGenerator[_NotADataclass, None]):
            @classmethod
            def on_bind(cls, params):  # type: ignore[no-untyped-def]
                raise NotImplementedError

            @classmethod
            def process(cls, params, state, out):  # type: ignore[no-untyped-def]
                raise NotImplementedError

            @property
            @abstractmethod
            def extra(self) -> int:
                """Left for a subclass to implement."""

        assert WithAbstractProperty._setting_params == {}
