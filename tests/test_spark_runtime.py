import os
from pathlib import Path

import pytest

from src import spark_runtime


@pytest.fixture(autouse=True)
def restore_java_environment() -> None:
    original_java_home = os.environ.get("JAVA_HOME")
    original_path = os.environ.get("PATH")

    yield

    if original_java_home is None:
        os.environ.pop("JAVA_HOME", None)
    else:
        os.environ["JAVA_HOME"] = original_java_home

    if original_path is None:
        os.environ.pop("PATH", None)
    else:
        os.environ["PATH"] = original_path


def test_configure_java_runtime_uses_existing_java_home(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    java_home = tmp_path / "jdk"
    java_bin = java_home / "bin"
    java_bin.mkdir(parents=True)
    (java_bin / "java").touch()
    monkeypatch.setenv("JAVA_HOME", str(java_home))
    monkeypatch.setattr(spark_runtime, "_java_on_path_works", lambda: False)

    result = spark_runtime.configure_java_runtime()

    assert result == str(java_home)


def test_configure_java_runtime_discovers_homebrew_jdk(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    java_home = tmp_path / "homebrew-jdk"
    java_bin = java_home / "bin"
    java_bin.mkdir(parents=True)
    (java_bin / "java").touch()
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setattr(spark_runtime, "_java_on_path_works", lambda: False)
    monkeypatch.setattr(spark_runtime, "_macos_java_home", lambda _: None)
    monkeypatch.setattr(spark_runtime, "HOMEBREW_JDK_HOMES", (java_home,))

    result = spark_runtime.configure_java_runtime()

    assert result == str(java_home)
    assert spark_runtime.os.environ["JAVA_HOME"] == str(java_home)
    assert spark_runtime.os.environ["PATH"].startswith(str(java_bin))


def test_configure_java_runtime_raises_clear_error_when_java_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setattr(spark_runtime, "_java_on_path_works", lambda: False)
    monkeypatch.setattr(spark_runtime, "_macos_java_home", lambda _: None)
    monkeypatch.setattr(spark_runtime, "HOMEBREW_JDK_HOMES", ())

    with pytest.raises(RuntimeError, match="Spark 4.1 requires Java"):
        spark_runtime.configure_java_runtime()
