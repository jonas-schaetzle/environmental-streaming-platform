import os
import platform
import shutil
import subprocess
from pathlib import Path


SUPPORTED_JAVA_VERSIONS = ("21", "17")
HOMEBREW_JDK_HOMES = (
    Path("/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"),
    Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
    Path("/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home"),
    Path("/usr/local/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"),
    Path("/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
    Path("/usr/local/opt/openjdk/libexec/openjdk.jdk/Contents/Home"),
)


def configure_java_runtime() -> str | None:
    java_home = os.environ.get("JAVA_HOME")

    if java_home and _java_binary_exists(Path(java_home)):
        return java_home

    if _java_on_path_works():
        return java_home

    discovered_java_home = discover_java_home()

    if discovered_java_home:
        os.environ["JAVA_HOME"] = str(discovered_java_home)
        os.environ["PATH"] = (
            f"{discovered_java_home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
        )
        return str(discovered_java_home)

    raise RuntimeError(
        "No supported Java runtime found. Spark 4.1 requires Java 17 or 21. "
        "Install one with `brew install openjdk@21` on macOS, or set JAVA_HOME "
        "to an existing JDK installation."
    )


def discover_java_home() -> Path | None:
    for version in SUPPORTED_JAVA_VERSIONS:
        java_home = _macos_java_home(version)

        if java_home and _java_binary_exists(java_home):
            return java_home

    for java_home in HOMEBREW_JDK_HOMES:
        if _java_binary_exists(java_home):
            return java_home

    return None


def _macos_java_home(version: str) -> Path | None:
    if platform.system() != "Darwin":
        return None

    result = subprocess.run(
        ["/usr/libexec/java_home", "-v", version],
        capture_output=True,
        check=False,
        text=True,
    )

    if result.returncode != 0:
        return None

    java_home = result.stdout.strip()

    return Path(java_home) if java_home else None


def _java_binary_exists(java_home: Path) -> bool:
    return (java_home / "bin" / "java").exists()


def _java_on_path_works() -> bool:
    if not shutil.which("java"):
        return False

    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            check=False,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False

    return result.returncode == 0
