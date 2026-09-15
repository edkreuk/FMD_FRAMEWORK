#!/usr/bin/env python3
"""
GUID-guard — handhaaft ADR-003 en ADR-009.

Het deployment-model leunt erop dat elke GUID in src/ een placeholder is die tijdens
deployment vervangen wordt. Staat een GUID niet in de vervang-tabel, dan wijst het
gedeployde artefact stilzwijgend naar een vreemde tenant.

Dit script faalt wanneer een GUID in SCAN_DIRS niet voorkomt in config/ en niet op de
allowlist staat.

Geen dependencies: draait op een kale python3.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Waar we op lekken scannen.
SCAN_DIRS = ["src", "setup", "wiki"]

# Waar bekende (te vervangen) GUID's vandaan komen. Elke GUID die hier ergens
# voorkomt geldt als gemapt — dat is precies de bestaande conventie.
CONFIG_DIRS = ["config"]

ALLOWLIST_FILE = REPO_ROOT / "config" / "guid_allowlist.txt"

# Bestandstypen die we openen. Binaire artefacten (.dacpac, .png) slaan we over.
TEXT_SUFFIXES = {".json", ".yaml", ".yml", ".py", ".sql", ".ipynb", ".md", ".txt", ".platform"}

GUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)

# Expliciet gemapt naar de doelworkspace door het deployment-script.
ALWAYS_ALLOWED = {"00000000-0000-0000-0000-000000000000"}


def iter_text_files(root: Path):
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if any(part in {".git", "node_modules", "__pycache__"} for part in path.parts):
            continue
        if path.suffix.lower() in TEXT_SUFFIXES or path.name == ".platform":
            yield path


def read(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return None


def collect_known_guids() -> set[str]:
    known: set[str] = set(ALWAYS_ALLOWED)
    for directory in CONFIG_DIRS:
        base = REPO_ROOT / directory
        if not base.exists():
            continue
        for path in iter_text_files(base):
            # De allowlist staat zelf in config/, maar telt niet als vervang-tabel:
            # anders zou een GUID erin ook zonder reden geaccepteerd worden.
            if path == ALLOWLIST_FILE:
                continue
            text = read(path)
            if text:
                known.update(g.lower() for g in GUID_RE.findall(text))
    return {g.lower() for g in known}


def load_allowlist() -> dict[str, str]:
    """Regels als:  <guid> — <reden>.  Regels zonder reden worden geweigerd."""
    allowed: dict[str, str] = {}
    if not ALLOWLIST_FILE.exists():
        return allowed
    for lineno, raw in enumerate(ALLOWLIST_FILE.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = GUID_RE.search(line)
        if not match:
            print(f"::warning file={ALLOWLIST_FILE},line={lineno}::regel zonder GUID genegeerd")
            continue
        guid = match.group(0).lower()
        reason = line[match.end():].lstrip(" \t-—:").strip()
        if not reason:
            print(
                f"::error file={ALLOWLIST_FILE},line={lineno}::"
                f"{guid} staat op de allowlist zonder reden"
            )
            sys.exit(2)
        allowed[guid] = reason
    return allowed


def main() -> int:
    known = collect_known_guids()
    allowlist = load_allowlist()
    permitted = known | set(allowlist)

    violations: list[tuple[Path, int, str]] = []

    for directory in SCAN_DIRS:
        base = REPO_ROOT / directory
        if not base.exists():
            continue
        for path in iter_text_files(base):
            text = read(path)
            if not text:
                continue
            for lineno, line in enumerate(text.splitlines(), 1):
                for guid in GUID_RE.findall(line):
                    if guid.lower() not in permitted:
                        violations.append((path.relative_to(REPO_ROOT), lineno, guid))

    if not violations:
        print(f"GUID-guard OK — {len(known)} gemapte GUID's, {len(allowlist)} op de allowlist.")
        return 0

    print(f"\nGUID-guard: {len(violations)} niet-gemapte GUID(s) gevonden.\n")
    for path, lineno, guid in violations:
        print(f"::error file={path},line={lineno}::niet-gemapte GUID {guid}")
        print(f"  {path}:{lineno}  {guid}")

    print(
        "\nElke GUID in src/, setup/ of wiki/ moet in config/ voorkomen, zodat"
        "\ndeploy_item() hem tijdens deployment vervangt. Twee manieren om dit op te lossen:"
        "\n"
        "\n  1. Registreer het item in config/item_config.yaml of config/item_deployment*.json"
        "\n     — dit is vrijwel altijd de juiste oplossing."
        "\n  2. Staat de GUID er bewust (voorbeeld in documentatie, comment), zet hem dan met"
        f"\n     een reden in {ALLOWLIST_FILE.relative_to(REPO_ROOT)}."
        "\n"
        "\nZie docs/adr/009-guid-guard.md."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
