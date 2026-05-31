"""Scan ``WORKFLOW_DIR`` and parse each ``*.yaml`` / ``*.yml`` into a Workflow.

Errors are *collected*, not raised — a single broken file MUST NOT stop the
loader from emitting the workflows in the other files (config-loader spec
"错误隔离" requirement). Each error keeps the file path, a Pydantic-style
field path ("" for file-level problems) and a human-readable reason.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml
from pydantic import ValidationError

from tg_conductor.workflows.schema import Workflow


@dataclass
class FileError:
    path: Path
    field_path: str  # "" for file-level errors (YAML syntax, wrong top-level type)
    message: str

    def format(self) -> str:
        loc = f"{self.path}"
        if self.field_path:
            loc += f" [{self.field_path}]"
        return f"{loc}: {self.message}"


@dataclass
class LoadResult:
    workflows: list[tuple[Path, Workflow]] = field(default_factory=list)
    errors: list[FileError] = field(default_factory=list)

    def ok(self) -> bool:
        return not self.errors


def load_workflows_from_dir(directory: Path) -> LoadResult:
    """Walk ``directory`` non-recursively, parse each YAML file, collect errors.

    The path under which a workflow was found is returned alongside the parsed
    model so the caller can produce diagnostics that point at the source file.
    The workflow's own ``name`` field — not the file name — is the canonical
    identifier (spec ``config-loader/YAML 文件布局``).
    """
    result = LoadResult()
    if not directory.exists():
        # Missing directory is not itself an error: empty config is valid.
        return result

    for path in sorted(directory.iterdir()):
        if not path.is_file() or path.suffix.lower() not in {".yaml", ".yml"}:
            continue
        _parse_one(path, result)
    return result


def _parse_one(path: Path, result: LoadResult) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        result.errors.append(FileError(path, "", f"read failed: {exc}"))
        return

    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        result.errors.append(FileError(path, "", f"YAML parse error: {exc}"))
        return

    if data is None:
        result.errors.append(FileError(path, "", "file is empty"))
        return
    if isinstance(data, list):
        result.errors.append(
            FileError(
                path,
                "",
                "each YAML file must describe exactly one Workflow, got a list",
            )
        )
        return
    if not isinstance(data, dict):
        result.errors.append(
            FileError(
                path,
                "",
                f"top-level must be a mapping, got {type(data).__name__}",
            )
        )
        return

    try:
        workflow = Workflow.model_validate(data)
    except ValidationError as exc:
        for err in exc.errors():
            loc = ".".join(str(p) for p in err.get("loc", ()))
            result.errors.append(FileError(path, loc, err.get("msg", "invalid value")))
        return

    result.workflows.append((path, workflow))
