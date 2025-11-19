"""
Utilidades de reporting y logging.

Funciones:
- write_report: guardar reportes en Markdown
- log_to_file: anexar eventos a un log de texto o JSONL
- save_error_rows: exportar filas inválidas a CSV
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Dict, Any, Optional


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def write_report(path: str | Path, title: str, body: str, metadata: Optional[Dict[str, Any]] = None) -> Path:
    p = Path(path)
    _ensure_parent(p)
    lines = [f"# {title}", "", body]
    if metadata:
        lines.append("\n\n---\n")
        lines.append("Metadata:")
        lines.append("")
        for k, v in metadata.items():
            lines.append(f"- {k}: {v}")
    p.write_text("\n".join(lines), encoding="utf-8")
    return p


def log_to_file(path: str | Path, message: Dict[str, Any] | str, jsonl: bool = False) -> Path:
    p = Path(path)
    _ensure_parent(p)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if jsonl:
        rec = {"ts": now, **(message if isinstance(message, dict) else {"msg": str(message)})}
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    else:
        with p.open("a", encoding="utf-8") as f:
            f.write(f"[{now}] {message if isinstance(message, str) else json.dumps(message, ensure_ascii=False)}\n")
    return p


def save_error_rows(path: str | Path, rows: Iterable[Dict[str, Any]], header: Optional[Iterable[str]] = None) -> Path:
    import csv
    p = Path(path)
    _ensure_parent(p)
    rows = list(rows)
    if not rows:
        return p
    if header is None:
        header = list(rows[0].keys())
    with p.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(header))
        writer.writeheader()
        writer.writerows(rows)
    return p
