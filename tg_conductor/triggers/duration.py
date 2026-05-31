"""Parse human-friendly duration strings (``"30m"`` / ``"1h30m"`` / ``"45s"``).

Schema validates the *format* in ``workflows.schema`` so by the time the
scheduler calls :func:`parse_duration` the input is well-formed; the
function still raises ``ValueError`` defensively if asked to parse junk.
"""

from __future__ import annotations

import re

_TOKEN_RE = re.compile(r"(\d+)\s*([smh])")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600}


def parse_duration(text: str) -> float:
    """Sum ``<int><unit>`` tokens into seconds. Unknown formats raise ``ValueError``."""
    total = 0
    matched_any = False
    for value, unit in _TOKEN_RE.findall(text):
        total += int(value) * _UNIT_SECONDS[unit]
        matched_any = True
    if not matched_any or total <= 0:
        raise ValueError(f"cannot parse duration: {text!r}")
    return float(total)
