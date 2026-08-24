"""Strict JSON parsing helpers shared by persisted restore contracts."""

from typing import Any, Dict, Iterable, Tuple
import json


def reject_duplicate_pairs(pairs: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key: {0}".format(key))
        result[key] = value
    return result


def reject_nonfinite_constant(value: str) -> None:
    raise ValueError("non-finite JSON number: {0}".format(value))


def load_strict_json(handle: Any) -> Any:
    return json.load(
        handle,
        object_pairs_hook=reject_duplicate_pairs,
        parse_constant=reject_nonfinite_constant,
    )


def loads_strict_json(value: str) -> Any:
    """Parse an already bounded JSON string with the same strict policy."""

    return json.loads(
        value,
        object_pairs_hook=reject_duplicate_pairs,
        parse_constant=reject_nonfinite_constant,
    )
