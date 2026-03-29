"""
Run golden natural-language query cases against the API.
"""
import argparse
import json
import re
import sys
from pathlib import Path

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:38018")
    parser.add_argument("--api-key", default="")
    parser.add_argument(
        "--cases",
        default=str(Path(__file__).with_name("golden_queries.json")),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cases = json.loads(Path(args.cases).read_text(encoding="utf-8"))
    headers = {"Content-Type": "application/json"}
    if args.api_key:
        headers["Authorization"] = f"Bearer {args.api_key}"
        headers["X-API-Key"] = args.api_key

    failures = []
    for case in cases:
        response = requests.post(
            f"{args.base_url.rstrip('/')}/api/query/natural",
            headers=headers,
            json={"query": case["question"]},
            timeout=120,
        )
        if response.status_code != 200:
            failures.append({"question": case["question"], "error": f"HTTP {response.status_code}: {response.text}"})
            continue

        payload = response.json()
        sql = payload.get("sql", "")
        rows = payload.get("data", [])
        serialized_rows = json.dumps(rows, ensure_ascii=False)

        if not re.search(case["expected_sql_pattern"], sql):
            failures.append({"question": case["question"], "error": f"SQL pattern mismatch: {sql}"})
            continue

        if len(rows) < int(case.get("expected_min_rows", 0)):
            failures.append({"question": case["question"], "error": f"row count {len(rows)} below expected minimum"})
            continue

        missing_values = [value for value in case.get("expected_result_contains", []) if value not in serialized_rows]
        if missing_values:
            failures.append({"question": case["question"], "error": f"missing expected values: {missing_values}"})

    if failures:
        sys.stdout.write(json.dumps({"success": False, "failures": failures}, ensure_ascii=False, indent=2) + "\n")
        return 1

    sys.stdout.write(json.dumps({"success": True, "count": len(cases)}, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
