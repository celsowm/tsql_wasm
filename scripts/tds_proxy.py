#!/usr/bin/env python3

from tds_proxy_app.orchestrator import parse_args, run_from_args


def main() -> int:
    return run_from_args(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
