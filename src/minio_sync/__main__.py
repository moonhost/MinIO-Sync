"""minio-sync 命令行入口：python -m minio_sync。"""

from __future__ import annotations

import sys

from minio_sync.config import Settings, parse_cli_args
from minio_sync.exceptions import ConfigError
from minio_sync.logging_setup import setup_logging
from minio_sync.scheduler import run_service


def main(argv: list[str] | None = None) -> int:
    args = parse_cli_args(argv)
    try:
        settings = Settings.load(
            log_level=args.log_level,
        )
    except ConfigError as e:
        print(f"配置错误: {e}", file=sys.stderr)
        return 2

    setup_logging(settings)

    if args.check:
        print("配置检查通过")
        print(f"  endpoint: {settings.endpoint}")
        print(f"  bucket:   {settings.bucket}")
        print(f"  secure:   {settings.secure} | verify_ssl: {settings.verify_ssl}")
        print(f"  local:    {settings.local_sync_path}")
        return 0

    run_service(settings)
    return 0


if __name__ == "__main__":
    sys.exit(main())
