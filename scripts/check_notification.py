"""
MinIO Bucket Notification 功能可用性检测脚本

分 4 步验证 listen_bucket_notification 是否可用：
  1. 基础连通性 — 能否连接 MinIO 并访问 bucket
  2. API 存在性 — minio-sdk 是否包含 listen_bucket_notification 方法
  3. 事件监听测试 — 实际调用 API，观察是否返回事件流
  4. 端到端验证 — 上传一个文件，确认能收到通知事件

用法:
  python scripts/check_notification.py
"""

import contextlib
import sys
import tempfile
import threading
import time
from pathlib import Path

# 未安装包时从源码目录导入
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from minio_sync.client import get_minio_client
from minio_sync.config import Settings

PASSED = "\033[92m✔ 通过\033[0m"
FAILED = "\033[91m✘ 失败\033[0m"
WARN = "\033[93m⚠ 警告\033[0m"


def check_config(settings: Settings) -> bool:
    """第0步：检查配置是否已填写"""
    print("\n" + "=" * 60)
    print("第 0 步：检查配置")
    print("=" * 60)

    missing = []
    if not settings.endpoint:
        missing.append("endpoint")
    if not settings.access_key:
        missing.append("access_key")
    if not settings.secret_key:
        missing.append("secret_key")
    if not settings.bucket:
        missing.append("bucket")

    if missing:
        print(f"  {FAILED} — 缺少以下配置项: {missing}")
        print("  请先在 .env 中填写 MINIO_ACCESS_KEY / MINIO_SECRET_KEY")
        return False

    print(f"  {PASSED} — 所有配置项已填写")
    print(f"    endpoint:   {settings.endpoint}")
    print(f"    bucket:     {settings.bucket}")
    print(f"    secure:     {settings.secure}")
    return True


def check_connectivity(settings: Settings) -> bool:
    """第1步：基础连通性"""
    print("\n" + "=" * 60)
    print("第 1 步：基础连通性检查")
    print("=" * 60)

    try:
        client = get_minio_client(settings)
        print("  MinIO 客户端创建成功")
    except Exception as e:
        print(f"  {FAILED} — 客户端创建失败: {e}")
        return False

    try:
        exists = client.bucket_exists(settings.bucket)
        if exists:
            print(f"  {PASSED} — bucket '{settings.bucket}' 存在且可访问")
        else:
            print(f"  {FAILED} — bucket '{settings.bucket}' 不存在")
            return False
    except Exception as e:
        print(f"  {FAILED} — 无法访问 bucket: {e}")
        return False

    with contextlib.suppress(Exception):
        print(f"  服务端地址: {client._base_url}")

    return True


def check_api_exists() -> bool:
    """第2步：API 存在性"""
    print("\n" + "=" * 60)
    print("第 2 步：listen_bucket_notification API 存在性")
    print("=" * 60)

    from minio import Minio

    if hasattr(Minio, "listen_bucket_notification"):
        print(f"  {PASSED} — minio.Minio.listen_bucket_notification 方法存在")
    else:
        print(f"  {FAILED} — minio.Minio 没有 listen_bucket_notification 方法")
        print("  当前 minio-py 版本可能过旧，请升级: pip install minio --upgrade")
        return False

    try:
        import minio as minio_pkg
        print(f"  minio-py 版本: {minio_pkg.__version__}")
    except AttributeError:
        print(f"  {WARN} — 无法获取 minio-py 版本号")

    return True


def check_event_listen(settings: Settings) -> bool:
    """第3步：事件监听测试（只监听 5 秒，不要求收到事件）"""
    print("\n" + "=" * 60)
    print("第 3 步：事件监听 API 调用测试（监听 5 秒）")
    print("=" * 60)

    client = get_minio_client(settings)
    prefix = settings.sync_prefix if settings.sync_prefix else ""

    result = {"ok": False, "error": None}
    events_received = []

    def listener() -> None:
        try:
            events = client.listen_bucket_notification(
                settings.bucket,
                prefix=prefix,
                events=("s3:ObjectCreated:*",),
            )
            result["ok"] = True
            for event in events:
                events_received.append(event)
                if len(events_received) >= 1:
                    break
        except Exception as e:
            if not result["ok"]:
                result["error"] = e

    t = threading.Thread(target=listener, daemon=True)
    t.start()
    t.join(timeout=5)

    if result["ok"]:
        print(f"  {PASSED} — listen_bucket_notification 调用成功，已建立事件流")
        if events_received:
            print(f"  在 5 秒内收到了 {len(events_received)} 个事件")
        else:
            print("  5 秒内未收到事件（正常，可能没有新文件上传）")
        return True
    elif result["error"]:
        err = result["error"]
        print(f"  {FAILED} — 调用失败: {type(err).__name__}: {err}")
        if "404" in str(err) or "Not Found" in str(err):
            print("\n  可能原因：")
            print("    1. MinIO 版本过旧（需要 RELEASE.2020-06-06 及以上）")
            print("    2. MinIO 以 S3 兼容模式运行（非原生 MinIO）")
            print("    3. 服务器端禁用了 Bucket Notification")
        if "403" in str(err) or "Forbidden" in str(err):
            print("\n  可能原因：")
            print("    1. 当前 access_key/secret_key 权限不足")
            print("    2. 需要给该用户授予 s3:ListenBucketNotification 权限")
        return False
    else:
        print(f"  {WARN} — 监听线程 5 秒内未返回（可能正在等待事件，API 本身可用）")
        return True


def check_end_to_end(settings: Settings) -> bool:
    """第4步：端到端验证 — 上传文件并确认收到通知"""
    print("\n" + "=" * 60)
    print("第 4 步：端到端验证（上传测试文件 → 确认收到通知）")
    print("=" * 60)

    client = get_minio_client(settings)
    prefix = settings.sync_prefix if settings.sync_prefix else ""
    test_key = (
        (prefix + "_notification_test_" + str(int(time.time())))
        if prefix
        else ("_notification_test_" + str(int(time.time())))
    )

    result = {"event_received": False, "error": None}
    listener_started = threading.Event()

    def listener() -> None:
        try:
            events = client.listen_bucket_notification(
                settings.bucket,
                prefix=prefix,
                events=("s3:ObjectCreated:*",),
            )
            listener_started.set()
            for event in events:
                for record in event.get("Records", []):
                    try:
                        obj_key = record["s3"]["object"]["key"]
                    except KeyError:
                        continue
                    if test_key in obj_key:
                        result["event_received"] = True
                        return
        except Exception as e:
            result["error"] = e
            listener_started.set()

    t = threading.Thread(target=listener, daemon=True)
    t.start()

    listener_started.wait(timeout=5)
    if not listener_started.is_set():
        print(f"  {FAILED} — 事件监听未能启动")
        return False

    time.sleep(1)

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("notification test content")
            tmp_path = f.name

        client.fput_object(settings.bucket, test_key, tmp_path)
        print(f"  已上传测试文件: {test_key}")

        Path(tmp_path).unlink(missing_ok=True)
    except Exception as e:
        print(f"  {FAILED} — 上传测试文件失败: {e}")
        return False

    t.join(timeout=10)

    if result["error"]:
        print(f"  {FAILED} — 监听过程中出错: {result['error']}")
        return False

    if result["event_received"]:
        print(f"  {PASSED} — 成功收到上传事件通知！端到端验证通过")
    else:
        print(f"  {WARN} — 10 秒内未收到通知事件")
        print("  可能原因：")
        print("    1. MinIO 服务器负载高，通知延迟")
        print("    2. 事件监听连接在测试期间断开重连")
        print("  这不一定意味着功能不可用，建议重试或延长等待时间")

    with contextlib.suppress(Exception):
        client.remove_object(settings.bucket, test_key)

    return result["event_received"]


def main() -> None:
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   MinIO Bucket Notification 可用性检测                    ║")
    print("╚══════════════════════════════════════════════════════════╝")

    settings = Settings.load()

    results = []

    results.append(("配置检查", check_config(settings)))
    if not results[-1][1]:
        print_summary(results)
        return

    results.append(("基础连通", check_connectivity(settings)))
    if not results[-1][1]:
        print_summary(results)
        return

    results.append(("API 存在", check_api_exists()))
    if not results[-1][1]:
        print_summary(results)
        return

    results.append(("事件监听", check_event_listen(settings)))
    if not results[-1][1]:
        print_summary(results)
        return

    results.append(("端到端", check_end_to_end(settings)))

    print_summary(results)


def print_summary(results: list) -> None:
    print("\n" + "=" * 60)
    print("检测结果汇总")
    print("=" * 60)

    all_passed = True
    for name, ok in results:
        status = PASSED if ok else FAILED
        print(f"  {status}  {name}")
        if not ok:
            all_passed = False

    print()
    if all_passed:
        print("  🎉 所有检测通过！listen_bucket_notification 可用。")
        print('  建议在配置中设置 sync_mode = "hybrid"')
    else:
        print("  ❌ 部分检测未通过，listen_bucket_notification 可能不可用。")
        print('  建议在配置中设置 sync_mode = "poll" 作为回退方案')
        print()
        print("  常见解决方案：")
        print("    1. 升级 MinIO 服务器到 RELEASE.2020-06-06 及以上版本")
        print("    2. 升级 minio-py: pip install minio --upgrade")
        print("    3. 检查用户权限是否包含 s3:ListenBucketNotification")
        print("    4. 如果 MinIO 是代理/网关模式，确认后端支持 Notification")


if __name__ == "__main__":
    main()
