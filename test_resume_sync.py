# -*- coding: utf-8 -*-
"""续传参数校验/同步的端到端验证（临时集群 + 真实 HTTP）。"""
import base64
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend import config

# ---- 临时目录 / 高端口，避免污染 data/ 与占用默认端口 ----
TMP = tempfile.mkdtemp(prefix="dfsvs_test_")
config.DATA_DIR = os.path.join(TMP, "data")
config.META_DIR = os.path.join(config.DATA_DIR, "meta")
config.SESSION_DIR = os.path.join(config.DATA_DIR, "sessions")
config.DATANODE_ROOT = os.path.join(config.DATA_DIR, "datanodes")
config.NAMENODE_PORT = 18020
config.DATANODE_PORTS = {
    "dn1": (18021, "rack-1"), "dn2": (18022, "rack-2"),
    "dn3": (18023, "rack-3"),
}

from backend.main import Cluster  # noqa: E402

BASE = "http://127.0.0.1:18020"
PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  ✔ " if cond else "  �’ "), name, ("" if cond else f"  -> {detail}"))


def call(method, path, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers=headers,
                                 method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def b64(b):
    return base64.b64encode(b).decode()


def send_all(token, sid, content, piece):
    total = max(1, (len(content) + piece - 1) // piece)
    for i in range(total):
        part = content[i * piece:(i + 1) * piece]
        ck = hashlib.sha256(part).hexdigest()
        st, r = call("POST", "/api/upload/chunk",
                     {"session": sid, "index": i, "data": b64(part),
                      "checksum": ck}, token)
        assert st == 200, (st, r)
    return total


def fs_exists(token, path):
    st, r = call("GET", "/api/fs/stat?path=" + urllib.parse.quote(path),
                 token=token)
    return st == 200


def main():
    import urllib.parse  # noqa
    glob = globals()
    glob["urllib"].parse = urllib.parse
    cluster = Cluster(datanode_count=3, nn_port=18020, reset=True, seed=False,
                      verbose=False)
    cluster.start()
    time.sleep(0.5)
    try:
        st, r = call("POST", "/api/auth/login",
                     {"username": "admin", "password": "admin123"})
        assert st == 200, (st, r)
        token = r["token"]
        content = bytes((i * 7 + 3) & 0xFF for i in range(300000))  # ~293KiB
        name = "resume_sync_test.bin"

        # ========== 场景 1：暂停后改目标目录续传 ==========
        print("[1] 暂停后改目标目录，续传应落到新目录")
        p1 = 32768
        st, b = call("POST", "/api/upload/begin",
                     {"path": "/docs", "filename": name, "size": len(content),
                      "piece_size": p1}, token)
        assert st == 200, (st, b)
        sid = b["session"]
        # 传一半
        half = b["total_pieces"] // 2
        for i in range(half):
            part = content[i * p1:(i + 1) * p1]
            st, r = call("POST", "/api/upload/chunk",
                         {"session": sid, "index": i, "data": b64(part),
                          "checksum": hashlib.sha256(part).hexdigest()}, token)
            assert st == 200, (st, r)
        # 新目录先建好
        st, r = call("POST", "/api/fs/mkdir", {"path": "/", "name": "newdir"}, token)
        assert st == 200, (st, r)
        # 用同一会话 id、新路径续传
        st, b2 = call("POST", "/api/upload/begin",
                      {"path": "/newdir", "filename": name,
                       "size": len(content), "piece_size": p1,
                       "session": sid}, token)
        check("续传返回 path 已同步为新目录", b2["path"] == "/newdir", b2)
        check("返回 path_changed=true", b2.get("path_changed") is True, b2)
        check("已收分片进度保留", b2["received_count"] == half, b2)
        send_all(token, sid, content, p1)
        st, r = call("POST", "/api/upload/complete",
                     {"session": sid, "path": "/newdir", "piece_size": p1},
                     token)
        check("complete 成功", st == 200, (st, r))
        check("文件落在新目录 /newdir",
              fs_exists(token, f"/newdir/{name}"), "")
        check("旧目录 /docs 没有该文件",
              not fs_exists(token, f"/docs/{name}"), "")

        # ========== 场景 2：暂停后改分片大小续传 ==========
        print("[2] 暂停后改分片大小，续传应按新分片重传并成功")
        old_piece, new_piece = 32768, 131072
        st, b = call("POST", "/api/upload/begin",
                     {"path": "/docs", "filename": name, "size": len(content),
                      "piece_size": old_piece}, token)
        sid = b["session"]
        part = content[:old_piece]
        call("POST", "/api/upload/chunk",
             {"session": sid, "index": 0, "data": b64(part),
              "checksum": hashlib.sha256(part).hexdigest()}, token)
        st, b2 = call("POST", "/api/upload/begin",
                      {"path": "/docs", "filename": name,
                       "size": len(content), "piece_size": new_piece,
                       "session": sid}, token)
        check("piece_size 同步为新值", b2["piece_size"] == new_piece, b2)
        check("total_pieces 按新大小重算",
              b2["total_pieces"] ==
              max(1, (len(content) + new_piece - 1) // new_piece), b2)
        check("旧分片全部作废(received 清空)", b2["received_count"] == 0, b2)
        check("piece_reset=true", b2.get("piece_reset") is True, b2)
        # 旧大小的分片按新会话写入必须被服务端拒绝
        st, r = call("POST", "/api/upload/chunk",
                     {"session": sid, "index": 0, "data": b64(part),
                      "checksum": hashlib.sha256(part).hexdigest()}, token)
        check("旧分片长度被服务端拒绝", st == 400, (st, r))
        send_all(token, sid, content, new_piece)
        st, r = call("POST", "/api/upload/complete",
                     {"session": sid, "path": "/docs",
                      "piece_size": new_piece}, token)
        check("按新分片完成上传", st == 200, (st, r))
        # 内容校验
        st, info = call("GET",
                        "/api/download/info?path=" +
                        urllib.parse.quote(f"/docs/{name}"), token=token)
        check("文件大小一致", st == 200 and info["size"] == len(content),
              (st, info))

        # ========== 场景 3：暂停期间删除目标目录，续传必须报错 ==========
        print("[3] 暂停期间删除目标目录，续传不得静默成功")
        call("POST", "/api/fs/mkdir", {"path": "/", "name": "victim"}, token)
        st, b = call("POST", "/api/upload/begin",
                     {"path": "/victim", "filename": name,
                      "size": len(content), "piece_size": 131072}, token)
        sid = b["session"]
        part = content[:131072]
        call("POST", "/api/upload/chunk",
             {"session": sid, "index": 0, "data": b64(part),
              "checksum": hashlib.sha256(part).hexdigest()}, token)
        # 删除目录
        st, r = call("POST", "/api/fs/delete", {"path": "/victim"}, token)
        assert st == 200, (st, r)
        st, b2 = call("POST", "/api/upload/begin",
                      {"path": "/victim", "filename": name,
                       "size": len(content), "piece_size": 131072,
                       "session": sid}, token)
        check("续传到已删除目录被拒绝(begin)", st == 400, (st, b2))
        # 即便绕过 begin 直接 complete，也不得静默重建目录报成功
        # （先把剩余分片补齐到同一路径的新会话不可行；这里直接 complete 旧会话，
        #  应因目录不存在而失败）
        st, r = call("POST", "/api/upload/complete",
                     {"session": sid, "path": "/victim",
                      "piece_size": 131072}, token)
        check("complete 对已删除目录明确报错(不会静默重建)", st == 400,
              (st, r))
        check("目录确实未被静默重建", not fs_exists(token, "/victim"), "")

        # ========== 场景 4：complete 回显参数与会话不一致 → 拒绝 ==========
        print("[4] complete 参数漂移防护")
        st, b = call("POST", "/api/upload/begin",
                     {"path": "/docs", "filename": name, "size": len(content),
                      "piece_size": 131072}, token)
        sid = b["session"]
        send_all(token, sid, content, 131072)
        st, r = call("POST", "/api/upload/complete",
                     {"session": sid, "path": "/elsewhere",
                      "piece_size": 131072}, token)
        check("complete 回显错误目录被拒绝", st == 400, (st, r))
        st, r = call("POST", "/api/upload/complete",
                     {"session": sid, "path": "/docs",
                      "piece_size": 32768}, token)
        check("complete 回显错误分片大小被拒绝", st == 400, (st, r))

        # ========== 场景 5：正常无漂移续传不受影响 ==========
        print("[5] 普通续传（无参数漂移）保持兼容")
        st, b = call("POST", "/api/upload/begin",
                     {"path": "/docs", "filename": "plain.bin",
                      "size": len(content), "piece_size": 131072}, token)
        sid = b["session"]
        part = content[:131072]
        call("POST", "/api/upload/chunk",
             {"session": sid, "index": 0, "data": b64(part),
              "checksum": hashlib.sha256(part).hexdigest()}, token)
        st, b2 = call("POST", "/api/upload/begin",
                      {"path": "/docs", "filename": "plain.bin",
                       "size": len(content), "piece_size": 131072,
                       "session": sid}, token)
        check("同参数续传进度保留",
              st == 200 and b2["received_count"] == 1
              and b2.get("resynced") is False, (st, b2))
        send_all(token, sid, content, 131072)
        st, r = call("POST", "/api/upload/complete",
                     {"session": sid}, token)
        check("不带回显参数的 complete 仍兼容成功", st == 200, (st, r))

    finally:
        cluster.stop()
        shutil.rmtree(TMP, ignore_errors=True)

    print(f"\n通过 {len(PASS)} / 失败 {len(FAIL)}")
    if FAIL:
        print("FAILED:", FAIL)
        sys.exit(1)
    print("ALL PASS")


if __name__ == "__main__":
    main()
