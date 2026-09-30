# 条件请求对象源站 API

基于 **Python + FastAPI + PostgreSQL** 的对象源站，完整实现 RFC 9110 条件请求与字节范围服务：

- 客户端按路径 `PUT /objects/{path}` 发布内容修订；每次发布生成新修订。
- 以内容的 **SHA-256** 生成**强 ETag**（`"<64 hex>"`），`Last-Modified` 为修订创建时间（秒级精度）。
- 所有方法按 **RFC 9110 §13.2.2** 的顺序求值前置条件：
  `If-Match` → `If-Unmodified-Since`（仅当无 `If-Match`）→ `If-None-Match` → `If-Modified-Since`（仅 GET/HEAD 且无 `If-None-Match`）。
  条件失败返回 **412**，GET/HEAD 命中 `If-None-Match` / `If-Modified-Since` 返回 **304**。
- 条件求值与修订选择发生在**同一事务**内：读请求对对象行加 `FOR SHARE` 锁、写请求加 `FOR UPDATE` 锁，并发发布串行化，杜绝并发覆盖（`If-None-Match: *` 的"仅创建"语义在并发下也只有一个成功）。
- GET/HEAD **先处理前置条件，再处理 Range**：支持单段（`bytes=0-99`、`bytes=0-`）、后缀（`bytes=-500`）与多段范围；重叠或相邻区间按升序合并；分别返回 200 / 206 / 304 / 416。
- 多段范围返回 `multipart/byteranges`，**边界由修订 ETag + 区间集合派生**，重复请求产生完全相同的边界与 `Content-Range`。
- `If-Range`（ETag 强比较或 HTTP 日期）失配时忽略 Range，返回完整实体（200）。
- 每次响应绑定同一修订：流式传输期间事务与行锁保持打开，此时发布新版本**不会混入**内容或元数据。
- **可恢复复制会话**：`POST /copy-sessions` 在同一数据库快照中锁定源路径集合与全部目标路径，生成按稳定路径与修订序排列、带 SHA-256 内容摘要的清单，并为每个目标路径（含目标尚不存在的状态）保存**修订身份基线**；客户端按游标逐段拉取内容（重复拉取字节、摘要、游标完全一致，即使源对象随后发布新修订），逐段确认时服务锁定目标并与基线比较修订身份——**建档后目标路径的任何发布（哪怕字节与待复制内容完全相同）都判为冲突**，返回 `409`、冲突对象与恢复位置，整批不应用、不推进游标；基线未变时维持原子应用与会话前同内容目标的幂等跳过，重复确认或重放返回同一批次结果。
- **历史修订回收**：`POST /gc` 提交 1～100 个对象路径（服务去重）、UTC 截止时间 `before` 与布尔 `dry_run`；只考虑创建时间**严格早于** `before` 的修订，且**当前修订、复制会话清单或目标基线引用的修订一律保留**。`dry_run`（缺省 `true`）在可重复读快照中按路径排序返回候选修订号、可释放字节数与被保留修订的原因，不改数据；`dry_run=false` 按固定路径升序锁定全部对象行，在**同一事务内重新判定并删除**，整批成功或回滚——与 PUT、复制会话建档/确认、流式 GET 并发时不会误删在用修订或留下悬空引用；重复执行返回空清单，后续发布的修订号仍递增。**已迁走的路径**（`POST /moves`）没有当前修订但仍保留修订历史，同样可以被回收：未被任何会话引用的旧修订正常清理，而会话清单/基线钉住的修订一律保留。
- **对象路径迁移**：`POST /moves` 提交非空且互不相同的 `source_path`、`target_path`、`source_etag`（强 ETag）与正整数 `source_revision_no`；仅当**源强 ETag 与修订号都匹配当前修订、且目标当前无内容**时，按路径升序同时锁住两端，在**一个事务**内把源内容与媒体类型发布为目标的**新修订**（目标侧按自身历史 `max+1` 编号），并把源路径摘除（其 GET/HEAD 随即 **404**）。源不存在（含已迁走）返回 **404**，源已变（ETag 或修订号不符）返回 **412**（响应带当前 ETag/修订号），目标已有内容返回 **409**；任何失败整事务回滚，不留部分生效。已迁走的路径再次 PUT 或作为迁移目标复用时，修订号由每路径高水位线 `revision_seq` 锚定，**只增不复用、不回退**——即便旧修订已被 `/gc` 全部清理。

## 快速启动（Docker Compose）

```bash
docker compose up --build
```

服务监听 <http://localhost:8000>。PostgreSQL 数据保存在 `pgdata` 卷中；表结构由应用启动时自动创建。

健康检查：

```bash
curl http://localhost:8000/healthz
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `PUT` | `/objects/{path}` | 发布新修订；请求体为内容，`Content-Type` 被保存；响应 `204` + `ETag` / `Last-Modified` / `X-Revision-No` |
| `GET` | `/objects/{path}` | 读取当前修订；支持全部条件头与 `Range` / `If-Range` |
| `HEAD` | `/objects/{path}` | 同 GET，仅返回头部 |
| `POST` | `/moves` | 原子路径迁移：把源当前修订（按强 ETag + 修订号锁定）发布为目标新修订并摘除源；201 / 404 / 412 / 409，失败整事务回滚 |
| `POST` | `/copy-sessions` | 创建复制会话：提交 `source_paths`、`target_prefix`、可选 `cursor`（上次确认游标）与 `segment_size`；响应 `201` + 带 SHA-256 摘要的清单 |
| `GET` | `/copy-sessions/{id}` | 会话状态：清单摘要、分段进度、恢复位置 |
| `GET` | `/copy-sessions/{id}/segments?cursor=` | 拉取游标之后的内容分段（无 `cursor` 参数时返回首段）；重复拉取字节、摘要、游标完全一致 |
| `POST` | `/copy-sessions/{id}/acks` | 按游标确认分段：锁定目标并与建档修订基线比较身份，原子批次应用；建档后任何发布（含同字节重发）返回 `409`，整批不应用、不推进游标；重复确认 / 重放返回同一批次结果 |
| `POST` | `/gc` | 历史修订回收：提交 `paths`（1～100，去重）、UTC 截止时间 `before` 与 `dry_run`；预览返回候选修订号、可释放字节数与保留原因，执行在同一事务内重判定并删除（整批成功或回滚）；路径不存在整次 `404` |
| `GET` | `/healthz` | 健康检查 |

支持的条件头：`If-Match`、`If-None-Match`、`If-Modified-Since`、`If-Unmodified-Since`（PUT/GET/HEAD 均按 §13.2.2 顺序求值）。

## curl 验证

```bash
# 1. 发布内容（记下返回的 ETag 与 Last-Modified）
curl -i -X PUT --data-binary 'hello world' -H 'Content-Type: text/plain' \
  http://localhost:8000/objects/demo.txt
# HTTP/1.1 204 ...  ETag: "b94d27b9..."  Last-Modified: Tue, 22 Sep 2026 ...

ETAG=$(curl -sI http://localhost:8000/objects/demo.txt | awk '/^[Ee][Tt]ag:/ {print $2}' | tr -d '\r')

# 2. 条件 GET：ETag 未变 -> 304（If-None-Match 用弱比较，W/"..." 形式同样命中）
curl -i -H "If-None-Match: $ETAG" http://localhost:8000/objects/demo.txt
curl -i -H "If-None-Match: W/${ETAG//\"/\\\"}" http://localhost:8000/objects/demo.txt

# 3. If-Modified-Since -> 304；If-Match 不匹配 -> 412（强比较，W/ 形式必失败）
curl -i -H "If-Modified-Since: $(curl -sI http://localhost:8000/objects/demo.txt | awk '/^[Ll]ast-[Mm]odified:/ {sub(/^[^:]*: /,""); print}' | tr -d '\r')" \
  http://localhost:8000/objects/demo.txt
curl -i -H 'If-Match: "stale"' http://localhost:8000/objects/demo.txt

# 4. 乐观并发写：仅当未修改才覆盖
curl -i -X PUT --data-binary 'v2' -H "If-Match: $ETAG" http://localhost:8000/objects/demo.txt   # 204
curl -i -X PUT --data-binary 'v3' -H "If-Match: $ETAG" http://localhost:8000/objects/demo.txt   # 412（ETag 已变）

# 5. 仅创建语义：已存在则 412
curl -i -X PUT --data-binary 'x' -H 'If-None-Match: *' http://localhost:8000/objects/demo.txt   # 412

# 6. 字节范围
curl -i -H 'Range: bytes=0-4'   http://localhost:8000/objects/demo.txt   # 206 + Content-Range
curl -i -H 'Range: bytes=-5'    http://localhost:8000/objects/demo.txt   # 后缀范围
curl -i -H 'Range: bytes=0-99999' http://localhost:8000/objects/demo.txt # 截断到末尾
curl -i -H 'Range: bytes=99999-' http://localhost:8000/objects/demo.txt  # 416 + Content-Range: bytes */N

# 7. 多段范围：重叠/相邻自动合并；重复请求边界与 Content-Range 完全一致
curl -i -H 'Range: bytes=0-1,2-3' http://localhost:8000/objects/demo.txt # 合并为单段 206
curl -i -H 'Range: bytes=0-1,5-9' http://localhost:8000/objects/demo.txt # multipart/byteranges
curl -s -H 'Range: bytes=0-1,5-9' http://localhost:8000/objects/demo.txt | sha256sum
curl -s -H 'Range: bytes=0-1,5-9' http://localhost:8000/objects/demo.txt | sha256sum   # 两次相同

# 8. If-Range：失配返回完整实体（200），匹配才返回 206
curl -i -H 'Range: bytes=0-4' -H 'If-Range: "stale"' http://localhost:8000/objects/demo.txt  # 200
curl -i -H 'Range: bytes=0-4' -H "If-Range: $ETAG"   http://localhost:8000/objects/demo.txt  # 206
```

## 对象路径迁移（POST /moves）

把一个对象按路径整体迁移到一个**当前为空**的新路径。请求同时给出源当前修订的**强 ETag 与修订号**作为乐观锁：

```bash
# 准备：源对象发布到修订 2
curl -si -X PUT --data-binary 'quarterly results' \
  -H 'Content-Type: text/plain; charset=utf-8' \
  http://localhost:8000/objects/demo/report.txt | grep -i x-revision-no   # X-Revision-No: 1
curl -si -X PUT --data-binary 'quarterly results v2' \
  -H 'Content-Type: text/plain; charset=utf-8' \
  http://localhost:8000/objects/demo/report.txt | grep -iE 'etag|x-revision-no'
# ETag: "a7fc813eb2f62294d9fc1408c4741fe3a4c0c75f75494d80297f2bfcd5e517b8"
# X-Revision-No: 2

# 迁移：源 ETag + 修订号都匹配、目标为空时才生效（单事务）
curl -i -X POST http://localhost:8000/moves \
  -H 'Content-Type: application/json' \
  -d '{"source_path": "demo/report.txt",
       "target_path": "archive/2026/report.txt",
       "source_etag": "\"a7fc813eb2f62294d9fc1408c4741fe3a4c0c75f75494d80297f2bfcd5e517b8\"",
       "source_revision_no": 2}'
# HTTP/1.1 201 Created
# ETag: "a7fc813eb2f62294d9fc1408c4741fe3a4c0c75f75494d80297f2bfcd5e517b8"
# Last-Modified: Wed, 30 Sep 2026 01:33:52 GMT
# X-Revision-No: 1
# Content-Type: application/json
#
# {
#   "source_path": "demo/report.txt",
#   "target_path": "archive/2026/report.txt",
#   "target_etag": "\"a7fc813eb2f62294d9fc1408c4741fe3a4c0c75f75494d80297f2bfcd5e517b8\"",
#   "target_revision_no": 1,
#   "source_etag": "\"a7fc813eb2f62294d9fc1408c4741fe3a4c0c75f75494d80297f2bfcd5e517b8\"",
#   "source_revision_no": 2
# }

# 源路径已摘除：GET/HEAD 都是 404；目标以自身的修订 1 提供相同内容与媒体类型
curl -i  http://localhost:8000/objects/demo/report.txt            # 404 {"detail":"Not Found"}
curl -sI http://localhost:8000/objects/demo/report.txt | head -1  # HTTP/1.1 404 Not Found
curl -i  http://localhost:8000/objects/archive/2026/report.txt   # 200，ETag 与源一致，text/plain

# 在已迁走的旧路径上重新 PUT：修订号接续（3），绝不回退或复用 1/2
curl -si -X PUT --data-binary 'new file at old path' \
  http://localhost:8000/objects/demo/report.txt | grep -i x-revision-no          # X-Revision-No: 3

# 失败一：源不存在（路径从未发布，或已经迁走）-> 404
curl -i -X POST http://localhost:8000/moves -H 'Content-Type: application/json' \
  -d '{"source_path":"demo/report.txt","target_path":"x/y.txt",
       "source_etag":"\"a7fc813eb2f62294d9fc1408c4741fe3a4c0c75f75494d80297f2bfcd5e517b8\"",
       "source_revision_no":2}'
# HTTP/1.1 404 Not Found
# {"detail":"source_not_found","source_path":"demo/report.txt"}

# 失败二：源已变（ETag 与修订号均不符）-> 412，响应给出当前 ETag/修订号
curl -i -X POST http://localhost:8000/moves -H 'Content-Type: application/json' \
  -d '{"source_path":"s.txt","target_path":"t.txt",
       "source_etag":"\"stale\"","source_revision_no":1}'
# HTTP/1.1 412 Precondition Failed
# ETag: "f9ef50d69e756978acc0ac3dcbc444ae28cfae32cd1449d4dfcd8849d8a02b31"
# {"detail":"precondition_failed","source_path":"s.txt",
#  "mismatches":["source_etag","source_revision_no"],
#  "current_etag":"\"f9ef50d69e756978acc0ac3dcbc444ae28cfae32cd1449d4dfcd8849d8a02b31\"",
#  "current_revision_no":2}

# 失败三：目标已有内容 -> 409（整事务回滚，源保持不动、目标保持原样）
curl -i -X POST http://localhost:8000/moves -H 'Content-Type: application/json' \
  -d '{"source_path":"s.txt","target_path":"busy.txt",
       "source_etag":"\"f9ef50d69e756978acc0ac3dcbc444ae28cfae32cd1449d4dfcd8849d8a02b31\"",
       "source_revision_no":2}'
# HTTP/1.1 409 Conflict
# {"detail":"target_already_exists","target_path":"busy.txt",
#  "current_etag":"\"20323a24be105b1b519962994b8e4e6a7f8e3cd0d005b8ee10c9aeb66da5d40a\"",
#  "current_revision_no":1}

# 请求体校验：四个字段必填；source_etag 必须是强 ETag（W/ 弱形式 400）；
# source_revision_no 必须是正整数；两端必须非空且不同（400）。
```

迁移与既有机制的关系：

- **复制会话不受迁移影响**：迁移前创建的会话清单钉住的是**不可变修订行**（源路径的旧修订），迁移只摘除源的 `current_id` 指针、不删修订，所以会话仍可逐段拉取已固定的源修订；会话目标基线在迁移后若发生变化（例如有人把另一个对象迁到了该目标），确认时仍按原规则报 **409**、整批不应用。要复制迁移后的新位置，用新路径重新建档即可。
- **历史回收覆盖已迁走路径**：迁走后源路径没有当前修订，其旧修订不再被 `current` 原因保留；`/gc` 可以清理其中**未被任何会话清单/基线引用**的修订，会话固定的修订仍被保留、不会误删。
- **并发安全**：迁移与 PUT、复制会话建档/确认、`/gc`、流式 GET 共用同一套"先按路径升序锁 `objects` 行、锁内再读修订"的规则；两个迁移交换/重叠端点时按相同加锁顺序串行化，不会死锁，也不会出现部分生效或修订号重复。


## 复制会话（可恢复的快照复制）

复制会话把一组源对象的**某一时刻快照**复制到目标路径前缀下，全过程可重试、可断点续传：

1. **创建会话**：提交源路径集合、目标前缀与可选的上次确认游标。服务在**同一事务**里先对源路径与全部目标路径（目标尚不存在时先插入占位行）按全局路径升序加 `FOR SHARE` 锁，再在锁内快照源修订并为**每个目标路径保存修订身份基线**（`revision_id` 为 NULL 即"目标不存在"）；清单按稳定路径与修订序生成，每条清单项带 SHA-256 内容摘要。清单与基线持久化后源对象发布新修订**不影响**本会话。
2. **拉取分段**：`GET /copy-sessions/{id}/segments` 返回首段；之后用上一段响应里的 `cursor`
   作为查询参数取下一段。每段包含条目内容（base64）、每条目的 SHA-256、段摘要与标识本段的游标。
   重复拉取同一段返回**完全相同的字节、摘要和游标**。
3. **逐段确认**：`POST /copy-sessions/{id}/acks`，请求体 `{"cursor": "<该段游标>"}`。服务将该分段
   对应的修订**作为一次原子批次**应用到目标前缀（目标路径 = `target_prefix + source_path`）。
   确认事务**先按目标路径升序锁定本批全部对象行——包括建档时留下的 `current_id` 为 NULL 的空占位
   行——再读取当前修订**（分两条语句：纯 `FOR UPDATE` 取锁之后才 JOIN `revisions`），然后**与建档基线
   比较修订身份**：
   基线不存在且目标仍不存在则创建修订；基线身份未变且摘要相同则幂等跳过；
   **基线之后目标路径只要发生过发布就是冲突——即使发布的字节与待复制内容完全相同**
   （仅凭摘要无法区分"未改动"与"同字节重新发布"，这是漏判的来源）。
   锁必须先于读、且必须覆盖空占位：确认若先 INNER JOIN `revisions` 再加锁，空占位行会被连接过滤掉、
   一把锁都拿不到，与该路径的首个 `PUT` 交错时双方可能各插一个修订号 1，确认甚至以 **500** 告终而不是
   返回冲突。修复后的两种交错结果：
   **PUT 先提交** → 确认在占位行锁上等待，醒来后看到新修订，返回 `409` 及冲突对象与恢复位置，整批回滚、
   游标不前进；**确认先持锁** → `PUT` 等待行锁，确认提交修订 1 后 `PUT` 接续发布修订 2（`max + 1`，
   不再硬编码 1，也不会死锁）。
   任一冲突则整批回滚，返回 `409` 及结构化结果（冲突对象、`reason`、期望/实际/基线摘要与修订号、
   恢复位置），不留部分应用、不推进游标。批次结果持久化，**重复确认或重放返回同一批次结果**。
4. **恢复**：确认严格按段序进行；游标过期 / 乱序确认返回 `409 cursor_expired` 与恢复位置。
   冲突解决后（例如用条件 `PUT` 修正目标），用结构化结果里的 `resume.last_confirmed_cursor`
   创建新会话即可从上次确认处继续，新会话基于**新的快照与基线**。
   升级前创建、缺少基线的未完成会话，确认未完成段时返回 `409 baseline_missing`（不会按摘要放行，
   也不会推进游标）；用新会话重新建档即可继续。已持久化的历史确认结果仍原样重放。

```bash
# 1. 准备源对象并创建会话（每段 2 个条目）
curl -s -X PUT --data-binary 'alpha' http://localhost:8000/objects/src/a.txt
curl -s -X PUT --data-binary 'beta'  http://localhost:8000/objects/src/b.txt
curl -s -X PUT --data-binary 'gamma' http://localhost:8000/objects/src/c.txt

SID=$(curl -s -X POST http://localhost:8000/copy-sessions \
  -H 'Content-Type: application/json' \
  -d '{"source_paths": ["src/a.txt", "src/b.txt", "src/c.txt"],
       "target_prefix": "backup/", "segment_size": 2}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["session"])')

# 2. 拉取首段（响应含内容、每条目 sha256、segment_sha256 与本段 cursor）
curl -s "http://localhost:8000/copy-sessions/$SID/segments" | python3 -m json.tool
CURSOR0=$(curl -s "http://localhost:8000/copy-sessions/$SID/segments" | python3 -c 'import sys,json;print(json.load(sys.stdin)["cursor"])')

# 源对象随后发布新修订，重拉该段字节/摘要/游标仍完全一致
curl -s -X PUT --data-binary 'alpha-v2' http://localhost:8000/objects/src/a.txt
curl -s "http://localhost:8000/copy-sessions/$SID/segments" | sha256sum
curl -s "http://localhost:8000/copy-sessions/$SID/segments" | sha256sum   # 两次相同

# 3. 确认首段：原子应用到 backup/ 前缀；重复确认返回同一结果
curl -s -X POST "http://localhost:8000/copy-sessions/$SID/acks" \
  -H 'Content-Type: application/json' -d "{\"cursor\": \"$CURSOR0\"}" | python3 -m json.tool
curl -s http://localhost:8000/objects/backup/src/a.txt   # alpha（源快照内容，非 alpha-v2）

# 4. 拉取并确认下一段，直至 has_more == false
CURSOR1=$(curl -s "http://localhost:8000/copy-sessions/$SID/segments?cursor=$CURSOR0" | python3 -c 'import sys,json;print(json.load(sys.stdin)["cursor"])')
curl -s -X POST "http://localhost:8000/copy-sessions/$SID/acks" \
  -H 'Content-Type: application/json' -d "{\"cursor\": \"$CURSOR1\"}" | python3 -m json.tool

# 5. 查看会话状态（complete）与恢复位置
curl -s "http://localhost:8000/copy-sessions/$SID" | python3 -m json.tool

# 6. 冲突示例：目标被独立修改后确认 -> 409 + 冲突对象 + 恢复位置，且整批未应用
curl -s -X PUT --data-binary 'divergent' http://localhost:8000/objects/backup/src/c.txt
SID2=$(curl -s -X POST http://localhost:8000/copy-sessions -H 'Content-Type: application/json' \
  -d '{"source_paths": ["src/c.txt"], "target_prefix": "backup/"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["session"])')
C=$(curl -s "http://localhost:8000/copy-sessions/$SID2/segments" | python3 -c 'import sys,json;print(json.load(sys.stdin)["cursor"])')
curl -s -X POST "http://localhost:8000/copy-sessions/$SID2/acks" \
  -H 'Content-Type: application/json' -d "{\"cursor\": \"$C\"}" | python3 -m json.tool
# -> {"status": "conflict", "conflicts": [{"target_path": "backup/src/c.txt", ...}],
#     "resume": {"next_segment": 0, "last_confirmed_cursor": null}}

# 6b. 同内容新修订冲突：建档后目标又发布了一份"字节完全相同"的修订
#     仅比较摘要会误判 already_present 并推进游标；修订身份基线使其返回 409
curl -s -X PUT --data-binary 'delta' http://localhost:8000/objects/src/d.txt
SID3=$(curl -s -X POST http://localhost:8000/copy-sessions -H 'Content-Type: application/json' \
  -d '{"source_paths": ["src/d.txt"], "target_prefix": "backup/"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["session"])')
# 会话已创建（基线记录目标 backup/src/d.txt 不存在）；随后独立发布完全相同的字节
curl -s -X PUT --data-binary 'delta' http://localhost:8000/objects/backup/src/d.txt
C3=$(curl -s "http://localhost:8000/copy-sessions/$SID3/segments" | python3 -c 'import sys,json;print(json.load(sys.stdin)["cursor"])')
curl -s -X POST "http://localhost:8000/copy-sessions/$SID3/acks" \
  -H 'Content-Type: application/json' -d "{\"cursor\": \"$C3\"}" | python3 -m json.tool
# -> 409
# {"status": "conflict", "applied": [],
#  "conflicts": [{"target_path": "backup/src/d.txt",
#                 "reason": "target_republished_same_content",
#                 "expected_sha256": "<sha256(delta)>",
#                 "actual_sha256": "<sha256(delta)>",   // 与期望相同，仍是冲突
#                 "actual_revision_no": 1,
#                 "baseline_revision_no": null, ...}],
#  "resume": {"next_segment": 0, "last_confirmed_cursor": null}}   // 游标不推进
# 重新确认重放同一结果；以新会话重新建档（新基线指向当前修订）后则幂等跳过

# 6c. 并发验证：建档时目标尚不存在（空占位行），确认与该路径的首个 PUT 交错。
#     下面的脚本（仅用 Python 标准库）用线程屏障让两个请求同时发出，
#     多跑几次即可观察到两种交错：
#       * PUT 先提交 -> 确认 409（含冲突对象与恢复位置），整批回滚、游标不前进；
#       * 确认先持锁 -> 确认 200 创建修订 1，PUT 等待后接续修订 2（不会重复 1，也不会 500）。
python3 - <<'PY'
import json, threading, urllib.request, uuid

BASE = "http://localhost:8000"

def request(method, path, body=None):
    data = body if isinstance(body, bytes) else (
        json.dumps(body).encode() if body is not None else None)
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.headers, resp.read()
    except urllib.error.HTTPError as e:      # 4xx 也是测试的正常结果
        return e.code, e.headers, e.read()

tag = uuid.uuid4().hex[:12]
src, dst = f"race/{tag}/a.txt", f"backup/race/{tag}/a.txt"
request("PUT", f"/objects/{src}", b"same-bytes")
sid = json.loads(request("POST", "/copy-sessions",
                         {"source_paths": [src], "target_prefix": "backup/"})[2])["session"]
cursor = json.loads(request("GET", f"/copy-sessions/{sid}/segments")[2])["cursor"]

barrier = threading.Barrier(2)
out = {}
def put():
    barrier.wait()
    if PUT_DELAY:                       # 置 0 可偏向"PUT 先提交"的交错
        threading.Event().wait(PUT_DELAY)
    out["put"] = request("PUT", f"/objects/{dst}", b"same-bytes")
def ack():
    barrier.wait()
    out["ack"] = request("POST", f"/copy-sessions/{sid}/acks", {"cursor": cursor})
# PUT_DELAY=0.03 偏向"确认先持锁"（ACK 200 / PUT 修订号 2）；
# PUT_DELAY=0    偏向"PUT 先提交"（ACK 409 / PUT 修订号 1）。两种都应断言通过。
PUT_DELAY = float(__import__("os").environ.get("PUT_DELAY", "0.03"))
threading.Thread(target=put).start(); threading.Thread(target=ack).start()
for th in [t for t in threading.enumerate() if t is not threading.main_thread()]:
    th.join()

put_status, put_headers, _ = out["put"]
ack_status, _, ack_body = out["ack"]
print("PUT:", put_status, "X-Revision-No =", put_headers["X-Revision-No"])
print("ACK:", ack_status)
print(json.dumps(json.loads(ack_body), ensure_ascii=False, indent=2))
assert ack_status in (200, 409)                       # 永远不应该是 500
assert int(put_headers["X-Revision-No"]) == (
    2 if ack_status == 200 else 1)                   # 修订号接续，不重复
PY

# 7. 断点续传：解决冲突后，用上次确认游标创建新会话，从确认处继续
#    （last_confirmed_cursor 取自上一次成功确认的响应 resume.last_confirmed_cursor）
curl -s -X POST http://localhost:8000/copy-sessions -H 'Content-Type: application/json' \
  -d '{"source_paths": ["src/a.txt", "src/b.txt", "src/c.txt"], "target_prefix": "backup/",
       "cursor": "<last_confirmed_cursor>"}' | python3 -m json.tool
```

## 历史修订回收（POST /gc）

对象每发布一次就新增一个修订，历史修订会一直占用存储。`POST /gc` 回收**不再被任何地方引用**的历史修订：

- 请求体：`paths`（1～100 个对象路径，服务去重）、`before`（ISO 8601 时间戳，无时区按 UTC 解释）、`dry_run`（布尔，缺省 `true`）。
- 只有创建时间**严格早于** `before` 的修订才会进入回收视野（出现在 `candidates` / `retained` 中）。
- **一律保留**：对象的当前修订、任何复制会话清单（`copy_manifest`）引用的修订、任何目标基线（`target_baseline`）引用的修订——因此回收永远不会让进行中的复制会话或流式 GET 读到悬空引用。
- 任一路径不存在（含只有空占位行的路径）时**整次返回 404**，不回收任何对象。**已迁走的路径**（`revision_seq > 0`）始终算"存在"：历史还在时正常回收，历史被全部清完后重复执行返回空清单（200，与普通对象一致），而其修订号由 `revision_seq` 保留、不复用；只有从未有过内容的空占位行才报 404。
- `dry_run=true`：在可重复读快照中按路径排序返回候选修订号、每路径与总的可释放字节数、被保留修订的原因，**不改动任何数据**。
- `dry_run=false`：按固定路径升序以 `FOR UPDATE` 锁定全部对象行（与发布、建档、确认相同的全局锁序），在锁内**重新判定**候选并删除，同一事务提交——任何失败整批回滚，不留部分回收。并发 PUT / 建档 / 确认 / 流式 GET 要么已提交（被锁内重判定看到），要么等待本事务提交，因此在用修订不会被误删。
- 重复执行返回空清单；当前修订（永远是最大修订号）不会被删除，后续发布继续 `max + 1` 递增，**不会复用已回收的修订号**。

```bash
# 1. 准备：发布三个修订（rev 1/2 是历史修订，rev 3 为当前修订）
curl -s -X PUT --data-binary 'v1-aaaa' http://localhost:8000/objects/doc.txt
curl -s -X PUT --data-binary 'v2-bb'    http://localhost:8000/objects/doc.txt
curl -s -X PUT --data-binary 'v3-c'     http://localhost:8000/objects/doc.txt

# 2. 预览（dry_run 缺省即 true）：不改动任何数据
curl -s -X POST http://localhost:8000/gc -H 'Content-Type: application/json' \
  -d '{"paths": ["doc.txt"], "before": "2099-01-01T00:00:00Z", "dry_run": true}' \
  | python3 -m json.tool
# {
#   "dry_run": true,
#   "before": "2099-01-01T00:00:00+00:00",
#   "objects": [
#     {
#       "path": "doc.txt",
#       "candidates": [
#         {"revision_no": 1, "bytes": 7},
#         {"revision_no": 2, "bytes": 5}
#       ],
#       "releasable_bytes": 12,
#       "retained": [{"revision_no": 3, "reasons": ["current"]}]
#     }
#   ],
#   "total_releasable_bytes": 12
# }

# 3. 执行：同一事务内重新判定并删除，整批成功或回滚
curl -s -X POST http://localhost:8000/gc -H 'Content-Type: application/json' \
  -d '{"paths": ["doc.txt"], "before": "2099-01-01T00:00:00Z", "dry_run": false}' \
  | python3 -m json.tool
# {
#   "dry_run": false,
#   "before": "2099-01-01T00:00:00+00:00",
#   "deleted": [
#     {"path": "doc.txt", "revision_no": 1, "bytes": 7},
#     {"path": "doc.txt", "revision_no": 2, "bytes": 5}
#   ],
#   "deleted_count": 2,
#   "deleted_bytes": 12
# }

# 4. 重复执行返回空清单；当前修订不受影响；修订号继续递增
curl -s -X POST http://localhost:8000/gc -H 'Content-Type: application/json' \
  -d '{"paths": ["doc.txt"], "before": "2099-01-01T00:00:00Z", "dry_run": false}'
# {"dry_run":false,"before":"2099-01-01T00:00:00+00:00","deleted":[],"deleted_count":0,"deleted_bytes":0}
curl -s http://localhost:8000/objects/doc.txt          # v3-c
curl -si -X PUT --data-binary 'v4' http://localhost:8000/objects/doc.txt | grep -i x-revision-no
# X-Revision-No: 4        <- 不复用已回收的 1/2

# 5. 被复制会话引用的修订一律保留：先建档（清单钉住 rev 1），再发布 rev 2
curl -s -X PUT --data-binary 'alpha' http://localhost:8000/objects/src/a.txt
SID=$(curl -s -X POST http://localhost:8000/copy-sessions -H 'Content-Type: application/json' \
  -d '{"source_paths": ["src/a.txt"], "target_prefix": "backup/"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["session"])')
curl -s -X PUT --data-binary 'alpha-v2' http://localhost:8000/objects/src/a.txt

curl -s -X POST http://localhost:8000/gc -H 'Content-Type: application/json' \
  -d '{"paths": ["src/a.txt"], "before": "2099-01-01T00:00:00Z", "dry_run": true}' \
  | python3 -m json.tool
# "retained": [
#   {"revision_no": 1, "reasons": ["copy_manifest"]},   <- 清单钉住，不回收
#   {"revision_no": 2, "reasons": ["current"]}
# ]
# 执行也不会删除它们；会话分段拉取仍返回 alpha 快照字节
curl -s "http://localhost:8000/copy-sessions/$SID/segments" | python3 -m json.tool

# 6. 路径不存在：整次 404，不回收任何对象
curl -s -X POST http://localhost:8000/gc -H 'Content-Type: application/json' \
  -d '{"paths": ["doc.txt", "nope.txt"], "before": "2099-01-01T00:00:00Z"}'
# 404 {"detail":"object_not_found","missing":["nope.txt"]}

# 7. 已迁走路径（POST /moves）：无当前修订但历史仍在，未被会话引用的旧修订照常回收；
#    GET 仍为 404，而再次 PUT 的修订号从旧高水位继续（不复用）。
curl -s -X PUT --data-binary 'm1' http://localhost:8000/objects/mv.txt >/dev/null
curl -s -X PUT --data-binary 'm2' http://localhost:8000/objects/mv.txt >/dev/null
curl -s -X POST http://localhost:8000/moves -H 'Content-Type: application/json' \
  -d "{\"source_path\":\"mv.txt\",\"target_path\":\"mv-dst.txt\",
       \"source_etag\":\"$(printf m2 | sha256sum | cut -d' ' -f1 | sed 's/^/\"/;s/$/\"/')\",
       \"source_revision_no\":2}" >/dev/null
curl -s -X POST http://localhost:8000/gc -H 'Content-Type: application/json' \
  -d '{"paths": ["mv.txt"], "before": "2099-01-01T00:00:00Z", "dry_run": false}' \
  | python3 -m json.tool     # deleted: mv.txt rev 1 与 rev 2
curl -si -X PUT --data-binary 'm3' http://localhost:8000/objects/mv.txt | grep -i x-revision-no
# X-Revision-No: 3        <- 历史已清空也不复用 1/2
```

## 本地开发

```bash
pip install -r requirements.txt
export DATABASE_URL=postgresql://origin:origin@localhost:5432/origin
uvicorn app.main:app --reload
```

## 测试

```bash
pip install pytest httpx pgserver   # pgserver 提供免安装的本地 PostgreSQL
python -m pytest
```

- `tests/test_etags.py` / `test_preconditions.py` / `test_ranges.py`：纯逻辑单元测试（ETag 强弱比较、§13.2.2 求值顺序、范围解析与合并、边界确定性）。
- `tests/test_copy_cursor.py`：复制游标与清单摘要的纯逻辑单元测试（编解码往返、防篡改、确定性）。
- `tests/test_integration.py`：真实 PostgreSQL + 完整 HTTP 服务的端到端测试，覆盖 200/204/206/304/404/412/416、多段 multipart 确定性、`If-Range`、并发"仅创建"只有一个成功、以及流式传输期间发布新版本不混入内容。
- `tests/test_copy_sessions.py`：复制会话端到端测试，覆盖清单顺序与摘要、分段重复拉取字节一致、源对象发布新修订后仍返回快照内容、原子应用、冲突整批回滚与结构化结果、**建档后同字节重新发布仍判冲突（含已存在目标的同内容新修订、整批回滚、新会话恢复）**、升级前无基线会话返回 `baseline_missing` 不按摘要放行、重放返回同一批次结果、乱序/伪造游标拒绝、跨会话断点续传与冲突恢复、并发确认收敛，以及**确认与目标首次发布在空占位行上的交错：DB 层确定性复现两种先后顺序（PUT 先 -> 409 非 500、修订号不重复；确认先 -> PUT 等待并接续修订号）+ HTTP 屏障压测两者皆不出现 500/重复修订号**。
- `tests/test_gc.py`：历史修订回收端到端测试，覆盖请求校验（路径数 1～100、`before`、`dry_run`）、路径不存在整次 404 且执行路径不删任何修订（整批回滚）、空占位行视为不存在、预览分类与零副作用、截止时间的严格小于语义、路径去重与排序、执行后重放返回空清单、修订号不复用继续递增、清单/基线引用保留与原因合并、混合对象只回收未钉住修订且确认仍可应用，以及并发安全：**流式 GET 期间回收等待且不误删在读修订**、回收与 PUT 交错不删新修订且编号连续、回收与确认交错清单钉住的修订必存活、两个并发执行在锁内重判定收敛（每个修订只删一次）。
- `tests/test_moves.py`：对象路径迁移端到端测试，覆盖请求体校验（字段必填、路径非空且不同、强 ETag、正整数修订号）、成功迁移后目标以自身修订号提供相同内容/媒体类型而源 GET/HEAD 为 404、目标编号接续目标自身历史、404（源缺失/已迁走）/412（ETag、修订号或两者不符并回带当前校验值）/409（目标已有内容）均无部分生效、失败回滚后目标仍可 `If-None-Match: *` 首建、**已迁走路径再次 PUT 或作为目标复用时修订号只增不复用、且在 /gc 清空历史后仍由 `revision_seq` 保持单调**、迁移前会话仍可拉取固定源修订且变化的目标基线按原规则 409、/gc 只清理未被会话引用的旧修订而不误删钉住修订，以及并发：同源发散迁移恰一个成功（另一个 404）、争同一目标恰一个成功（另一个 409）、重叠锁集的交叉迁移不死锁且结果为合法串行历史、迁移与目标首发布交错（PUT 先则 409、迁移先则 PUT 接续修订 2，无 500/无重复编号）、流式 GET 期间迁移等待且不撕裂响应。

## 设计要点

- **数据模型**：`objects(path, current_id, revision_seq)` 指向 `revisions(id, object_path, revision_no, content bytea, sha256, media_type, created_at)`；`created_at` 默认 `date_trunc('second', now())`，保证秒级 `Last-Modified`；`revision_seq` 是每路径修订号高水位线，保证路径迁走并清空历史后编号仍单调不复用。
- **事务与锁**：GET/HEAD 在事务内 `SELECT ... FOR SHARE` 锁定对象行并读取当前修订，流式响应在事务提交前持续持有锁；PUT 与批次确认都是**先取对象行锁、再读修订**——先用不含 `revisions` 连接的纯 `FOR UPDATE` 锁定 `objects` 行（`current_id` 为 NULL 的空占位行也会被锁住），锁到手后才 JOIN 读取当前修订并 `max(revision_no)+1` 分配新修订号；并发发布串行执行，前置条件在锁内求值，因此"检查-然后-写"不会交错。
- **空占位行竞态**：建档为不存在的目标留下 `current_id` NULL 的占位行。确认若像旧实现那样"INNER JOIN `revisions` 后对对象加 `FOR UPDATE`"，空占位行会被连接过滤、一行都锁不住：与该路径首个 PUT 交错时双方可能各插修订号 1（唯一约束冲突导致确认 **500**）。修复把**取锁**与**读修订**拆成两条语句——先按路径升序纯 `FOR UPDATE` 锁住包括空占位在内的全部目标行，随后的连接读取不会阻塞、也不会在批次执行期间变陈旧；PUT 侧同样先锁行再读。于是：PUT 先提交时确认等待后看到新修订并报 `409`（冲突对象 + 恢复位置，整批回滚、游标不前进）；确认先持锁时 PUT 等待，确认提交修订 1 后 PUT 以 `max+1` 接续修订 2，双方不死锁。
- **流式隔离**：响应体由 `StreamingResponse` 从被锁定的修订分块读出（`substring(content ...)`），事务在最后一个字节发送后才提交，新修订的发布会等待锁释放，故响应内容、ETag、`Content-Range` 始终属于同一修订。
- **确定性 multipart**：边界 = `SHA-256(ETag | media-type | 升序合并后的区间)`，不含任何随机因素。
- **复制会话快照**：`copy_sessions` + `copy_session_entries` 在创建事务内先对源集与全部目标路径（先 `INSERT ... ON CONFLICT DO NOTHING` 补齐占位行）按全局路径升序 `FOR SHARE` 锁定，再在锁内读取源修订；清单钉住 `revision_id`（修订行永不更新；`/gc` 回收保留一切被清单/基线引用的修订），因此分段内容在会话生命周期内不可变。
- **目标修订基线**：建档时在同一锁快照中把每个目标路径的当前修订身份写入 `copy_session_target_baselines`，目标不存在记 NULL。确认时以修订 `id`（而非摘要）判定是否变更：摘要相同只能证明字节相同，不能区分"自基线未改动"与"基线后又发布了同字节修订"，后者必须报冲突。占位行保证"目标不存在"这一基线也被行锁覆盖，并发首发布会阻塞到建档事务提交。
- **确定性游标**：游标 = HMAC-SHA-256 签名的 `(会话, 段号, 清单摘要, 段末路径, 目标前缀, 源集摘要)`，是同一会话状态的纯函数——重发必相同、跨会话/跨清单必失效（`cursor_expired`）；签名密钥存于 `service_meta` 表，重启后游标仍有效。
- **原子批次确认**：确认在单事务内先用一条纯 `FOR UPDATE`（不连接 `revisions`）按目标路径升序锁定本批**全部** `objects` 行——建档留下的空占位行也在内——再用第二条语句读取当前修订（与建档的 `FOR SHARE`、并发发布及其它会话使用同一全局锁序，无死锁地串行化），逐目标与基线比较修订身份：基线不存在且仍不存在则创建、身份未变且摘要相同则幂等跳过、身份变化（含同字节重发，`reason=target_republished_same_content`）或摘要分歧（`content_diverged`）记为冲突；整批应用 + ack 记录 + 进度推进同事务提交，冲突则整批回滚后单独落库冲突结果。批次结果以**文本**形式存储（保持原始键序），重放与首次响应字节一致；`INSERT ... ON CONFLICT DO NOTHING` 使并发确认收敛到同一条结果。
- **升级兼容**：表结构由启动时的 `CREATE TABLE IF NOT EXISTS` 自动补齐，旧库重启即有基线表；升级前创建的未完成会话没有基线行，确认未完成段时返回 `409 baseline_missing` 且不推进游标（绝不按摘要放行），重新创建会话取得新基线后继续；已经持久化的历史确认结果仍优先重放，不受影响。
- **历史修订回收**：`POST /gc` 的执行路径在单事务内先用一条按路径升序的纯 `FOR UPDATE`（不连接 `revisions`、不补占位行）锁定全部请求对象行——与发布、建档（`FOR SHARE`）、确认共用同一全局锁序，彼此串行化而不死锁——再在锁内重新分类：只考虑 `created_at` 严格早于 `before` 的修订，且当前修订、被任何清单或基线引用的修订一律保留，其余同事务删除，任何失败整批回滚。因为重判定发生在锁内，并发 PUT、建档、确认或流式 GET 要么已提交（其结果对分类可见）、要么阻塞到回收提交，所以在用修订不会被误删、外键（`objects.current_id`、清单、基线）不会悬空。当前修订永远是最大修订号且必被保留，`max(revision_no) + 1` 的编号在回收后仍严格递增、不复用旧号。预览（`dry_run`）则在 `REPEATABLE READ` 只读快照中完成存在性检查与分类，不阻塞任何写者。**路径迁移后**，源路径的对象行仍在但 `current_id` 为 NULL：存在性判定把"`current_id` 非空 **或** `revision_seq > 0`"视为存在，所以迁走的路径（包括历史已被全部回收的）始终是合法回收对象、重复执行返回空清单；只有从未有过内容的空占位行（`revision_seq = 0`）才报 404。
- **对象路径迁移**：`POST /moves` 在单事务内先为两端（按需）插入占位行，再用一条按路径升序的纯 `FOR UPDATE` 同时锁住源与目标的 `objects` 行——与 PUT、建档、确认、回收完全相同的全局锁序，交叉/交换端点的迁移也按同一顺序取锁，串行化而不死锁——锁到手后才 LEFT JOIN `revisions` 读取两端当前修订并按 **404（源无当前修订）→ 412（源强 ETag 或修订号不符）→ 409（目标已有内容）** 顺序判定；全部满足时，把源当前修订的**字节与媒体类型**经 `publish_revision` 作为目标的**全新修订**插入（新的修订 `id`，修订号按目标自身历史 `max+1`），再把源行 `current_id` 置 NULL 摘除——插入新修订与摘除源在同一事务提交，任何失败整批回滚，外部永远看不到部分迁移。迁移不复制修订行本身：它在目标路径新建修订，因此源路径的旧修订行保持原样、仍被迁移前的复制会话清单/基线钉住，`/gc` 据此正常保留。每路径的 `objects.revision_seq` 高水位线在每次发布时随 `revision_no` 一起推进；摘除源行不清零。发布编号取 `GREATEST(revision_seq, max(revision_no)) + 1`，于是已迁走路径（行在但 `current_id` 为 NULL，且其旧修订可被 `/gc` 全部删除）再次 PUT 或作为迁移目标复用时，编号只从旧高水位 +1 继续——绝不回退、绝不复用。
