# Large File Archive API

基于 FastAPI 的大文件归档服务：分片断点续传、逐片 SHA-256 校验、并发安全的原子合并、
整文件哈希校验发布、内容寻址去重（重复文件直接复用已有对象），以及基于一次性 token 的
**受控分享下载**（有效期 + 最大下载次数，事务内原子扣减）。元数据存 SQLite，
分片与归档对象存本地卷，启动时自动初始化数据库。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# API 监听 http://localhost:8000 ，交互文档 http://localhost:8000/docs
```

数据持久化在命名卷 `archive-data`（容器内 `/data`）。

### 本地开发

```bash
pip install -r requirements-dev.txt
DATA_DIR=./data uvicorn app.main:app --reload --port 8000
pytest                 # 运行测试（每个用例使用独立临时数据目录）
```

## 上传流程

```
客户端                                服务端
  |  POST /v1/sessions  (声明总分片数、整文件 SHA-256)  |
  | --------------------------------------------------> |  若哈希已存在 → 秒传，直接 completed
  |  PUT /v1/sessions/{id}/chunks/{i}  (乱序/可重复)    |
  |  X-Chunk-SHA256: <该分片哈希>                        |
  | --------------------------------------------------> |  校验分片哈希+大小，落盘 chunks/{id}/
  |  GET  /v1/sessions/{id}  (断点查询：缺哪些分片)      |
  | --------------------------------------------------> |
  |  POST /v1/sessions/{id}/complete                    |
  | --------------------------------------------------> |  原子合并 → 校验整文件哈希 → 发布/去重
  |  GET  /v1/archives/{sha256}/download                |
  | --------------------------------------------------> |
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/v1/sessions` | 创建上传会话（声明 `total_size` / `chunk_size` / `total_chunks` / `file_sha256`）；哈希已存在时秒传完成 |
| `PUT` | `/v1/sessions/{id}/chunks/{i}` | 上传分片（body 为原始字节，请求头 `X-Chunk-SHA256` 必填）；乱序、重复上传均安全 |
| `GET` | `/v1/sessions/{id}` | 断点查询：已收/缺失分片清单、状态、上次错误 |
| `POST` | `/v1/sessions/{id}/complete` | 触发合并发布；并发调用只有一个生效，其余返回 409；已完成时幂等返回 |
| `DELETE` | `/v1/sessions/{id}` | 清理会话：删除暂存分片并释放归档引用（引用计数归零才删对象） |
| `GET` | `/v1/archives` | 列出归档对象 |
| `GET` | `/v1/archives/{sha256}` | 归档元数据 |
| `GET` | `/v1/archives/{sha256}/download` | 下载归档（流式，原始端点，始终保留、不受次数限制） |
| `POST` | `/v1/archives/{sha256}/shares` | 创建受控分享（body：`expires_in` 秒、`max_downloads`）；返回仅展示一次的不可逆 token |
| `GET` | `/v1/shares/{token}` | 查询分享状态（`active` / `expired` / `exhausted` / `revoked`、剩余次数） |
| `GET` | `/v1/shares/{token}/download` | 分享下载：事务内校验未过期/未撤销/有剩余次数并原子扣减，随后流式下载；响应头 `X-Share-Remaining-Downloads` 给出剩余次数 |
| `DELETE` | `/v1/shares/{token}` | 撤销分享（幂等；重复撤销返回 `revoked:false`） |
| `GET` | `/healthz` | 健康检查 |

### 分享错误码

| 场景 | 状态码 | detail |
| --- | --- | --- |
| 归档不存在（创建分享时） | `404` | `archive not found` |
| 分享不存在/token 错误 | `404` | `share not found` |
| 分享已过期 | `410` | `share has expired` |
| 下载次数耗尽 | `410` | `share download limit reached` |
| 分享已撤销 | `410` | `share has been revoked` |

注意：只有校验通过、成功认领下载时才扣减次数；过期/耗尽/撤销导致的失败**不扣次数**。
归档被删除（引用计数归零）时，其分享通过外键级联一并删除，token 随即返回 404。

### curl 示例

```bash
# 1. 创建会话（total_chunks 必须等于 ceil(total_size / chunk_size)）
SID=$(curl -s -X POST localhost:8000/v1/sessions -H 'Content-Type: application/json' -d '{
  "file_name": "big.bin", "total_size": 5242880,
  "chunk_size": 1048576, "total_chunks": 5,
  "file_sha256": "<整文件sha256>"
}' | jq -r .session_id)

# 2. 乱序上传分片（重复上传同哈希分片返回 duplicate:true，幂等）
split -b 1048576 -d big.bin part_
for i in 4 2 0 3 1; do
  curl -X PUT localhost:8000/v1/sessions/$SID/chunks/$i \
    -H "X-Chunk-SHA256: $(sha256sum part_0$i | cut -d' ' -f1)" \
    --data-binary @part_0$i
done

# 3. 断点查询 → 4. 合并发布 → 5. 下载
curl localhost:8000/v1/sessions/$SID
curl -X POST localhost:8000/v1/sessions/$SID/complete
curl -OJ localhost:8000/v1/archives/<整文件sha256>/download

# 6. 清理会话（释放归档引用）
curl -X DELETE localhost:8000/v1/sessions/$SID
```

### 受控分享下载示例

```bash
SHA=<整文件sha256>

# 创建分享：1 小时内有效、最多下载 3 次。token 只在响应里出现这一次，务必保存
TOKEN=$(curl -s -X POST localhost:8000/v1/archives/$SHA/shares \
  -H 'Content-Type: application/json' \
  -d '{"expires_in": 3600, "max_downloads": 3}' | jq -r .token)

# 凭 token 下载（响应头 X-Share-Remaining-Downloads 为剩余次数）
curl -OJ localhost:8000/v1/shares/$TOKEN/download

# 查询状态：state ∈ active / expired / exhausted / revoked
curl localhost:8000/v1/shares/$TOKEN

# 撤销分享（幂等）
curl -X DELETE localhost:8000/v1/shares/$TOKEN
```

## 设计要点

**数据布局**（`DATA_DIR`，默认容器内 `/data`）

```
archive.db                 SQLite（WAL 模式），含 sessions/chunks/archives/shares 表
chunks/<session>/<i>.part  各会话的分片暂存区
objects/<aa>/<sha256>      内容寻址的归档对象（去重）
tmp/<uuid>.tmp             上传/合并临时文件（与 objects 同卷，保证 rename 原子）
```

**完整性与一致性**

- 分片上传：流式落盘同时计算 SHA-256，与 `X-Chunk-SHA256` 及声明大小双重校验，
  任一不符即拒绝并删除临时文件；同哈希重复上传直接幂等返回（不重写磁盘）。
- 合并发布：`complete` 先在单条 `BEGIN IMMEDIATE` 事务里把会话从 `active` 翻到
  `merging`（并发下只有一个请求成功，其余 409）；合并本身在事务外按序拼接分片到
  临时文件并计算整文件哈希；**仅当整文件哈希与声明一致**才 `os.replace` 原子改名
  发布，随后在单个事务内完成「归档 upsert + 会话置 completed + 清理分片记录」。
  哈希不匹配 → 422，会话回退 `active`，客户端可重传后重试。
- 去重：归档以内容 SHA-256 为主键 + 引用计数。创建会话时哈希已存在 → 秒传；
  合并后发现同内容对象 → 直接复用（`deduplicated: true`）。`DELETE` 会话释放引用，
  计数归零才删除底层对象，共享文件互不影响。
- 崩溃恢复：启动时把残留的 `merging` 会话重置为 `active`（此时不可能有合并在进行），
  并清理无属主的分片目录；SQLite 自动建库建表（含 `shares` 表），无需迁移工具。
- 受控分享：创建分享时生成 32 字节 CSPRNG 随机 token（URL-safe），数据库只存它的
  SHA-256（`token_hash` 主键）——即使库文件泄露也无法还原 token，原始 token 仅在创建
  响应中返回一次。`GET /v1/shares/{token}/download` 在单个 `BEGIN IMMEDIATE` 事务内
  执行一条带全部前置条件的条件更新：
  `UPDATE ... SET downloads_used=downloads_used+1 WHERE token_hash=? AND revoked=0
  AND expires_at > ? AND downloads_used < max_downloads`。
  `rowcount=0` 即校验失败，事务回滚、**不扣次数**，再按撤销/过期/耗尽返回明确的 410
  错误；`rowcount=1` 则扣减已随提交生效（WAL + 行级写锁串行化，40 路并发也绝不会
  超过 `max_downloads`），之后才开始流式传输文件。撤销只置 `revoked=1`，幂等；
  `shares` 通过外键 `ON DELETE CASCADE` 跟随归档删除；所有分享状态落 SQLite，
  重启后有效期、剩余次数、撤销状态全部保留。

**约束与说明**

- `chunks/`、`objects/`、`tmp/` 必须位于同一文件系统（单卷挂载），原子改名依赖于此。
- 分片大小由客户端在创建会话时约定，除最后一片外每片必须恰好 `chunk_size` 字节。
- 断点查询返回完整分片清单，分片数极大（百万级）时响应体会相应增大。
- 极端情况下（发布对象后、提交事务前进程崩溃）可能留下无引用的对象文件，
  不影响正确性，可定期按 `archives` 表对账清理。

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `./data`（容器内 `/data`） | 数据根目录 |
| `DB_PATH` | `$DATA_DIR/archive.db` | SQLite 文件路径 |

## 项目结构

```
app/
  main.py             应用入口（lifespan：建目录、初始化 DB、崩溃恢复）
  config.py           环境配置
  db.py               SQLite 连接 / 事务 / schema
  schemas.py          Pydantic 模型
  service.py          合并、恢复等核心逻辑
  storage.py          文件系统布局
  routers/sessions.py 会话与分片端点
  routers/archives.py 归档查询、原始下载端点、创建分享
  routers/shares.py   受控分享：状态查询、原子认领下载、撤销
tests/test_api.py     端到端测试（乱序/重复/断点/哈希校验/去重/清理）
tests/test_shares.py  分享测试（次数/过期/撤销/并发抢占/重启持久化/级联删除）
Dockerfile / docker-compose.yml
```
