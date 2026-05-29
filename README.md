# pasdns

一个运行在客户端的跨平台高性能 DNS 代理，基于 Hickory-DNS 构建。

## 特性

- 高性能 — 基于 Hickory-DNS，支持 UDP/TCP 协议，fastrace 追踪
- 转发策略 — 轮询（Round Robin）、最快响应（Fastest）、近 10 次最低平均延迟（Min-Avg-10）
- 灵活的路由 — 域名匹配（精确/通配符/关键字/正则）+ Geosite 分组
- 多上游分组 — 每个分组包含一批 DNS 上游，支持域名到分组的多对多映射
- 乐观缓存 — 基于 Moka 的高性能缓存，支持过期后继续返回旧结果
- 退出持久化 — 使用 rkyv 零拷贝序列化，自动捕获关机/注销/退出信号，保存缓存到磁盘
- SOCKS5 代理 — 可选的 SOCKS5 代理支持，用于上游 DNS 连接

## 快速开始

从 Release 中下载可执行文件；在目录下创建 `pasdns.toml` 配置文件，然后运行：

```bash
pasdns
```

## 配置示例

完整的 `pasdns.toml` 配置文件：

```toml
# ---------- 服务端配置 ----------
[server]
# 监听地址列表，可同时监听多个地址
# 支持 IPv4 和 IPv6
listen = ["0.0.0.0:53"]

# 支持的协议列表
# 可选值: "udp", "tcp", "dot" (DNS-over-TLS), "doh" (DNS-over-HTTPS)
# 注意: dot 和 doh 目前尚未实现
protocol = ["udp", "tcp"]

# ---------- Geosite 数据路径 ----------
# geosite.dat 文件的路径，用于基于地理位置的域名分组。如果不设置或文件不存在，则跳过 geosite 规则
geosite_path = "geosite.dat"

# ---------- SOCKS5 代理配置 ----------
# 用于连接上游 DNS 服务器的代理，默认禁用代理（直连）
[proxy]
# 是否启用代理
enabled = false
# SOCKS5 代理地址
addr = "127.0.0.1:1080"

# ---------- 上游分组配置 ----------
# 每个分组定义一批 DNS 上游服务器和一个转发策略
# 可以将域名通过路由规则定向到特定分组

[[group]]
name = "default"
# 转发策略:
#   "round-robin"  — 轮询（默认）
#   "fastest"      — 选择最近一次响应最快的上游
#   "min-avg-10"   — 选择近 10 次请求平均延迟最低的上游
strategy = "round-robin"
# 上游地址列表，支持简写和详细格式
upstreams = [
  "8.8.8.8:53",                              # 简写格式，默认使用 UDP
  { addr = "1.1.1.1:53", protocol = "tcp" }, # 详细格式，指定协议
]

[[group]]
name = "china"
strategy = "fastest"
upstreams = ["223.5.5.5:53", "119.29.29.29:53"]

[[group]]
name = "secure"
strategy = "min-avg-10"
upstreams = [
  { addr = "9.9.9.9:53", protocol = "tcp" },
  { addr = "1.0.0.1:53", protocol = "tcp" },
]

# ---------- 路由规则 ----------
# 按顺序匹配，第一个命中的规则生效
# 未命中任何规则的域名使用第一个分组（或名为 "default" 的分组）

[[route]]
# 手写域名列表
# 支持精确匹配和通配符匹配
# "example.com" 会同时匹配 "example.com" 和 "*.example.com"
# "*.google.com" 会匹配 "google.com" 和所有子域名
domains = ["baidu.com", "*.baidu.com", "taobao.com"]
# Geosite 标签列表（需要配置 geosite_path）
geosite = ["cn"]
# 命中后使用的上游分组
group = "china"

[[route]]
domains = ["google.com", "*.google.com", "youtube.com"]
group = "secure"

[[route]]
geosite = ["google", "facebook"]
group = "secure"

# ---------- 缓存配置 ----------
[cache]
# 是否启用缓存
enabled = true
# 最大缓存条目数
max_entries = 100000
# 默认 TTL（秒），仅在 DNS 响应中无 TTL 时使用
default_ttl_secs = 36000
# Serve Stale 过期时间（秒）
# 缓存条目过期后，在此时间窗口内仍会返回旧结果（乐观缓存）
serve_stale_secs = 300

[cache.persistence]
# 是否启用持久化
# 启用后，关机时会将缓存保存到磁盘，下次启动时自动恢复
enabled = true
# 缓存文件路径
path = "pasdns.cache"
```

## 工作原理

```
客户端请求 → pasdns 监听 (UDP/TCP)
    ↓
路由匹配（域名 → 上游分组）
    ↓
缓存查找（乐观缓存：命中则直接返回）
    ↓
转发策略选择上游服务器
    ↓
通过代理（可选）转发到上游 DNS
    ↓
缓存结果 → 返回给客户端
```

## 许可证

MIT
