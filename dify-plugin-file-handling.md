# Dify 插件文件接收完全指南

> 基于实战项目整理，覆盖「声明参数 → 接收对象 → 下载内容 → 处理文件」全流程，附常见坑点说明。

---

## 一、整体流程概览

```
用户在 Dify UI 上传文件
       ↓
Dify 平台保存文件到对象存储，生成带签名的预览 URL
       ↓
插件被调用时，Dify 通过 tool_parameters 传入 File 对象
       ↓
插件拿到 File 对象后，通过 .url 属性构建下载地址
       ↓
插件发 HTTP GET 下载文件字节流，得到 bytes
       ↓
用 bytes 做业务处理（解析 Excel、读取 PDF 等）
```

Dify 插件**不会**直接给你文件的字节数据，它只给你一个带签名 URL 的 **File 对象**，你需要自己发 HTTP 请求把文件内容下载下来。

---

## 二、第一步：在 YAML 中声明文件参数

插件的工具参数通过 `tools/<tool-name>.yaml` 声明。文件类型参数写法如下：

```yaml
# tools/my-tool.yaml
parameters:
  - name: input_file
    type: file          # ← 关键：类型设为 file
    required: true
    label:
      en_US: Upload File
      zh_Hans: 上传文件
    human_description:
      zh_Hans: "上传需要处理的文件"
    llm_description: "The file to process."
    form: form          # form 表示由用户在表单中手动选择；llm 表示由 LLM 决定
```

**`type: file` 的效果**：Dify UI 会渲染一个文件上传组件，用户上传后平台会把文件封装成 File 对象传给插件。

---

## 三、第二步：在 Python 中接收 File 对象

```python
from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage
from typing import Any

class MyTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any], ...):
        # 从参数字典中取出文件对象
        input_file = tool_parameters.get("input_file")
        
        # input_file 是 dify_plugin 的 File 对象（不是 bytes，不是路径）
        print(type(input_file))  # <class 'dify_plugin.entities.tool.tool_file.ToolFile'>
```

---

## 四、Dify File 对象的属性

Dify 的 File 对象包含以下标准属性：

| 属性 | 类型 | 说明 | 示例 |
|---|---|---|---|
| `url` | `str` | 文件预览/下载 URL（带签名，**可能是相对路径**） | `localhost/files/xxx/file-preview?sign=...` |
| `filename` | `str` | 原始文件名 | `报告数据.xlsx` |
| `mime_type` | `str` | MIME 类型 | `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` |
| `extension` | `str` | 文件扩展名（不带点） | `xlsx` |
| `size` | `int` | 文件大小（字节） | `12846` |
| `type` | `str` | 文件大类 | `document` / `image` |

```python
# 读取属性示例
print(input_file.filename)   # 报告数据.xlsx
print(input_file.mime_type)  # application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
print(input_file.size)       # 12846
print(input_file.url)        # localhost/files/a5bd82e5-.../file-preview?timestamp=...&sign=...
```

---

## 五、第三步：通过 URL 下载文件内容

### ⚠️ 核心坑点：URL 是「主机名+路径」的半相对格式

Dify 返回的 `url` 通常**不带协议**，格式为：

```
localhost/files/a5bd82e5-3763-4d1f-9f21-a8480d03132f/file-preview?timestamp=1773325569&nonce=xxx&sign=xxx
```

注意：这里 `localhost` 是**主机名**，不是路径的一部分。你不能直接用 `requests.get(url)` 去请求，需要先拼出完整的带协议地址。

### 正确的 URL 构建逻辑

```python
import os
import requests

def download_dify_file(dify_file) -> bytes:
    url = dify_file.url
    
    # 情况1：已经是绝对 URL（带 http:// 或 https://），直接用
    if url.startswith("http://") or url.startswith("https://"):
        full_url = url
    else:
        # 情况2：相对路径，需要拼上基础 URL
        # 优先级：FILES_URL > DIFY_FILES_BASE_URL > DIFY_API_BASE_URL（去掉 /v1）
        base = (
            os.environ.get("FILES_URL")
            or os.environ.get("DIFY_FILES_BASE_URL")
            or os.environ.get("DIFY_API_BASE_URL")
        )
        
        if not base:
            raise ValueError("需要配置 FILES_URL 或 DIFY_API_BASE_URL")
        
        if not base.startswith("http"):
            base = f"http://{base}"
        
        base = base.rstrip("/")
        
        # 重点：DIFY_API_BASE_URL 通常是 http://localhost/v1
        # 文件接口在 /files/ 下，不在 /v1/ 下，必须去掉 /v1
        if base.endswith("/v1"):
            base = base[:-3]  # → http://localhost
        
        # 拆分 "localhost/files/xxx" → host="localhost", path="/files/xxx"
        # 只保留 path 部分与 base 拼接
        if "/" in url and not url.startswith("/"):
            path = "/" + url.split("/", 1)[1]  # → "/files/a5bd82e5-.../file-preview?..."
        else:
            path = url if url.startswith("/") else "/" + url
        
        full_url = base + path  # → "http://localhost/files/a5bd82e5-.../file-preview?..."
    
    # 发送 HTTP 请求下载
    response = requests.get(full_url, timeout=30)
    response.raise_for_status()
    return response.content  # bytes
```

### 为什么文件 URL 不在 `/v1` 下？

```
http://localhost/v1/...        ← Dify REST API（对话、工作流、插件调用等）
http://localhost/files/...     ← Dify 文件服务（由 Nginx 直接代理）
```

这是 Dify 架构上的分层设计。文件的上传/预览/下载走的是独立的文件服务路由，不经过 API 路由层。

---

## 六、完整示例代码

```python
import os
import requests
from typing import Any
from collections.abc import Generator
from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage


class FileDemoTool(Tool):
    
    def _is_dify_file(self, obj: Any) -> bool:
        """判断是否是 Dify File 对象"""
        if obj is None:
            return False
        # 标准判断：同时有 url 和 filename 属性
        if hasattr(obj, "url") and hasattr(obj, "filename"):
            return True
        # 备用判断：类名包含 File 且来自 dify_plugin 模块
        type_str = str(type(obj))
        return "dify_plugin" in type_str and "File" in type_str
    
    def _build_file_url(self, raw_url: str) -> str:
        """将 Dify 返回的相对 URL 转为可访问的完整 URL"""
        if raw_url.startswith("http://") or raw_url.startswith("https://"):
            return raw_url  # 已是绝对路径
        
        base = (
            os.environ.get("FILES_URL")
            or os.environ.get("DIFY_FILES_BASE_URL")
            or os.environ.get("DIFY_API_BASE_URL", "http://localhost")
        )
        if not base.startswith("http"):
            base = f"http://{base}"
        base = base.rstrip("/")
        if base.endswith("/v1"):
            base = base[:-3]
        
        if "/" in raw_url and not raw_url.startswith("/"):
            path = "/" + raw_url.split("/", 1)[1]
        else:
            path = raw_url if raw_url.startswith("/") else "/" + raw_url
        
        return base + path
    
    def _download_file(self, dify_file: Any) -> tuple[bytes, str]:
        """
        下载文件，返回 (文件字节, 文件名)
        """
        # 1. 获取文件名
        filename = getattr(dify_file, "filename", None) \
            or getattr(dify_file, "name", "uploaded_file")
        
        # 如果文件名没有扩展名，从 extension 属性补充
        ext = getattr(dify_file, "extension", None)
        if ext and not filename.endswith(f".{ext}"):
            filename = f"{filename}.{ext}"
        
        # 2. 构建完整 URL
        raw_url = dify_file.url
        full_url = self._build_file_url(raw_url)
        
        # 3. 下载文件内容
        response = requests.get(full_url, timeout=30)
        response.raise_for_status()
        
        return response.content, filename
    
    def _invoke(
        self,
        tool_parameters: dict[str, Any],
    ) -> Generator[ToolInvokeMessage, None, None]:
        
        input_file = tool_parameters.get("input_file")
        
        if not input_file:
            yield self.create_text_message("❌ 请上传文件")
            return
        
        # 兼容多种输入形式
        if self._is_dify_file(input_file):
            # 标准 Dify 文件对象（最常见）
            file_bytes, filename = self._download_file(input_file)
        
        elif isinstance(input_file, str) and os.path.exists(input_file):
            # 本地文件路径（调试时可能出现）
            with open(input_file, "rb") as f:
                file_bytes = f.read()
            filename = os.path.basename(input_file)
        
        elif hasattr(input_file, "read"):
            # 类文件对象
            file_bytes = input_file.read()
            filename = getattr(input_file, "filename", "file")
        
        else:
            yield self.create_text_message(f"❌ 不支持的文件类型: {type(input_file)}")
            return
        
        # 此处 file_bytes 是 bytes，filename 是字符串
        # 可以用于后续处理，例如：
        # import openpyxl, io
        # wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
        
        yield self.create_text_message(
            f"✅ 文件接收成功\n文件名: {filename}\n大小: {len(file_bytes)/1024:.1f} KB"
        )
```

---

## 七、环境变量配置

在 `.env` 文件中配置文件服务的基础地址：

```bash
# 方案一：直接配置文件服务根地址（推荐）
FILES_URL=http://localhost

# 方案二：Dify API 地址（插件会自动去掉 /v1）
DIFY_API_BASE_URL=http://localhost/v1

# 如果 Dify 部署在其他地址
# FILES_URL=https://your-dify.example.com
# DIFY_API_BASE_URL=https://your-dify.example.com/v1
```

**优先级**：`FILES_URL` > `DIFY_FILES_BASE_URL` > `DIFY_API_BASE_URL`（自动去掉 `/v1`）

---

## 八、常见问题排查

### 问题1：404 NOT FOUND，URL 变成 `http://localhost/v1/localhost/files/...`

**原因**：直接用了 `DIFY_API_BASE_URL`（含 `/v1`）拼接，且没有去掉主机名部分。  
**解决**：使用上文的 `_build_file_url` 方法，它会自动去掉 `/v1` 并拆分主机名。

### 问题2：连接超时或拒绝连接

**原因**：插件和 Dify 不在同一网络，或 `localhost` 解析不到 Dify 服务。  
**解决**：把 `FILES_URL` 改为 Dify 实际的 IP 或域名，例如 `http://172.18.0.1`（Docker bridge 网关 IP）。

### 问题3：签名失效（403 或 401）

**原因**：带签名的 URL 有时效性，超时后无法使用。  
**解决**：在插件被调用时立即下载，不要缓存 URL 后延迟使用。

### 问题4：文件名没有扩展名

**原因**：`filename` 属性有时只有基础名称，没有扩展名。  
**解决**：优先读 `filename`，再从 `extension` 属性补充扩展名：

```python
ext = getattr(dify_file, "extension", None)
if ext and not filename.endswith(f".{ext}"):
    filename = f"{filename}.{ext}"
```

---

## 九、架构总结图

```
┌──────────────────────────────────────────────────┐
│                  Dify 平台                        │
│                                                  │
│  用户上传文件 → 保存到对象存储                      │
│                    ↓                             │
│  生成带签名 URL: localhost/files/xxx/file-preview  │
│                    ↓                             │
│  Tool 调用时注入 File 对象到 tool_parameters       │
└──────────────────────────┬───────────────────────┘
                           │  File 对象（含 .url .filename 等）
                           ↓
┌──────────────────────────────────────────────────┐
│                  插件（Plugin）                   │
│                                                  │
│  1. tool_parameters.get("input_file")            │
│  2. 判断是 Dify File 对象                         │
│  3. 读取 .url，构建完整 HTTP 地址                  │
│     "localhost/files/..." → "http://localhost/files/..."│
│  4. requests.get(full_url) → bytes               │
│  5. 用 bytes 做业务处理                           │
└──────────────────────────────────────────────────┘
```

---

## 十、快速检查清单

- [ ] `tools/<tool>.yaml` 中参数 `type` 设为 `file`
- [ ] Python 代码从 `tool_parameters.get("input_file")` 取对象
- [ ] 判断对象类型用 `hasattr(obj, "url") and hasattr(obj, "filename")`
- [ ] 构建 URL 时去掉 `/v1` 后缀，拆分主机名只取 path 部分
- [ ] `.env` 中配置了 `FILES_URL` 或 `DIFY_API_BASE_URL`
- [ ] 下载用 `requests.get(full_url, timeout=30)`，调用 `raise_for_status()`




详细分析：为什么 URL 会错？
1. Dify 返回的文件 URL 是什么格式？
当 Dify 向插件传递文件对象时，dify_file.url 属性返回的是一个不带协议的相对路径，形如：

localhost/files/a5bd82e5-3763-4d1f-9f21-a8480d03132f/file-preview?timestamp=...&sign=...
注意这里有个容易混淆的地方：localhost 出现在路径里，但它其实是「主机名」，不是路径的一部分。完整理解是：

部分	值
主机名	localhost
路径	/files/a5bd82e5-.../file-preview
查询参数	?timestamp=...&sign=...
2. 旧代码为什么出错？
旧代码的逻辑是：

# 这个 URL 不以 http 开头，进入相对路径分支
url = "localhost/files/a5bd82e5-.../file-preview?..."
# 取 DIFY_API_BASE_URL = "http://localhost/v1"
files_base_url = "http://localhost/v1"
# 直接把 url 拼到后面（只确保 url 以 / 开头）
url = "/" + url   # → "/localhost/files/..."
full_url = files_base_url + url
# → "http://localhost/v1/localhost/files/..."  ← 错了！
错误核心：代码把 localhost/files/... 当成了「纯路径字符串」，在前面加了个 /，然后直接拼上去。结果是 localhost 既出现在基础 URL 里（作为主机名），又出现在路径里（作为路径片段）。

3. 文件 URL 和 API URL 根本不是同一个端点
这是另一个关键问题：

DIFY_API_BASE_URL = http://localhost/v1     ← 这是 Dify REST API 的入口
文件预览地址     = http://localhost/files/... ← 这是 Dify Web 服务的文件接口
文件预览（/files/...）是 Dify Nginx/Web 层直接提供的静态/代理路由，不经过 /v1 API 路由。如果用 http://localhost/v1 去拼文件路径，天然就错了。

4. 新代码如何修复？
新逻辑做了两件事：

第一步：去掉 /v1 后缀，取到 Web 根

files_base_url = "http://localhost/v1"
if files_base_url.endswith("/v1"):
    files_base_url = files_base_url[:-3]
# files_base_url → "http://localhost"
第二步：正确拆分「主机名+路径」

url = "localhost/files/a5bd82e5-.../file-preview?..."
# url 包含 / 且不以 / 开头 → 说明第一段是主机名，/ 后面才是真正的路径
if "/" in url and not url.startswith("/"):
    path = "/" + url.split("/", 1)[1]
    # path → "/files/a5bd82e5-.../file-preview?..."
full_url = files_base_url + path
# → "http://localhost/files/a5bd82e5-.../file-preview?..."  ✅
5. 结合你的网络环境理解
从 ip addr 可以看到你的环境：

eth0: 172.20.62.200   ← WSL2 宿主机 IP
br-8de679250c18: 172.18.0.1  ← Docker bridge 1（Dify 服务所在网络）
br-5a965d72e775: 172.19.0.1  ← Docker bridge 2
Dify 跑在 Docker 里，通过 localhost 映射到宿主机（WSL2 层）的 80 端口（Nginx）。插件进程跑在 WSL2 宿主机上，访问 http://localhost 等同于访问 Docker Nginx，这是通的。

所以修复后最终请求的 URL：

http://localhost/files/a5bd82e5-3763-4d1f-9f21-a8480d03132f/file-preview?timestamp=...&sign=...
这条路径由 Dify 的 Nginx 处理，会转发给文件服务，返回文件内容，完全正确。

6. 一句话总结
Dify 插件 SDK 给到的 file.url 是 主机名/路径 格式的半相对 URL，旧代码把整个字符串当路径直接拼接，同时还带着 /v1 前缀，导致请求地址变成了 http://localhost/v1/localhost/files/...，双重错误。新代码正确剥离主机名部分只取路径，并自动去掉 API 前缀 /v1，生成正确的文件下载地址。