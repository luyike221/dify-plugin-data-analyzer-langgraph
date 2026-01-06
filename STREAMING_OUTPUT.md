# 流式输出实现说明

## 实现方式

### 当前实现

由于 LangGraph 的节点函数是**同步的**，无法在节点执行过程中实时 yield。因此采用以下方案：

1. **节点内部流式收集**
   - 节点函数中使用 `call_llm(stream=True, stream_callback=...)` 
   - 实时收集每个 token 到 `stream_chunks` 列表
   - 节点执行完成后，将 `stream_chunks` 添加到 `stream_output`

2. **LangGraph 流式传递**
   - 节点函数返回后，LangGraph 的 `stream()` 方法会实时传递状态更新
   - `analyze_stream()` 方法逐个 yield `stream_output` 中的每个 chunk
   - 用户可以看到实时的流式输出

### 工作流程

```
节点函数执行
    ↓
call_llm(stream=True)
    ↓
实时收集 token → stream_chunks = ["token1", "token2", ...]
    ↓
节点函数返回
    ↓
stream_output = ["标题"] + stream_chunks + ["格式化结果"]
    ↓
LangGraph stream() 传递状态更新
    ↓
analyze_stream() 逐个 yield stream_output 中的 chunk
    ↓
用户看到实时输出
```

## 流式输出的节点

以下节点已支持流式输出：

1. **analyze_intent_node** - 意图分析节点
   - 流式输出 LLM 的意图分析过程
   - 输出重写后的需求和策略

2. **generate_code_node** - 代码生成节点
   - 流式输出代码生成过程
   - 输出最终生成的代码

3. **fix_code_node** - 代码修复节点
   - 流式输出代码修复过程
   - 输出修复后的代码

4. **generate_report_node** - 报告生成节点
   - 流式输出报告生成过程
   - 输出最终的分析报告

## 输出格式

每个节点的流式输出包含：

1. **标题**：说明当前正在执行的操作
2. **流式内容**：LLM 生成的 token（逐个传递）
3. **格式化结果**：解析后的结构化内容

例如代码生成节点：
```
📝 **正在生成分析代码...**

[流式 token 1]
[流式 token 2]
[流式 token 3]
...

📝 **生成的分析代码：**

```python
import pandas as pd
...
```
```

## 注意事项

### 实时性限制

由于 LangGraph 节点函数的同步特性：
- **节点执行期间**：无法实时 yield（必须等待节点完成）
- **节点完成后**：LangGraph 会立即传递所有流式输出
- **用户感知**：看到的是节点完成后的流式输出，不是逐 token 的实时输出

### 优化建议

如果需要真正的逐 token 实时输出，可以考虑：

1. **使用异步节点**（如果 LangGraph 支持）
2. **使用线程/进程**在节点执行过程中实时更新状态
3. **使用 LangGraph 的 astream_events**（如果可用）

### 当前效果

虽然不能实现真正的逐 token 实时输出，但：
- ✅ 用户可以看到 LLM 的完整生成过程
- ✅ 输出是流式的，不是一次性显示
- ✅ 每个节点的输出都会实时传递
- ✅ 支持打字机效果（在节点完成后）

## 使用示例

```python
# 流式执行分析
graph = DataAnalysisGraph()
for chunk in graph.analyze_stream(
    workspace_dir="...",
    csv_path="...",
    user_prompt="分析数据",
    api_url="...",
    model="qwen3-32b",
    ...
):
    print(chunk, end="", flush=True)  # 实时输出
```

## 配置

流式输出使用与代码生成相同的 LLM 配置：
- `api_url`: LLM API 地址
- `model`: 模型名称（如 qwen3-32b）
- `api_key`: API 密钥
- `temperature`: 生成温度

所有节点都使用流式调用，确保输出的一致性。

