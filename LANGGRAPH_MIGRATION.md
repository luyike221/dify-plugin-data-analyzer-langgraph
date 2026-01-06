# LangGraph 1.0.0+ 迁移指南

本文档说明如何将项目从 DeepAnalyze 专用模型迁移到使用普通 LLM（如 GPT-4、Claude 等）的 LangGraph 工作流。

## 迁移概述

### 变化对比

| 特性 | 旧版（Legacy） | 新版（LangGraph） |
|------|---------------|-------------------|
| **依赖模型** | DeepAnalyze-8B | 任意 OpenAI 兼容 LLM |
| **工作流** | 硬编码循环 | LangGraph StateGraph |
| **消息格式** | `# Instruction` + `<Code>` 标签 | 标准 OpenAI 消息格式 |
| **停止条件** | 特殊 token ID | 自然结束 |
| **错误处理** | 有限 | 自动重试（最多3次） |
| **可扩展性** | 困难 | 易于添加新节点 |

### 新增模块

```
core/analyzer/
├── __init__.py      # 模块导出
├── state.py         # LangGraph 状态定义
├── prompts.py       # Prompt 模板管理
├── graph.py         # LangGraph 工作流图
└── api.py           # 兼容性 API 接口
```

## 使用指南

### 1. 配置分析器类型

在 Dify 插件配置中选择：

- **LangGraph（推荐）**：支持任意 OpenAI 兼容的 LLM
- **Legacy**：需要 DeepAnalyze 模型

或通过环境变量设置：

```bash
export ANALYZER_TYPE=langgraph  # 或 legacy
```

### 2. 配置 LLM API

```bash
# 分析 API（必填）
export ANALYSIS_API_URL=https://api.openai.com/v1/chat/completions
export ANALYSIS_MODEL=gpt-4o
export ANALYSIS_API_KEY=sk-xxx
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

新增的依赖：
- `langgraph>=1.0.0`
- `langchain-core>=0.3.0`
- `langchain-openai>=0.2.0`

## LangGraph 工作流详解

### 工作流图结构

```
START
  │
  ▼
┌─────────────────┐
│  generate_code  │  ← 调用 LLM 生成 Python 代码
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  execute_code   │  ← 在本地执行代码
└────────┬────────┘
         │
    ┌────┴────┐
    │ 成功？  │
    └────┬────┘
    是 ↙    ↘ 否
      │      │
      │      ▼
      │  ┌─────────────────┐
      │  │   fix_code      │  ← 调用 LLM 修复代码（最多3次）
      │  └────────┬────────┘
      │           │
      │      ┌────┴────┐
      │      │ 有修复？ │
      │      └────┬────┘
      │      是 ↙    ↘ 否
      │        │      │
      │        ▼      │
      │   (返回 execute_code)
      │               │
      ▼               ▼
┌─────────────────────────────┐
│      generate_report        │  ← 调用 LLM 生成分析报告
└──────────────┬──────────────┘
               │
               ▼
              END
```

### 状态定义

```python
class AnalysisState(TypedDict):
    # 输入数据
    workspace_dir: str       # 工作空间目录
    csv_path: str            # CSV 文件路径
    user_prompt: str         # 用户分析需求
    
    # LLM 配置
    api_url: str
    model: str
    
    # 工作流状态
    phase: str               # 当前阶段
    current_code: str        # 当前代码
    current_output: str      # 执行输出
    
    # 历史记录（自动追加）
    code_history: List[str]
    execution_history: List[CodeExecution]
    
    # 输出
    report: str              # 最终报告
```

### Prompt 模板

系统使用三种 Prompt：

1. **代码生成 Prompt** (`CODE_GENERATION_PROMPT`)
   - 输入：数据信息 + 用户需求
   - 输出：Python 代码块

2. **代码修复 Prompt** (`CODE_FIX_PROMPT`)
   - 输入：原代码 + 错误信息
   - 输出：修复后的代码

3. **报告生成 Prompt** (`REPORT_GENERATION_PROMPT`)
   - 输入：执行结果 + 用户需求
   - 输出：Markdown 报告

## API 兼容性

新的 LangGraph 分析器完全兼容现有 API：

```python
# 旧的调用方式（仍然有效）
from core.excel_analyze_api import analyze_excel_stream

for chunk in analyze_excel_stream(
    file_content=...,
    filename=...,
    analysis_api_url=...,
    analysis_model=...,
    # ... 其他参数
):
    print(chunk)

# 新增：可以通过 analyzer_type 参数明确指定分析器
for chunk in analyze_excel_stream(
    ...,
    analyzer_type="langgraph",  # 或 "legacy"
):
    print(chunk)
```

## 直接使用 LangGraph 分析器

```python
from core.analyzer import DataAnalysisGraph, run_langgraph_analysis_stream

# 方式1：使用封装类
graph = DataAnalysisGraph()
result = graph.analyze(
    workspace_dir="/path/to/workspace",
    thread_id="thread-xxx",
    csv_path="/path/to/data.csv",
    column_names=["col1", "col2"],
    column_metadata={...},
    row_count=1000,
    data_preview="...",
    user_prompt="分析销售数据趋势",
    api_url="https://api.openai.com/v1/chat/completions",
    model="gpt-4o",
    api_key="sk-xxx",
)

print(result.report)

# 方式2：使用流式 API
for chunk in run_langgraph_analysis_stream(
    workspace_dir="/path/to/workspace",
    thread_id="thread-xxx",
    csv_path="/path/to/data.csv",
    column_names=["col1", "col2"],
    column_metadata={...},
    row_count=1000,
    user_prompt="分析销售数据趋势",
    api_url="https://api.openai.com/v1/chat/completions",
    model="gpt-4o",
    api_key="sk-xxx",
):
    print(chunk, end="")
```

## 扩展工作流

### 添加新节点

```python
from core.analyzer.graph import create_analysis_graph
from langgraph.graph import END

# 获取工作流图
workflow = create_analysis_graph()

# 添加新节点
def my_custom_node(state):
    # 自定义逻辑
    return {"my_field": "value"}

workflow.add_node("my_node", my_custom_node)
workflow.add_edge("generate_report", "my_node")
workflow.add_edge("my_node", END)

# 编译并使用
graph = workflow.compile()
result = graph.invoke(initial_state)
```

### 自定义 Prompt

```python
from core.analyzer.prompts import PromptTemplates

# 修改系统 Prompt
PromptTemplates.CODE_GENERATION_SYSTEM = """
你是数据分析专家...（自定义内容）
"""

# 或创建新的模板方法
@classmethod
def format_my_prompt(cls, ...):
    ...
```

## 故障排除

### 1. LangGraph 导入错误

```bash
pip install langgraph>=1.0.0
```

### 2. 代码执行超时

调整 `config.py` 中的 `CODE_EXECUTION_TIMEOUT`：

```python
CODE_EXECUTION_TIMEOUT = 180  # 秒
```

### 3. 回退到 Legacy 模式

```bash
export ANALYZER_TYPE=legacy
```

或在 Dify 配置中选择 "Legacy"。

## 最佳实践

1. **选择合适的模型**
   - GPT-4 / GPT-4o：最佳代码生成能力
   - Claude-3：优秀的长文本理解
   - 本地模型：需要较大参数量（推荐 13B+）

2. **优化 Prompt**
   - 根据数据类型调整分析提示词
   - 明确指定输出格式要求

3. **监控执行**
   - 查看日志了解工作流状态
   - 使用流式输出实时观察进度

4. **处理大文件**
   - 限制数据预览行数
   - 使用采样分析

## 版本历史

- **v2.0.0** - 引入 LangGraph 1.0.0+ 支持
- **v1.x** - DeepAnalyze 专用版本

