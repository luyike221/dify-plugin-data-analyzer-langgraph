# 意图分析和策略制定功能

## 功能概述

在代码生成节点之前新增了**意图分析和策略制定节点**，用于：
1. 判断用户输入与数据的相关性
2. 如果无关，直接返回澄清消息
3. 如果相关，重写用户需求并制定分析策略
4. 针对银行内部使用场景优化

## 工作流变化

### 原工作流
```
START → generate_code → execute_code → generate_report → END
```

### 新工作流
```
START → analyze_intent ─┬─(需要澄清)─→ END
                        │
                        └─(可以分析)─→ generate_code → execute_code → generate_report → END
```

## 新增节点：analyze_intent_node

### 功能说明

**位置**：`core/analyzer/graph.py::analyze_intent_node()`

**主要功能**：

1. **意图识别**
   - 分析用户输入是否与提供的数据相关
   - 判断是否需要用户澄清

2. **需求理解**
   - 如果相关，猜测用户的真实分析目的
   - 重写用户输入，使其更明确、具体

3. **策略制定**
   - 制定具体的分析策略
   - 列出研究方向（针对银行场景）

### 处理逻辑

```python
# 1. 调用 LLM 进行意图分析
response = call_llm(...)

# 2. 解析 JSON 响应
intent_result = {
    "is_relevant": true/false,
    "needs_clarification": true/false,
    "clarification_message": "...",
    "refined_prompt": "重写后的需求",
    "analysis_strategy": "分析策略",
    "research_directions": ["方向1", "方向2"]
}

# 3. 根据结果路由
if not is_relevant:
    → 返回澄清消息，结束工作流
elif needs_clarification:
    → 返回澄清消息，结束工作流
else:
    → 继续到代码生成节点
```

## Prompt 设计

### System Prompt

针对银行场景优化的系统提示词，包含：
- 银行数据分析常见场景（客户分析、交易分析、风险分析等）
- 判断标准
- 输出格式要求（JSON）

### User Prompt

包含：
- 数据文件信息（路径、行数、列名）
- 列详细信息
- 数据预览
- 用户原始输入

## 状态字段扩展

在 `AnalysisState` 中新增字段：

```python
# 意图分析结果
refined_prompt: str              # 重写后的用户输入
analysis_strategy: str           # 分析策略
research_directions: List[str]    # 研究方向列表
intent_analysis_result: str      # 意图分析结果（JSON格式）
needs_clarification: bool         # 是否需要用户澄清
clarification_message: Optional[str]  # 澄清消息
```

## 使用示例

### 示例 1：用户输入与数据无关

**用户输入**："今天天气怎么样？"

**意图分析结果**：
```json
{
    "is_relevant": false,
    "needs_clarification": true,
    "clarification_message": "您的问题与当前数据文件不相关。请提供与数据相关的分析需求，或上传正确的数据文件。"
}
```

**工作流**：直接结束，返回澄清消息

### 示例 2：用户输入模糊

**用户输入**："分析一下"

**意图分析结果**：
```json
{
    "is_relevant": true,
    "needs_clarification": true,
    "clarification_message": "您的分析需求不够明确。请具体说明：1. 想分析哪些指标？2. 关注哪些维度？3. 希望得到什么结论？"
}
```

**工作流**：直接结束，返回澄清消息

### 示例 3：正常分析

**用户输入**："看看客户流失情况"

**意图分析结果**：
```json
{
    "is_relevant": true,
    "needs_clarification": false,
    "refined_prompt": "分析客户流失情况，包括：1. 流失客户数量及占比 2. 流失客户特征分析 3. 流失原因分析 4. 流失趋势预测",
    "analysis_strategy": "从客户数据中识别流失客户，分析流失客户的共同特征，计算流失率，预测未来流失趋势",
    "research_directions": [
        "流失客户识别和统计",
        "流失客户特征分析",
        "流失率计算",
        "流失趋势分析"
    ]
}
```

**工作流**：继续到代码生成节点，使用 `refined_prompt` 生成代码

## 银行场景优化

### 常见分析场景

系统 Prompt 中包含了银行常见的数据分析场景：

- **客户分析**：客户画像、客户分群、客户流失分析
- **交易分析**：交易趋势、异常交易检测、交易模式分析
- **风险分析**：信用风险、操作风险、市场风险
- **业务分析**：产品分析、渠道分析、业绩分析
- **财务分析**：收入分析、成本分析、利润分析
- **合规分析**：反洗钱、合规检查、审计分析

### 研究方向制定

LLM 会根据银行场景和用户需求，制定具体的研究方向，例如：

- 客户流失分析 → ["流失客户识别", "流失特征分析", "流失率计算", "流失预测"]
- 交易异常检测 → ["交易模式分析", "异常交易识别", "风险评分", "预警机制"]
- 信用风险评估 → ["信用评分", "违约概率", "风险等级", "授信建议"]

## 代码生成节点的改进

代码生成节点现在使用 `refined_prompt`（重写后的需求）而不是原始 `user_prompt`：

```python
# 使用重写后的用户需求（如果存在），否则使用原始需求
user_prompt = state.get("refined_prompt") or state["user_prompt"]
```

这样可以：
- 使用更明确、具体的分析需求
- 提高代码生成的质量
- 减少因需求不明确导致的代码错误

## 配置说明

意图分析节点使用与代码生成相同的 LLM 配置：
- `api_url`: LLM API 地址
- `model`: 模型名称（如 qwen-plus）
- `api_key`: API 密钥
- `temperature`: 生成温度（默认 0.4）

## 错误处理

如果 LLM 返回的 JSON 无法解析：
- 默认继续分析流程
- 使用原始用户输入
- 记录警告日志

## 日志输出

意图分析节点会输出：
- 重写后的分析需求
- 分析策略
- 研究方向列表

用户可以在流式输出中看到这些信息，了解系统如何理解他们的需求。


