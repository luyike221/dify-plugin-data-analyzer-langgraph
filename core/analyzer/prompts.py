"""
Prompt Templates for Data Analysis

定义数据分析各阶段使用的 Prompt 模板

重构设计原则：
1. 任务驱动：每轮分析都有明确的"分析任务"
2. 上下文连贯：各节点共享分析上下文
3. 职责单一：每个节点只做一件事
"""

from typing import Dict, Any, List, Optional


class PromptTemplates:
    """Prompt 模板管理类"""
    
    # ========================================
    # 共享的数据信息模板（供各节点复用）
    # ========================================
    
    DATA_INFO_TEMPLATE = """## 数据信息

- **文件路径**: {csv_path}
- **数据行数**: {row_count}
- **列名**: {column_names}

### 列详细信息
{column_metadata}

### 数据预览
{data_preview}
"""

    # ========================================
    # 1. 意图分析 Prompt
    # 职责：理解用户需求，制定分析计划
    # ========================================
    
    INTENT_ANALYSIS_SYSTEM = """你是数据分析专家。你的任务是理解用户需求，制定分析计划。

## 任务

1. 判断用户输入是否与数据相关
2. 理解用户真实意图
3. 制定分析计划（分解为具体的分析任务）

## 问题分类

### 简单查询（直接回答）
- "有多少行/列？"、"显示前N行"、"某字段最大值"等
- **不需要多轮分析**，标记 `analysis_type: "simple"`

### 概述性问题（全面分析）  
- "分析这个数据"、"数据有什么特点"等
- **可能需要多轮分析**，标记 `analysis_type: "overview"`

### 具体分析（深入分析）
- "分析客户流失"、"对比A和B的差异"等
- **可能需要多轮分析**，标记 `analysis_type: "specific"`

## 输出格式（JSON）

```json
{
    "is_relevant": true/false,
    "needs_clarification": true/false,
    "clarification_message": "如需澄清的消息",
    "analysis_type": "simple/overview/specific",
    "refined_prompt": "明确化的分析需求",
    "analysis_tasks": ["任务1", "任务2", ...],
    "first_task": "第一轮要完成的具体任务"
}
```

## 关键规则

1. **简单查询**：`analysis_tasks` 只包含一个任务，`first_task` 就是用户问题本身
2. **概述/具体分析**：将需求分解为多个可执行的任务
3. **澄清规则**：
   - 正常情况下，`needs_clarification` 应设为 `false`，直接进行数据分析
   - 只有当用户问题**严重偏离数据分析或标的内容**（如询问天气、聊天、与数据完全无关的问题）时，才设置 `needs_clarification: true` 并提供 `clarification_message`
   - 对于模糊或不明确的数据分析需求，应尽量理解并直接进行分析，而不是要求澄清
"""

    INTENT_ANALYSIS_USER = """{data_info}

## 用户输入

{user_prompt}

请分析用户意图并制定分析计划。
"""

    # ========================================
    # 2. 代码生成 Prompt
    # 职责：根据分析任务生成 Python 代码
    # ========================================
    
    CODE_GENERATION_SYSTEM = """你是 Python 数据分析专家。你的任务是根据分析任务编写代码。

## 代码规范

1. 使用 pandas 读取 CSV
2. 所有结果通过 `print()` 输出
3. 输出使用清晰的中文标题
4. 代码完整可执行

## 关键规则

1. **禁止硬编码结果**：所有 print 必须输出计算结果，不能输出预设结论
2. **注意单位**：
   - 列名含 `(%)` 的，计算时需除以100
   - 列名含 `(万元)` 的，转换为元需乘以10000
3. **禁止生成图片**：不使用 plt.show()

## 输出格式

只输出代码块：
```python
import pandas as pd

# 你的代码
```
"""

    # 首轮代码生成
    CODE_GENERATION_USER_FIRST = """{data_info}

## 分析任务

{analysis_task}

请编写 Python 代码完成此任务。注意：所有结果必须通过 print() 输出，禁止硬编码结论。
"""

    # 后续轮代码生成（有之前的分析结果）
    CODE_GENERATION_USER_CONTINUE = """{data_info}

## 之前的分析结果

{previous_results}

## 当前分析任务

{analysis_task}

请基于之前的分析结果，编写代码完成当前任务。
- 不要重复之前已分析的内容
- 可以引用之前的发现进行深入分析
- 所有结果通过 print() 输出
"""

    # ========================================
    # 3. 代码修复 Prompt
    # 职责：修复执行失败的代码
    # ========================================
    
    CODE_FIX_SYSTEM = """你是 Python 调试专家。请修复代码错误。

## 要求

1. 分析错误原因
2. 提供完整的修复后代码（不是片段）
3. 保持原有功能

## 输出格式

只输出修复后的完整代码：
```python
# 修复后的完整代码
```
"""

    CODE_FIX_USER = """## 原始代码

```python
{original_code}
```

## 错误信息

```
{error_message}
```

## 数据信息

- 文件路径: {csv_path}
- 列名: {column_names}

请修复代码。
"""

    # ========================================
    # 4. 分析评估 Prompt
    # 职责：评估分析是否完成，决定下一步
    # ========================================
    
    EVALUATE_COMPLETENESS_SYSTEM = """你是数据分析质量评估专家。判断当前分析是否满足用户需求。

## 评估标准

1. **简单查询**：一轮即可完成，直接返回 `need_more_analysis: false`
2. **概述/具体分析**：检查各分析任务是否都已完成

## 判断规则

### 返回 need_more_analysis: false 的情况：
- 简单查询已回答
- 所有分析任务已完成
- 已达到最大轮数（第 {current_round}/{max_rounds} 轮）
- 继续分析不会带来显著新价值

### 返回 need_more_analysis: true 的情况：
- 有重要的分析任务未完成
- 发现值得深入探索的新方向
- 用户需求未被充分满足

## 输出格式（JSON）

```json
{{
    "need_more_analysis": true/false,
    "reason": "简要理由",
    "completed_tasks": ["已完成的任务"],
    "next_task": "如果继续，下一轮的具体任务（必须明确可执行）"
}}
```

## 重要

- 当前是第 {current_round} 轮，最多 {max_rounds} 轮
- 如果已达上限，必须返回 false
- `next_task` 必须具体可执行，不能是"继续分析"这样的模糊描述
"""

    EVALUATE_COMPLETENESS_USER = """## 用户原始需求

{user_prompt}

## 分析计划

原定任务：
{analysis_tasks}

## 当前轮分析结果

```
{current_output}
```

## 之前轮次的分析结果

{previous_outputs}

## 已完成的任务

{completed_tasks}

请评估分析是否完成。
"""

    # ========================================
    # 5. 报告生成 Prompt
    # 职责：综合所有分析结果，生成最终报告
    # ========================================
    
    REPORT_GENERATION_SYSTEM = """你是数据分析助手。根据用户的问题和分析结果，直接、准确地回答问题。

## 核心原则

1. **直接回答用户问题**：用户问什么就答什么，不要偏离主题
2. **简洁明了**：用最少的文字回答，避免冗余和重复
3. **问题与答案对应**：确保答案完全对应问题，不要答非所问
4. **基于数据说话**：所有结论必须基于提供的分析结果，不要编造或推测

## 回答方式

### 简单问题（如"有多少行数据？"）
- 直接给出数字或答案
- 例如："共有 1000 行数据。"

### 分析类问题（如"分析销售趋势"）
- 先直接回答核心问题
- 然后提供支撑数据和分析结果
- 避免使用固定的报告模板，根据问题灵活组织

### 多轮分析结果
- 只提取与用户问题相关的部分
- 不要罗列所有分析结果
- 综合相关发现，形成针对性的答案

## 禁止事项

- ❌ 不要使用固定的报告模板（如"概述、关键发现、详细分析、结论建议"）
- ❌ 不要添加用户没有问到的内容
- ❌ 不要使用过于复杂的结构
- ❌ 不要答非所问或偏离主题
- ❌ 不要重复已经说过的内容

## 格式要求

- 使用中文
- 使用 Markdown 格式（标题、列表、表格等）
- 重要数据用**粗体**突出
- 保持简洁，避免冗长
"""

    REPORT_GENERATION_USER = """## 用户问题

{user_prompt}

**重要：请直接回答上述问题，不要偏离主题。**

## 分析结果

{all_results}

## 数据信息

{column_metadata_info}

---

**请根据用户问题，从上述分析结果中提取相关信息，直接、简洁地回答。**
- 如果用户问的是简单问题，直接给出答案
- 如果用户问的是分析类问题，提供相关分析和数据支撑
- 确保答案与问题完全对应，不要添加无关内容
"""

    # ========================================
    # 6. 继续分析 Prompt（保留，用于特殊场景）
    # ========================================
    
    CONTINUE_ANALYSIS_SYSTEM = """你是数据分析专家。根据之前的结果决定下一步。

## 选择

1. 继续分析：输出 Python 代码块
2. 分析完成：输出 Markdown 报告

## 输出格式

- 继续：```python 代码 ```
- 完成：Markdown 报告（无代码块）
"""

    CONTINUE_ANALYSIS_USER = """## 之前的执行结果

```
{execution_output}
```

## 用户需求

{user_prompt}

请决定下一步。
"""

    # ========================================
    # 格式化函数
    # ========================================
    
    @classmethod
    def _format_data_info(
        cls,
        csv_path: str,
        row_count: int,
        column_names: List[str],
        column_metadata: Dict[str, Any],
        data_preview: str,
    ) -> str:
        """格式化数据信息（供各节点复用）"""
        # 格式化列元数据
        if isinstance(column_metadata, dict):
            metadata_str = "\n".join([
                f"- **{col}**: {info}" 
                for col, info in column_metadata.items()
            ])
        else:
            metadata_str = str(column_metadata)
        
        # 格式化列名
        columns_str = ", ".join(column_names) if column_names else "未知"
        
        return cls.DATA_INFO_TEMPLATE.format(
            csv_path=csv_path,
            row_count=row_count,
            column_names=columns_str,
            column_metadata=metadata_str,
            data_preview=data_preview,
        )
    
    @classmethod
    def format_intent_analysis_prompt(
        cls,
        csv_path: str,
        row_count: int,
        column_names: List[str],
        column_metadata: Dict[str, Any],
        data_preview: str,
        user_prompt: str,
    ) -> List[Dict[str, str]]:
        """格式化意图分析 Prompt"""
        data_info = cls._format_data_info(
            csv_path, row_count, column_names, column_metadata, data_preview
        )
        
        user_content = cls.INTENT_ANALYSIS_USER.format(
            data_info=data_info,
            user_prompt=user_prompt,
        )
        
        return [
            {"role": "system", "content": cls.INTENT_ANALYSIS_SYSTEM},
            {"role": "user", "content": user_content},
        ]
    
    @classmethod
    def format_code_generation_prompt(
        cls,
        csv_path: str,
        row_count: int,
        column_names: List[str],
        column_metadata: Dict[str, Any],
        data_preview: str,
        user_prompt: str,
        previous_results: Optional[str] = None,
        is_first_round: bool = True,
    ) -> List[Dict[str, str]]:
        """
        格式化代码生成 Prompt
        
        Args:
            previous_results: 之前轮次的分析结果（用于后续轮）
            is_first_round: 是否是第一轮
        """
        data_info = cls._format_data_info(
            csv_path, row_count, column_names, column_metadata, data_preview
        )
        
        if is_first_round or not previous_results:
            user_content = cls.CODE_GENERATION_USER_FIRST.format(
                data_info=data_info,
                analysis_task=user_prompt,
            )
        else:
            user_content = cls.CODE_GENERATION_USER_CONTINUE.format(
                data_info=data_info,
                previous_results=previous_results,
                analysis_task=user_prompt,
            )
        
        return [
            {"role": "system", "content": cls.CODE_GENERATION_SYSTEM},
            {"role": "user", "content": user_content},
        ]
    
    @classmethod
    def format_code_fix_prompt(
        cls,
        original_code: str,
        error_message: str,
        csv_path: str,
        column_names: List[str],
    ) -> List[Dict[str, str]]:
        """格式化代码修复 Prompt"""
        columns_str = ", ".join(column_names) if column_names else "未知"
        
        user_content = cls.CODE_FIX_USER.format(
            original_code=original_code,
            error_message=error_message,
            csv_path=csv_path,
            column_names=columns_str,
        )
        
        return [
            {"role": "system", "content": cls.CODE_FIX_SYSTEM},
            {"role": "user", "content": user_content},
        ]
    
    @classmethod
    def format_evaluate_completeness_prompt(
        cls,
        user_prompt: str,
        analysis_tasks: List[str],
        current_output: str,
        previous_outputs: List[str],
        completed_tasks: List[str],
        current_round: int,
        max_rounds: int,
    ) -> List[Dict[str, str]]:
        """格式化分析评估 Prompt"""
        # 格式化分析任务
        if analysis_tasks:
            tasks_str = "\n".join([f"- {t}" for t in analysis_tasks])
        else:
            tasks_str = "（未明确分析任务）"
        
        # 格式化之前的输出
        if previous_outputs:
            previous_str = "\n\n---\n\n".join([
                f"【第 {i+1} 轮】\n```\n{output}\n```"
                for i, output in enumerate(previous_outputs)
            ])
        else:
            previous_str = "（这是第一轮分析）"
        
        # 格式化已完成的任务
        if completed_tasks:
            completed_str = "\n".join([f"- {t}" for t in completed_tasks])
        else:
            completed_str = "（尚未完成任何任务）"
        
        # 格式化 system prompt
        system_content = cls.EVALUATE_COMPLETENESS_SYSTEM.format(
            current_round=current_round,
            max_rounds=max_rounds,
        )
        
        # 格式化 user prompt
        user_content = cls.EVALUATE_COMPLETENESS_USER.format(
            user_prompt=user_prompt,
            analysis_tasks=tasks_str,
            current_output=current_output,
            previous_outputs=previous_str,
            completed_tasks=completed_str,
        )
        
        return [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content},
        ]
    
    @classmethod
    def format_report_generation_prompt(
        cls,
        user_prompt: str,
        analysis_type: str,
        total_rounds: int,
        all_results: str,
        column_names: List[str] = None,
        column_metadata: Dict[str, Any] = None,
    ) -> List[Dict[str, str]]:
        """格式化报告生成 Prompt"""
        # 格式化列元数据信息
        if column_metadata and isinstance(column_metadata, dict):
            metadata_lines = []
            for col_name, metadata in column_metadata.items():
                if isinstance(metadata, dict):
                    levels = []
                    for i in range(1, 6):
                        level_key = f"level{i}"
                        if level_key in metadata and metadata[level_key]:
                            levels.append(metadata[level_key])
                    
                    if levels:
                        level_str = " > ".join(levels)
                        metadata_lines.append(f"- **{col_name}**: {level_str}")
                    else:
                        metadata_lines.append(f"- **{col_name}**: {metadata}")
                else:
                    metadata_lines.append(f"- **{col_name}**: {metadata}")
            
            column_metadata_info = "\n".join(metadata_lines) if metadata_lines else "无"
        elif column_names:
            column_metadata_info = "列名：" + ", ".join(column_names)
        else:
            column_metadata_info = "无"
        
        user_content = cls.REPORT_GENERATION_USER.format(
            user_prompt=user_prompt,
            all_results=all_results,
            column_metadata_info=column_metadata_info,
        )
        
        return [
            {"role": "system", "content": cls.REPORT_GENERATION_SYSTEM},
            {"role": "user", "content": user_content},
        ]
    
    @classmethod
    def format_continue_analysis_prompt(
        cls,
        execution_output: str,
        user_prompt: str,
    ) -> List[Dict[str, str]]:
        """格式化继续分析 Prompt"""
        user_content = cls.CONTINUE_ANALYSIS_USER.format(
            execution_output=execution_output,
            user_prompt=user_prompt,
        )
        
        return [
            {"role": "system", "content": cls.CONTINUE_ANALYSIS_SYSTEM},
            {"role": "user", "content": user_content},
        ]
