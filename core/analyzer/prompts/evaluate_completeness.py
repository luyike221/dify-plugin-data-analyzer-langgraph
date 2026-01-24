"""
分析评估 Prompt

职责：评估分析是否完成，决定下一步
"""

from typing import List, Dict


# 分析评估 System Prompt
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

# 分析评估 User Prompt
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


def format_evaluate_completeness_prompt(
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
    system_content = EVALUATE_COMPLETENESS_SYSTEM.format(
        current_round=current_round,
        max_rounds=max_rounds,
    )
    
    # 格式化 user prompt
    user_content = EVALUATE_COMPLETENESS_USER.format(
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

