"""
报告生成 Prompt

职责：综合所有分析结果，生成最终报告
"""

from typing import Dict, Any, List, Optional


# 报告生成 System Prompt
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

# 报告生成 User Prompt
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


def format_report_generation_prompt(
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
    
    user_content = REPORT_GENERATION_USER.format(
        user_prompt=user_prompt,
        all_results=all_results,
        column_metadata_info=column_metadata_info,
    )
    
    return [
        {"role": "system", "content": REPORT_GENERATION_SYSTEM},
        {"role": "user", "content": user_content},
    ]

