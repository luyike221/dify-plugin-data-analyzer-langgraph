"""
代码生成 Prompt

职责：根据分析任务生成 Python 代码
"""

from typing import Dict, Any, List, Optional
from .data_info import format_data_info


# 代码生成 System Prompt
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

# 首轮代码生成 User Prompt
CODE_GENERATION_USER_FIRST = """{data_info}

## 分析任务

{analysis_task}

请编写 Python 代码完成此任务。注意：所有结果必须通过 print() 输出，禁止硬编码结论。
"""

# 后续轮代码生成 User Prompt（有之前的分析结果）
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


def format_code_generation_prompt(
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
    data_info = format_data_info(
        csv_path, row_count, column_names, column_metadata, data_preview
    )
    
    if is_first_round or not previous_results:
        user_content = CODE_GENERATION_USER_FIRST.format(
            data_info=data_info,
            analysis_task=user_prompt,
        )
    else:
        user_content = CODE_GENERATION_USER_CONTINUE.format(
            data_info=data_info,
            previous_results=previous_results,
            analysis_task=user_prompt,
        )
    
    return [
        {"role": "system", "content": CODE_GENERATION_SYSTEM},
        {"role": "user", "content": user_content},
    ]

