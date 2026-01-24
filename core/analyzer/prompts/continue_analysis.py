"""
继续分析 Prompt

保留，用于特殊场景
"""

from typing import List, Dict


# 继续分析 System Prompt
CONTINUE_ANALYSIS_SYSTEM = """你是数据分析专家。根据之前的结果决定下一步。

## 选择

1. 继续分析：输出 Python 代码块
2. 分析完成：输出 Markdown 报告

## 输出格式

- 继续：```python 代码 ```
- 完成：Markdown 报告（无代码块）
"""

# 继续分析 User Prompt
CONTINUE_ANALYSIS_USER = """## 之前的执行结果

```
{execution_output}
```

## 用户需求

{user_prompt}

请决定下一步。
"""


def format_continue_analysis_prompt(
    execution_output: str,
    user_prompt: str,
) -> List[Dict[str, str]]:
    """格式化继续分析 Prompt"""
    user_content = CONTINUE_ANALYSIS_USER.format(
        execution_output=execution_output,
        user_prompt=user_prompt,
    )
    
    return [
        {"role": "system", "content": CONTINUE_ANALYSIS_SYSTEM},
        {"role": "user", "content": user_content},
    ]

