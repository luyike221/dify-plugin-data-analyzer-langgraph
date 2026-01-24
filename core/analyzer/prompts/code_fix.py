"""
代码修复 Prompt

职责：修复执行失败的代码
"""

from typing import List, Dict


# 代码修复 System Prompt
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

# 代码修复 User Prompt
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


def format_code_fix_prompt(
    original_code: str,
    error_message: str,
    csv_path: str,
    column_names: List[str],
) -> List[Dict[str, str]]:
    """格式化代码修复 Prompt"""
    columns_str = ", ".join(column_names) if column_names else "未知"
    
    user_content = CODE_FIX_USER.format(
        original_code=original_code,
        error_message=error_message,
        csv_path=csv_path,
        column_names=columns_str,
    )
    
    return [
        {"role": "system", "content": CODE_FIX_SYSTEM},
        {"role": "user", "content": user_content},
    ]

