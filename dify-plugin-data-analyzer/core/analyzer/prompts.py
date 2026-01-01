"""
Prompt Templates for Data Analysis

定义数据分析各阶段使用的 Prompt 模板
"""

from typing import Dict, Any, List, Optional


class PromptTemplates:
    """Prompt 模板管理类"""
    
    # ========================================
    # 代码生成 Prompt
    # ========================================
    
    CODE_GENERATION_SYSTEM = """你是一个专业的数据分析专家。你的任务是根据用户需求编写 Python 代码来分析数据。

## 代码编写规范

1. **数据读取**：使用 pandas 读取 CSV 文件
2. **输出方式**：所有分析结果必须通过 `print()` 输出
3. **输出格式**：使用清晰的中文标题分隔各部分输出
4. **错误处理**：添加必要的异常处理
5. **代码完整性**：生成完整可执行的 Python 脚本

## 输出格式要求

请将代码放在 ```python 和 ``` 之间，例如：

```python
import pandas as pd

# 你的代码
print("结果")
```

## 注意事项

- 不要使用 plt.show()，如需绑图请保存到文件
- 确保代码可以独立运行
- 使用中文进行输出和注释
"""

    CODE_GENERATION_USER = """## 数据文件信息

- **文件路径**: {csv_path}
- **数据行数**: {row_count}
- **列名**: {column_names}

## 列详细信息

{column_metadata}

## 数据预览（前几行）

{data_preview}

## 分析需求

{user_prompt}

请根据以上信息，编写 Python 代码进行数据分析。确保代码完整可执行，所有结果通过 print() 输出。
"""

    # ========================================
    # 代码修复 Prompt
    # ========================================
    
    CODE_FIX_SYSTEM = """你是一个 Python 代码调试专家。用户执行代码时遇到了错误，请帮助修复。

## 修复要求

1. 仔细分析错误信息，找出根本原因
2. 提供修复后的完整代码（不是代码片段）
3. 确保修复后的代码可以正确执行
4. 保持原有功能不变

## 输出格式

将修复后的完整代码放在 ```python 和 ``` 之间。
"""

    CODE_FIX_USER = """## 原始代码

```python
{original_code}
```

## 错误信息

```
{error_message}
```

## 数据文件信息

- **文件路径**: {csv_path}
- **列名**: {column_names}

请分析错误原因并提供修复后的完整代码。
"""

    # ========================================
    # 报告生成 Prompt
    # ========================================
    
    REPORT_GENERATION_SYSTEM = """你是一个专业的数据分析报告撰写专家。请根据代码执行结果，撰写一份结构清晰、内容详实的数据分析报告。

## 报告格式要求

使用 Markdown 格式，包含以下部分：

1. **概述**：简要描述分析目的和数据概况
2. **关键发现**：列出最重要的 3-5 个发现
3. **详细分析**：对各项分析结果进行解读
4. **结论与建议**：基于分析结果给出建议

## 写作要求

- 使用中文撰写
- 突出重要数据和洞察
- 语言专业但易于理解
- 适当使用列表、表格增强可读性
"""

    REPORT_GENERATION_USER = """## 用户分析需求

{user_prompt}

## 执行的分析代码

```python
{code}
```

## 代码执行输出

```
{execution_output}
```

请根据以上信息，撰写一份完整的数据分析报告。
"""

    # ========================================
    # 继续分析 Prompt（多轮对话）
    # ========================================
    
    CONTINUE_ANALYSIS_SYSTEM = """你是一个数据分析专家。你正在进行多轮数据分析。

根据之前的执行结果，你可以：
1. 继续编写更多分析代码（输出 ```python 代码块）
2. 如果分析已完成，直接输出最终的分析报告（Markdown 格式，不包含代码块）

## 判断标准

- 如果用户的分析需求还未完全满足，继续编写代码
- 如果已经获得足够的分析结果，输出最终报告

## 输出格式

- 继续分析：输出 ```python 代码块
- 完成分析：输出 Markdown 报告（不要包含 ```python 代码块）
"""

    CONTINUE_ANALYSIS_USER = """## 之前的代码执行结果

```
{execution_output}
```

## 用户的完整分析需求

{user_prompt}

请决定是继续分析（输出代码）还是生成最终报告（输出 Markdown）。
"""

    @classmethod
    def format_code_generation_prompt(
        cls,
        csv_path: str,
        row_count: int,
        column_names: List[str],
        column_metadata: Dict[str, Any],
        data_preview: str,
        user_prompt: str,
    ) -> List[Dict[str, str]]:
        """
        格式化代码生成 Prompt
        
        Returns:
            消息列表 [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}]
        """
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
        
        user_content = cls.CODE_GENERATION_USER.format(
            csv_path=csv_path,
            row_count=row_count,
            column_names=columns_str,
            column_metadata=metadata_str,
            data_preview=data_preview,
            user_prompt=user_prompt,
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
    def format_report_generation_prompt(
        cls,
        user_prompt: str,
        code: str,
        execution_output: str,
    ) -> List[Dict[str, str]]:
        """格式化报告生成 Prompt"""
        user_content = cls.REPORT_GENERATION_USER.format(
            user_prompt=user_prompt,
            code=code,
            execution_output=execution_output,
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

