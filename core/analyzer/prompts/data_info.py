"""
数据信息模板

共享的数据信息模板，供各节点复用
 支持单文件和多文件场景
"""

from typing import Dict, Any, List, Optional


# 单文件数据信息模板
DATA_INFO_TEMPLATE = """## 数据信息

- **文件路径**: {csv_path}
- **数据行数**: {row_count}
- **列名**: {column_names}

### 列详细信息
{column_metadata}

### 数据预览
{data_preview}
"""

# 多文件数据信息模板
MULTI_FILE_DATA_INFO_TEMPLATE = """## 可用数据文件

当前工作空间中有以下数据文件可供分析：

{files_info}

**重要提示**：
- 请根据用户需求，选择合适的文件进行分析
- 可以分析单个文件，也可以对比分析多个文件
- 在代码中使用文件路径时，请使用上述路径
"""


def format_data_info(
    csv_path: str,
    row_count: int,
    column_names: List[str],
    column_metadata: Dict[str, Any],
    data_preview: str,
) -> str:
    """格式化单文件数据信息（供各节点复用）"""
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
    
    return DATA_INFO_TEMPLATE.format(
        csv_path=csv_path,
        row_count=row_count,
        column_names=columns_str,
        column_metadata=metadata_str,
        data_preview=data_preview,
    )


def format_multi_file_data_info(
    files_info: List[Dict[str, Any]],
) -> str:
    """
    格式化多文件数据信息
    
    Args:
        files_info: 文件信息列表，每个元素包含：
            - filename: 文件名
            - csv_path: CSV文件路径
            - row_count: 数据行数
            - column_names: 列名列表
            - column_metadata: 列元数据
            - data_preview: 数据预览（可选）
    """
    files_info_strs = []
    
    for i, file_info in enumerate(files_info, 1):
        filename = file_info.get("filename", f"文件{i}")
        csv_path = file_info.get("csv_path", "")
        row_count = file_info.get("row_count", 0)
        column_names = file_info.get("column_names", [])
        column_metadata = file_info.get("column_metadata", {})
        data_preview = file_info.get("data_preview", "")
        
        # 格式化列名
        columns_str = ", ".join(column_names) if column_names else "未知"
        
        # 格式化列元数据
        if isinstance(column_metadata, dict):
            metadata_str = "\n".join([
                f"    - **{col}**: {info}" 
                for col, info in column_metadata.items()
            ])
        else:
            metadata_str = str(column_metadata)
        
        # 构建单个文件信息
        file_info_str = f"""### 文件 {i}: {filename}

- **文件路径**: `{csv_path}`
- **数据行数**: {row_count}
- **列名**: {columns_str}

#### 列详细信息
{metadata_str}
"""
        
        # 如果有数据预览，添加预览
        if data_preview:
            file_info_str += f"""
#### 数据预览
```
{data_preview}
```
"""
        
        files_info_strs.append(file_info_str)
    
    return MULTI_FILE_DATA_INFO_TEMPLATE.format(
        files_info="\n".join(files_info_strs)
    )

