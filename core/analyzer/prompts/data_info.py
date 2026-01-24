"""
数据信息模板

共享的数据信息模板，供各节点复用
"""

from typing import Dict, Any, List


# 数据信息模板
DATA_INFO_TEMPLATE = """## 数据信息

- **文件路径**: {csv_path}
- **数据行数**: {row_count}
- **列名**: {column_names}

### 列详细信息
{column_metadata}

### 数据预览
{data_preview}
"""


def format_data_info(
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
    
    return DATA_INFO_TEMPLATE.format(
        csv_path=csv_path,
        row_count=row_count,
        column_names=columns_str,
        column_metadata=metadata_str,
        data_preview=data_preview,
    )

