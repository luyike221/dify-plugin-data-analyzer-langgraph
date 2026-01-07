"""
Excel智能处理模块
支持：
1. 自动跳过无效行（注释、标题等）
2. 单表头/多表头自动识别
3. 可选调用LLM进行智能分析
4. 合并单元格处理
5. 列结构元数据生成
"""

import pandas as pd
import json
import re
import os
import requests
import logging
import tempfile
import shutil
import time
from openpyxl import load_workbook
from typing import Tuple, List, Dict, Optional, Any
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from pathlib import Path

# 配置日志
logger = logging.getLogger(__name__)

# 导入配置（避免循环导入，使用延迟导入）

from .config import EXCEL_LLM_API_KEY, EXCEL_LLM_BASE_URL, EXCEL_LLM_MODEL



@dataclass
class HeaderAnalysis:
    """表头分析结果"""
    skip_rows: int          # 需要跳过的无效行数
    header_rows: int        # 表头占用的行数
    header_type: str        # 'single' 或 'multi'
    data_start_row: int     # 数据开始行（1-indexed）
    confidence: str         # 置信度: high/medium/low
    reason: str             # 分析原因说明
    start_col: int = 1      # 数据起始列（1-indexed），第一个表头行中第一个非空表头开始的列
    valid_cols: Optional[List[int]] = None  # 有效列的索引列表（1-indexed），None表示所有列都有效
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = asdict(self)
        if result.get('valid_cols') is None:
            result['valid_cols'] = None
        return result


@dataclass
class ExcelProcessResult:
    """Excel处理结果"""
    success: bool
    header_analysis: Optional[HeaderAnalysis]
    processed_file_path: Optional[str]      # 处理后的CSV文件路径
    metadata_file_path: Optional[str]       # 元数据JSON文件路径
    column_names: List[str]                 # 列名列表
    column_metadata: Dict[str, Dict]        # 列结构元数据
    row_count: int                          # 数据行数
    error_message: Optional[str]            # 错误信息
    llm_analysis_response: Optional[str] = None  # LLM分析原始响应（用于调试）
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "success": self.success,
            "header_analysis": self.header_analysis.to_dict() if self.header_analysis else None,
            "processed_file_path": self.processed_file_path,
            "metadata_file_path": self.metadata_file_path,
            "column_names": self.column_names,
            "column_metadata": self.column_metadata,
            "row_count": self.row_count,
            "error_message": self.error_message
        }


class SmartHeaderProcessor:
    """智能表头处理器"""
    
    def __init__(self, filepath: str, sheet_name: str = None):
        self.filepath = filepath
        self.sheet_name = sheet_name
        self.file_ext = Path(filepath).suffix.lower()
        self._temp_xlsx_path = None  # 用于存储临时转换的 .xlsx 文件路径
        
        # 如果是 .xls 格式，先转换为 .xlsx
        if self.file_ext == '.xls':
            logger.info(f"🔄 检测到 .xls 格式文件，正在转换为 .xlsx...")
            self._temp_xlsx_path = self._convert_xls_to_xlsx(filepath)
            actual_filepath = self._temp_xlsx_path
            logger.info(f"✅ 转换完成: {self._temp_xlsx_path}")
        else:
            actual_filepath = filepath
        
        # 统一使用 openpyxl 读取
        # 注意：不使用 read_only 模式，因为需要访问 merged_cells 属性来处理合并单元格
        self.wb = load_workbook(actual_filepath, data_only=True)
        # 修复：明确使用第一个工作表，而不是依赖 wb.active（active可能是用户最后查看的工作表）
        if sheet_name:
            self.ws = self.wb[sheet_name]
        else:
            # 明确使用第一个工作表（索引0），确保行为一致
            if not self.wb.sheetnames:
                raise ValueError("Excel文件不包含任何工作表")
            self.ws = self.wb[self.wb.sheetnames[0]]
        self.merged_cells_map = self._build_merged_cells_map()
    
    def _convert_xls_to_xlsx(self, xls_path: str) -> str:
        """
        将 .xls 文件转换为 .xlsx 格式
        
        参数:
            xls_path: .xls 文件路径
        
        返回:
            临时 .xlsx 文件路径
        """
        try:
            # 读取所有工作表
            excel_file = pd.ExcelFile(xls_path, engine='xlrd')
            
            # 创建临时文件
            temp_dir = os.path.dirname(xls_path)
            temp_xlsx_path = os.path.join(
                temp_dir, 
                f"{Path(xls_path).stem}_converted_{os.getpid()}.xlsx"
            )
            
            # 使用 ExcelWriter 写入所有工作表
            with pd.ExcelWriter(temp_xlsx_path, engine='openpyxl') as writer:
                for sheet_name in excel_file.sheet_names:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name, engine='xlrd')
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            logger.info(f"✅ .xls 文件已转换为 .xlsx: {temp_xlsx_path}")
            return temp_xlsx_path
            
        except Exception as e:
            logger.error(f"❌ 转换 .xls 文件失败: {e}")
            raise ValueError(
                f"无法转换 .xls 文件。请确保已安装 xlrd 库: pip install xlrd。错误: {str(e)}"
            )
    
    def _build_merged_cells_map(self) -> Dict[Tuple[int, int], str]:
        """构建合并单元格映射"""
        merged_map = {}
        try:
            for merged_range in self.ws.merged_cells.ranges:
                min_row, min_col = merged_range.min_row, merged_range.min_col
                value = self.ws.cell(min_row, min_col).value
                for row in range(merged_range.min_row, merged_range.max_row + 1):
                    for col in range(merged_range.min_col, merged_range.max_col + 1):
                        merged_map[(row, col)] = value
        except Exception as e:
            # 如果无法获取合并单元格信息，记录警告并返回空字典
            logger.warning(f"⚠️ 构建合并单元格映射时出错: {e}，将使用空映射")
        
        return merged_map
    
    def get_cell_value(self, row: int, col: int) -> Any:
        """获取单元格值，处理合并单元格"""
        if (row, col) in self.merged_cells_map:
            return self.merged_cells_map[(row, col)]
        return self.ws.cell(row, col).value
    
    def get_preview_data(self, max_rows: int = 30, max_cols: int = 20) -> List[List[Any]]:
        """
        获取预览数据用于分析（增强版）
        
        改进：
        1. 增加预览行数和列数，更好地展示复杂表头
        2. 标记合并单元格，便于识别层级结构
        3. 区分文本和数值，便于识别数据起始行
        """
        actual_max_col = min(self.ws.max_column, max_cols)
        actual_max_row = min(self.ws.max_row, max_rows)
        
        data = []
        for row in range(1, actual_max_row + 1):
            row_data = []
            for col in range(1, actual_max_col + 1):
                value = self.get_cell_value(row, col)
                
                # 检查是否是合并单元格的左上角
                is_merged_top_left = False
                merge_marker = ""
                try:
                    for merged_range in self.ws.merged_cells.ranges:
                        if merged_range.min_row == row and merged_range.min_col == col:
                            is_merged_top_left = True
                            # 标记合并范围
                            row_span = merged_range.max_row - merged_range.min_row + 1
                            col_span = merged_range.max_col - merged_range.min_col + 1
                            if row_span > 1 or col_span > 1:
                                merge_marker = f"[合并:行{row_span}列{col_span}]"
                            break
                except Exception:
                    # 如果无法获取合并单元格信息，跳过标记
                    pass
                
                # 转换为字符串便于分析
                if value is None:
                    # 检查是否在合并单元格内（但不是左上角）
                    is_in_merged = (row, col) in self.merged_cells_map
                    if is_in_merged and not is_merged_top_left:
                        row_data.append("[合并内]")
                    else:
                        row_data.append("")
                elif isinstance(value, (int, float)):
                    row_data.append(f"[数值:{value}]")
                else:
                    value_str = str(value)[:40]  # 截断过长内容
                    if is_merged_top_left and merge_marker:
                        row_data.append(f"{value_str}{merge_marker}")
                    else:
                        row_data.append(value_str)
            data.append(row_data)
        return data
    
    def get_merged_info(self) -> List[Dict]:
        """
        获取合并单元格信息（增强版）
        
        改进：
        1. 增加更多行数的合并单元格信息（前30行）
        2. 区分行合并和列合并
        3. 提供更详细的结构信息
        """
        merged_info = []
        try:
            for merged_range in self.ws.merged_cells.ranges:
                if merged_range.min_row <= 30:  # 关注前30行，覆盖复杂表头
                    row_span = merged_range.max_row - merged_range.min_row + 1
                    col_span = merged_range.max_col - merged_range.min_col + 1
                    value = self.ws.cell(merged_range.min_row, merged_range.min_col).value
                    value_str = str(value)[:50] if value else ""
                    
                    merged_info.append({
                        'range': str(merged_range),
                        'rows': f"{merged_range.min_row}-{merged_range.max_row}",
                        'cols': f"{merged_range.min_col}-{merged_range.max_col}",
                        'row_span': row_span,
                        'col_span': col_span,
                        'value': value_str,
                        'is_row_merge': row_span > 1,  # 是否跨行合并
                        'is_col_merge': col_span > 1,  # 是否跨列合并
                    })
            
            # 按行号排序，便于分析
            merged_info.sort(key=lambda x: int(x['rows'].split('-')[0]))
        except Exception as e:
            # 如果无法获取合并单元格信息，记录警告并返回空列表
            logger.warning(f"⚠️ 获取合并单元格信息时出错: {e}")
        
        return merged_info
    
    def analyze_with_llm(self, 
                         llm_api_key: Optional[str] = None,
                         llm_base_url: Optional[str] = None,
                         llm_model: Optional[str] = None,
                         timeout: Optional[int] = None) -> Tuple[HeaderAnalysis, str]:
        """
        使用LLM直接分析Excel表格的行和列结构
        
        参数:
            llm_api_key: LLM API密钥（可选）
            llm_base_url: LLM API地址（可选）
            llm_model: LLM模型名称（可选）
            timeout: 超时时间（秒），默认90秒
        
        返回:
            (分析结果, LLM原始响应)（如果LLM调用失败，抛出异常）
        """
        # 增加预览行数和列数，更好地展示复杂多级表头
        preview_data = self.get_preview_data(max_rows=30, max_cols=20)
        merged_info = self.get_merged_info()
        max_col = self.ws.max_column
        
        # 构建分析提示词
        prompt = self._build_llm_analysis_prompt(preview_data, merged_info, max_col)
        
        # 调用LLM（使用传入的配置或从全局配置读取）
        result = self._call_llm(prompt, llm_api_key, llm_base_url, llm_model, timeout=timeout)
        
        if not result:
            raise ValueError("LLM分析失败：无法获取LLM响应，请检查API配置")
        
        # 解析LLM分析结果
        analysis = self._parse_llm_analysis_response(result)
        
        return analysis, result
    
    def validate_with_llm(self, rule_analysis: HeaderAnalysis, 
                         llm_api_key: Optional[str] = None,
                         llm_base_url: Optional[str] = None,
                         llm_model: Optional[str] = None,
                         timeout: Optional[int] = None) -> HeaderAnalysis:
        """
        使用LLM验证规则分析的结果（已废弃，保留用于兼容性）
        
        参数:
            rule_analysis: 规则分析的结果
            llm_api_key: LLM API密钥（可选）
            llm_base_url: LLM API地址（可选）
            llm_model: LLM模型名称（可选）
            timeout: 超时时间（秒），默认30秒
        
        返回:
            验证后的分析结果（如果LLM验证失败，返回原规则分析结果）
        """
        preview_data = self.get_preview_data()
        merged_info = self.get_merged_info()
        
        # 构建验证提示词
        prompt = self._build_validation_prompt(preview_data, merged_info, rule_analysis)
        
        # 调用LLM（使用传入的配置或从全局配置读取）
        result = self._call_llm(prompt, llm_api_key, llm_base_url, llm_model, timeout=timeout)
        
        # 解析LLM验证结果
        validated = self._parse_validation_response(result, rule_analysis)
        
        return validated
    
    def _build_llm_analysis_prompt(self, preview_data: List[List], merged_info: List[Dict], 
                                   max_col: int) -> str:
        """
        构建LLM分析提示词（完全重构版）
        
        采用分步骤方法识别复杂多级表头：
        1. 先识别无效行
        2. 再识别表头层级结构
        3. 最后确定数据起始行
        """
        # 格式化预览数据为表格形式（显示更多列，并标记行特征）
        num_cols_to_show = min(20, len(preview_data[0]) if preview_data else 20)
        col_headers = " | ".join([f"列{i+1:2d}" for i in range(num_cols_to_show)])
        table_str = f"行号 | {col_headers} | 行特征\n" + "-" * 140 + "\n"
        
        for i, row in enumerate(preview_data, 1):
            row_str = " | ".join(str(cell)[:12] for cell in row[:num_cols_to_show])
            
            # 分析行的特征，便于识别无效行
            non_empty_count = sum(1 for cell in row[:num_cols_to_show] if cell and str(cell).strip() and str(cell) != "[合并内]")
            has_merge = any("[合并" in str(cell) for cell in row[:num_cols_to_show])
            has_text_label = any(
                cell and isinstance(cell, str) and 
                (len(cell) > 2) and 
                not cell.startswith("[数值") and 
                not cell.startswith("[合并") and
                not cell.startswith("[合并内")
                for cell in row[:num_cols_to_show]
            )
            
            # 标记行特征（便于识别无效行和表头行）
            row_marker = ""
            if non_empty_count <= 3 and not has_merge and not has_text_label:
                row_marker = f"⚠️可能无效行（只有{non_empty_count}个非空单元格，无表头特征）"
            elif has_merge:
                row_marker = "✅表头行（包含合并单元格）"
            elif has_text_label:
                row_marker = "✅可能表头行（包含文本标签）"
            elif non_empty_count > 5:
                row_marker = "📊可能数据行（包含多个数值）"
            
            table_str += f"  {i:2d}  | {row_str} | {row_marker}\n"
        
        # 格式化合并单元格信息（按类型分类）
        if merged_info:
            # 按行合并和列合并分类
            row_merges = [m for m in merged_info if m.get('is_row_merge', False)]
            col_merges = [m for m in merged_info if m.get('is_col_merge', False)]
            both_merges = [m for m in merged_info if m.get('is_row_merge', False) and m.get('is_col_merge', False)]
            
            merged_str = "【合并单元格详细信息】\n\n"
            
            if both_merges:
                merged_str += "同时跨行跨列的合并单元格（通常是多级表头的关键标识）：\n"
                for m in both_merges[:20]:
                    merged_str += f"  - 行{m['rows']} 列{m['cols']} (跨{m['row_span']}行{m['col_span']}列): '{m['value']}'\n"
                merged_str += "\n"
            
            if row_merges:
                merged_str += "跨行合并单元格（表示表头的层级结构）：\n"
                for m in row_merges[:20]:
                    if not (m.get('is_row_merge') and m.get('is_col_merge')):
                        merged_str += f"  - 行{m['rows']} (跨{m['row_span']}行): '{m['value']}'\n"
                merged_str += "\n"
            
            if col_merges:
                merged_str += "跨列合并单元格（表示列的分组）：\n"
                for m in col_merges[:20]:
                    if not (m.get('is_row_merge') and m.get('is_col_merge')):
                        merged_str += f"  - 列{m['cols']} (跨{m['col_span']}列): '{m['value']}'\n"
                merged_str += "\n"
            
            # 按行号汇总，便于识别表头范围
            merged_str += "按行号汇总（便于识别表头范围）：\n"
            row_merge_map = {}
            for m in merged_info[:30]:
                row_start = int(m['rows'].split('-')[0])
                row_end = int(m['rows'].split('-')[1])
                for r in range(row_start, row_end + 1):
                    if r not in row_merge_map:
                        row_merge_map[r] = []
                    row_merge_map[r].append(f"'{m['value']}'")
            
            for row_num in sorted(row_merge_map.keys())[:30]:
                merged_str += f"  行{row_num}: {', '.join(row_merge_map[row_num][:3])}\n"
        else:
            merged_str = "无合并单元格"
        
        prompt = f"""你是一个Excel表格结构分析专家。请使用分步骤方法分析以下复杂多级表头结构。

【表格预览】（前30行，[数值:xxx]表示数值类型，[合并:行X列Y]表示合并单元格，[合并内]表示合并单元格内部）
{table_str}

{merged_str}

【总列数】{max_col}

## 分析任务

请按照以下步骤分析表格结构，并返回JSON格式的结果。

## 步骤1：识别无效行（skip_rows）

**目标**：找出表头**之前**的无效行（如文档标题、说明文字、注释、公司名称等）

**无效行的典型特征**：
1. **只有少量非空单元格**（如只有1-3个单元格有值）
2. **不包含表头特征**：
   - 不包含列名（如"销售额"、"增长率"等）
   - 不包含分类标签（如"销售事业部"、"华东大区"等）
   - 不包含合并单元格（合并单元格通常是表头的标识）
3. **内容类型**（这些都是无效行）：
   - 文档标题（如"2024年度报表"、"场景业务统计表"）
   - 公司名称或部门名称（如"XX公司"、"XX部门"、"卫栖梧额外企"）
   - 说明文字或注释
   - 只有数字没有标签的行（如只有"222"、"111"、"22"这样的数字，没有对应的列名或标签）
   - 完全空行
   - **填报说明文字**（必须识别为无效行）：
     - "填报机构"、"填报日期"、"填报机构/日期"、"填报机构、日期"
     - "填报单位"、"填报人"、"填报时间"
     - 任何包含"填报"关键词的行
     - 格式如"（填报机构/日期）"、"填报机构：XX"等

**表头行的典型特征**（用于区分）：
1. **包含列名或分类标签**（如"销售事业部"、"线上销售额"等）
2. **包含合并单元格**（合并单元格是表头的重要标识）
3. **有明确的层级结构**（如大区、省份、城市、指标等）
4. **跨越多列**（表头通常跨越多列，而不是只有1-2个单元格）

**判断步骤**：
1. **从第1行开始向下检查**
2. **如果某行符合无效行特征**，继续检查下一行
3. **如果某行符合表头行特征**（特别是包含合并单元格或分类标签），则这行是表头开始
4. **skip_rows = 表头开始行号 - 1**

**示例**：
- 第1行：只有数字"222"、"111"（无标签） → 无效行
- 第2行：空行或只有少量文本 → 无效行
- 第3行：公司名称"XX公司"或"卫栖梧额外企" → 无效行
- 第4行：填报说明"填报机构/日期"或"（填报机构/日期）" → **无效行**（必须识别）
- 第5行：包含"销售事业部"和合并单元格 → **这是表头开始！**
- **结果**：skip_rows=4（前4行都是无效行，包括填报说明行）

**注意**：表头行不能算作无效行！如果第1行就包含表头特征（如合并单元格、分类标签），则 skip_rows=0

## 步骤2：识别表头层级结构（header_rows）

**目标**：找出所有表头行，包括多级表头的所有层级

**判断标准**：
1. **表头行的特征**：
   - 包含列名、分类标签、分组信息等文本内容
   - 可能包含合并单元格（合并单元格是表头的重要标识）
   - 通常不包含大量数值数据

2. **合并单元格的处理（关键）**：
   - **合并单元格跨越的所有行都是表头的一部分**
   - 例如：如果合并单元格覆盖行1-3，则行1、行2、行3都是表头
   - 即使某些行看起来"空"（值只在合并区域左上角），这些行仍然是表头
   - 查看【合并单元格详细信息】，找出所有被合并单元格覆盖的行

3. **多级表头识别**：
   - 第1层：可能有跨多行的大分类（如"销售事业部"）
   - 第2层：可能有跨多行的中分类（如"华东大区"）
   - 第3层：可能有跨多行的小分类（如"江苏省"）
   - 第4层：具体的列名（如"线上销售额"）
   - **所有层级的行都要计入 header_rows**

4. **识别方法**：
   - 从【合并单元格按行号汇总】中，找出所有包含合并单元格的行
   - 这些行通常都是表头的一部分
   - 继续向下查找，直到找到第一行包含大量数值数据的行（这是数据行）

## 步骤3：确定数据起始行（data_start_row）

**计算公式**：data_start_row = skip_rows + header_rows + 1

**验证方法**：
- 数据行通常包含大量数值数据（标记为[数值:xxx]）
- 数据行不再是表头文本或分类标签

## 步骤4：确定数据起始列（start_col）

**目标**：找出第一个表头行中第一个非空表头开始的列号

**判断标准**：
- 如果所有列都有表头，则 start_col=1
- 如果前几列为空（如第1、2列为空），从第3列开始有表头，则 start_col=3

## 输出格式

请以JSON格式返回分析结果：

```json
{{
    "skip_rows": <表头之前的无效行数，如果第1行就是表头则填0>,
    "header_rows": <表头占用的总行数（包括所有表头行和合并单元格覆盖的所有行）>,
    "header_type": "<single或multi>",
    "data_start_row": <数据开始行号（1-indexed），必须等于skip_rows+header_rows+1>,
    "start_col": <数据起始列号（1-indexed）>,
    "valid_cols": null,
    "confidence": "<high/medium/low>",
     "reason": "<详细说明：\n1. 如何识别无效行（skip_rows），特别说明是否包含填报说明、公司名称、只有数字的行等\n2. 如何识别表头层级结构（header_rows），特别说明合并单元格的处理\n3. 如何确定数据起始行和起始列>"
}}
```

## 关键规则

1. **无效行识别（重要）**：
   - **只有数字没有标签的行是无效行**（如只有"222"、"111"、"22"这样的数字，没有"销售额"、"增长率"等标签）
   - **公司名称、部门名称行是无效行**（如"XX公司"、"XX部门"、"卫栖梧额外企"）
   - **填报说明行是无效行**（如"填报机构"、"填报日期"、"填报机构/日期"、"（填报机构/日期）"等，任何包含"填报"关键词的行）
   - **空行或只有少量文本的行可能是无效行**
   - **不包含合并单元格、列名、分类标签的行通常是无效行**

2. **表头行识别（重要）**：
   - **包含合并单元格的行通常是表头**（合并单元格是表头的重要标识）
   - **包含分类标签的行是表头**（如"销售事业部"、"华东大区"等）
   - **包含列名的行是表头**（如"线上销售额"、"增长率"等）

3. **表头行不能算作skip_rows**：如果1-5行都是表头，则 skip_rows=0, header_rows=5

4. **合并单元格覆盖的所有行都是表头**：不要遗漏任何被合并单元格覆盖的行

5. **不要因为行看起来"空"就认为它不是表头**：合并单元格的值只在左上角显示

6. **行号和列号都从1开始计数**

7. **valid_cols 始终设为 null**

8. **只返回JSON，不要其他内容**

## 复杂示例

### 示例1：包含无效行的复杂表头

如果表格结构是：
- 第1行：只有数字"222"、"111"（无表头特征，无效行）
- 第2行：空行或只有少量文本（无效行）
- 第3行：公司名称"XX公司"或"卫栖梧额外企"和数字"22"（无效行）
- 第4行：填报说明"填报机构/日期"或"（填报机构/日期）"（**无效行，必须识别**）
- 第5行：合并单元格（行5，列D-I）显示"销售事业部"（表头开始！）
- 第6行：合并单元格显示"华东大区"、"华北大区"等（表头）
- 第7-9行：多级表头（表头）
- 第10行：数据开始（包含数值）

**分析过程**：
1. **识别无效行**：
   - 第1行：只有数字，无表头特征 → 无效行
   - 第2行：空行 → 无效行
   - 第3行：公司名称，无表头特征 → 无效行
   - 第4行：填报说明"填报机构/日期" → **无效行**（包含"填报"关键词）
   - 第5行：包含"销售事业部"和合并单元格 → **这是表头开始！**
   - skip_rows=4（前4行都是无效行，包括填报说明行）

2. **识别表头**：
   - 第5行：合并单元格显示"销售事业部" → 表头
   - 第6-9行：多级表头结构 → 表头
   - header_rows=5（行5-9都是表头）

3. **确定数据起始行**：
   - data_start_row=10（skip_rows+header_rows+1=4+5+1）

**正确输出**：
```json
{{
    "skip_rows": 4,
    "header_rows": 5,
    "header_type": "multi",
    "data_start_row": 10,
    "start_col": 1,
    "valid_cols": null,
    "confidence": "high",
     "reason": "第1-4行是无效行：第1行只有数字无表头特征，第2行空行，第3行公司名称，第4行填报说明'填报机构/日期'。第5行开始是表头（包含'销售事业部'和合并单元格），所以skip_rows=4。第5-9行是多级表头，所以header_rows=5。第10行开始包含数值数据，所以data_start_row=10。"
}}
```

### 示例2：无无效行的表头

如果表格结构是：
- 第1行：合并单元格（行1-2，列1-5）显示"销售事业部"（大分类）
- 第3行：合并单元格（行3-4，列1-3）显示"华东大区"（中分类）
- 第5行：具体列名（"线上销售额"、"线下销售额"等）
- 第6行：数据开始（包含数值）

**分析过程**：
1. skip_rows=0（第1行就是表头）
2. header_rows=5（行1-5都是表头，包括合并单元格覆盖的行2和行4）
3. data_start_row=6（skip_rows+header_rows+1=0+5+1）

**正确输出**：
```json
{{
    "skip_rows": 0,
    "header_rows": 5,
    "header_type": "multi",
    "data_start_row": 6,
    "start_col": 1,
    "valid_cols": null,
    "confidence": "high",
    "reason": "第1行开始就是表头，skip_rows=0。第1行有合并单元格（行1-2），第3行有合并单元格（行3-4），第5行是具体列名，所以header_rows=5。第6行开始包含数值数据，所以data_start_row=6。"
}}
```"""
        
        return prompt
    
    def _build_validation_prompt(self, preview_data: List[List], merged_info: List[Dict], 
                                rule_analysis: HeaderAnalysis) -> str:
        """
        构建LLM验证提示词（重构版）
        
        使用与分析方法一致的格式和逻辑
        """
        # 格式化预览数据为表格形式（显示更多列）
        num_cols_to_show = min(20, len(preview_data[0]) if preview_data else 20)
        col_headers = " | ".join([f"列{i+1:2d}" for i in range(num_cols_to_show)])
        table_str = f"行号 | {col_headers}\n" + "-" * 120 + "\n"
        
        for i, row in enumerate(preview_data, 1):
            row_str = " | ".join(str(cell)[:12] for cell in row[:num_cols_to_show])
            table_str += f"  {i:2d}  | {row_str}\n"
        
        # 格式化合并单元格信息（与分析方法一致）
        if merged_info:
            # 按行合并和列合并分类
            row_merges = [m for m in merged_info if m.get('is_row_merge', False)]
            col_merges = [m for m in merged_info if m.get('is_col_merge', False)]
            both_merges = [m for m in merged_info if m.get('is_row_merge', False) and m.get('is_col_merge', False)]
            
            merged_str = "【合并单元格详细信息】\n\n"
            
            if both_merges:
                merged_str += "同时跨行跨列的合并单元格：\n"
                for m in both_merges[:20]:
                    merged_str += f"  - 行{m['rows']} 列{m['cols']} (跨{m['row_span']}行{m['col_span']}列): '{m['value']}'\n"
                merged_str += "\n"
            
            if row_merges:
                merged_str += "跨行合并单元格：\n"
                for m in row_merges[:20]:
                    if not (m.get('is_row_merge') and m.get('is_col_merge')):
                        merged_str += f"  - 行{m['rows']} (跨{m['row_span']}行): '{m['value']}'\n"
                merged_str += "\n"
            
            # 按行号汇总
            merged_str += "按行号汇总：\n"
            row_merge_map = {}
            for m in merged_info[:30]:
                row_start = int(m['rows'].split('-')[0])
                row_end = int(m['rows'].split('-')[1])
                for r in range(row_start, row_end + 1):
                    if r not in row_merge_map:
                        row_merge_map[r] = []
                    row_merge_map[r].append(f"'{m['value']}'")
            
            for row_num in sorted(row_merge_map.keys())[:30]:
                merged_str += f"  行{row_num}: {', '.join(row_merge_map[row_num][:3])}\n"
        else:
            merged_str = "无合并单元格"
        
        prompt = f"""请验证以下Excel表格的表头分析结果是否正确。

【表格预览】（前30行，[数值:xxx]表示数值类型，[合并:行X列Y]表示合并单元格，[合并内]表示合并单元格内部）
{table_str}

{merged_str}

【当前分析结果】
- 跳过行数(skip_rows): {rule_analysis.skip_rows} （表头之前的无效行数）
- 表头行数(header_rows): {rule_analysis.header_rows} （所有表头行的总数，包括合并单元格覆盖的所有行）
- 表头类型: {rule_analysis.header_type}
- 数据起始行: {rule_analysis.data_start_row} （应该等于 skip_rows + header_rows + 1）
- 分析原因: {rule_analysis.reason}

请验证这个结果是否合理，并以JSON格式返回：
{{
    "is_valid": <true或false，表示结果是否合理>,
    "confidence": "<high/medium/low>",
    "suggestions": {{
        "skip_rows": <建议的跳过行数，如果合理则与当前结果相同>,
        "header_rows": <建议的表头行数，如果合理则与当前结果相同>,
        "header_type": "<single或multi>",
        "data_start_row": <建议的数据起始行，应该等于skip_rows+header_rows+1>
    }},
    "reason": "<验证说明：如果合理，说明为什么；如果不合理，指出问题并给出建议>"
}}

## 验证要点（重要）

### 1. skip_rows 验证
- **关键**：skip_rows 只计算**表头之前**的无效行（如文档标题、注释等）
- **常见错误**：不要把表头行算作skip_rows
- **判断标准**：
  - 如果第1行就是表头（包含列名、分类标签），则 skip_rows 应该为 0
  - 如果前N行是文档标题等非表头内容，第N+1行才是表头，则 skip_rows 应该为 N

### 2. header_rows 验证
- **关键**：header_rows 应该包含**所有表头行**，包括多级表头的所有行
- **判断标准**：
  - 单表头：只有一行表头 → header_rows=1
  - 多级表头：如果1-5行都是表头，则 header_rows=5
  - **合并单元格处理（重要）**：
    - 合并单元格跨越的所有行都应该计入 header_rows
    - 即使合并单元格让某些行看起来"空"（值只在合并区域左上角），这些行仍然是表头的一部分
    - 查看【合并单元格】信息，确认所有被合并的行都包含在 header_rows 中
    - 例如：如果合并单元格显示"行1-2"，则行1和行2都应该计入 header_rows

### 3. data_start_row 验证
- **计算公式**：data_start_row 必须等于 skip_rows + header_rows + 1
- **判断标准**：数据行通常包含数值数据，不再是表头文本

### 4. 常见错误检查
- ❌ **错误**：如果1-5行都是表头，但分析结果是 skip_rows=3, header_rows=3
  - **正确**：应该是 skip_rows=0, header_rows=5
- ❌ **错误**：把表头行误判为需要跳过的无效行
- ❌ **错误**：遗漏了多级表头的某些行
- ❌ **错误（合并单元格）**：如果合并单元格覆盖行1-2，但只计算了行1，遗漏了行2
  - **正确**：合并单元格覆盖的所有行（行1-2）都应该计入 header_rows
- ❌ **错误（合并单元格）**：因为合并单元格让某些行看起来"空"，就认为这些行不是表头
  - **正确**：即使行看起来空（值在合并区域左上角），这些行仍然是表头的一部分

## 验证步骤

1. **首先查看合并单元格信息**：识别哪些行被合并单元格覆盖，这些行都应该计入 header_rows
2. **确认表头范围**：从第几行到第几行是表头？（包括合并单元格覆盖的所有行）
3. **检查skip_rows**：表头之前是否有无效行？如果没有，skip_rows应该为0
4. **验证header_rows**：
   - 是否包含了所有表头行？
   - **特别注意**：合并单元格覆盖的所有行是否都包含在内？
   - 不要因为某些行看起来"空"就认为它们不是表头
5. **验证data_start_row**：是否等于 skip_rows + header_rows + 1？

如果当前分析结果合理，保持原结果；如果不合理，在suggestions中给出修正建议。

只返回JSON，不要其他内容"""
        
        return prompt
    
    def _call_llm(self, prompt: str, llm_api_key: Optional[str] = None, 
                  llm_base_url: Optional[str] = None, llm_model: Optional[str] = None,
                  timeout: Optional[int] = None) -> str:
        """调用LLM API（支持OpenAI兼容接口）
        
        参数:
            prompt: 提示词
            llm_api_key: LLM API密钥（可选，如果不提供则从配置读取）
            llm_base_url: LLM API地址（可选，如果不提供则从配置读取）
            llm_model: LLM模型名称（可选，如果不提供则从配置读取）
            timeout: 超时时间（秒），默认30秒
        """
        # 优先使用传入的参数，否则从配置读取
        api_key = llm_api_key if llm_api_key is not None else EXCEL_LLM_API_KEY
        base_url = llm_base_url if llm_base_url is not None else EXCEL_LLM_BASE_URL
        model = llm_model if llm_model is not None else EXCEL_LLM_MODEL
        
        logger.info("=" * 60)
        logger.info("🤖 调用 LLM API 进行Excel表格分析")
        logger.info(f"🔗 EXCEL_LLM_BASE_URL: {base_url}")
        logger.info(f"📌 模型: {model}")
        logger.info(f"🔑 API Key: {'已配置' if api_key else '未配置'}")
        
        if not api_key:
            logger.error("❌ 未配置 LLM API Key，无法进行分析")
            return None
            
        url = base_url
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        # 使用流式调用以支持 thinking 功能
        base_payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.4,
            "max_tokens": 1000,  # 增加token数量以支持更详细的分析
            "stream": True,  # 流式调用
        }
        
        # 使用传入的超时时间，默认90秒
        request_timeout = timeout if timeout is not None else 90
        
        logger.info(f"📡 发送 LLM API 请求到: {url} (流式调用)")
        logger.info(f"📝 提示词长度: {len(prompt)} 字符")
        logger.info(f"⏱️ 超时设置: {request_timeout} 秒")
        
        try:
            # 优先尝试启用 thinking 功能
            payload_with_thinking = base_payload.copy()
            payload_with_thinking["enable_thinking"] = True
            
            logger.debug(f"📦 请求 payload (启用 thinking): {json.dumps(payload_with_thinking, ensure_ascii=False, indent=2)}")
            
            response = requests.post(
                url, 
                headers=headers, 
                json=payload_with_thinking, 
                timeout=request_timeout,
                stream=True  # 启用流式响应
            )
            
            # 如果启用 thinking 失败，回退到不使用 thinking
            if response.status_code != 200:
                try:
                    error_json = response.json()
                    if "enable_thinking" in str(error_json).lower():
                        logger.warning("⚠️ 启用 thinking 失败，尝试不使用 thinking")
                        payload_no_thinking = base_payload.copy()
                        logger.debug(f"📦 请求 payload (不使用 thinking): {json.dumps(payload_no_thinking, ensure_ascii=False, indent=2)}")
                        response = requests.post(
                            url, 
                            headers=headers, 
                            json=payload_no_thinking, 
                            timeout=request_timeout,
                            stream=True
                        )
                except:
                    pass
            
            # 如果请求失败，输出详细的错误信息
            if response.status_code != 200:
                error_detail = ""
                try:
                    # 对于流式响应，尝试读取错误信息
                    error_text = ""
                    for line in response.iter_lines():
                        if line:
                            line_str = line.decode('utf-8')
                            if line_str.startswith('data: '):
                                line_str = line_str[6:]
                            try:
                                error_json = json.loads(line_str)
                                error_detail = json.dumps(error_json, ensure_ascii=False, indent=2)
                                break
                            except:
                                error_text += line_str + "\n"
                    if not error_detail:
                        error_detail = error_text or response.text
                except:
                    try:
                        error_detail = response.text
                    except:
                        error_detail = f"无法读取错误详情 (状态码: {response.status_code})"
                
                logger.error(f"❌ LLM API 调用失败 (状态码: {response.status_code})")
                logger.error(f"📋 错误详情:\n{error_detail}")
                logger.error(f"🔗 请求 URL: {url}")
                logger.error(f"📦 请求 payload: {json.dumps(base_payload, ensure_ascii=False, indent=2)}")
                return None
            
            # 处理流式响应
            full_content = ""
            for line in response.iter_lines():
                if line:
                    line_str = line.decode('utf-8')
                    # 跳过 SSE 格式的前缀 "data: "
                    if line_str.startswith('data: '):
                        line_str = line_str[6:]
                    
                    # 检查是否是结束标记
                    if line_str.strip() == '[DONE]':
                        break
                    
                    # 解析 JSON
                    try:
                        chunk_data = json.loads(line_str)
                        if 'choices' in chunk_data and len(chunk_data['choices']) > 0:
                            delta = chunk_data['choices'][0].get('delta', {})
                            content = delta.get('content', '')
                            if content:
                                full_content += content
                    except json.JSONDecodeError:
                        # 忽略无法解析的行（可能是空行或其他格式）
                        continue
            
            if not full_content:
                logger.warning("⚠️ LLM 流式响应为空")
                return None
            
            logger.info("✅ LLM API 调用成功")
            logger.info("=" * 60)
            logger.info("📝 LLM 响应内容:")
            logger.info("=" * 60)
            logger.info(full_content)
            logger.info("=" * 60)
            
            return full_content
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ LLM调用失败 (网络错误): {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_json = e.response.json()
                    logger.error(f"📋 API 错误响应: {json.dumps(error_json, ensure_ascii=False, indent=2)}")
                except:
                    logger.error(f"📋 API 错误响应 (文本): {e.response.text}")
            logger.debug("异常详情:", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"❌ LLM调用失败: {e}")
            logger.debug("异常详情:", exc_info=True)
            return None
    
    def _parse_llm_analysis_response(self, response: str) -> HeaderAnalysis:
        """解析LLM分析结果（包含行和列信息）"""
        if not response:
            raise ValueError("LLM响应为空")
        
        try:
            # 提取JSON部分（支持嵌套JSON）
            # 先尝试找到第一个 { 到最后一个 } 之间的内容
            start_idx = response.find('{')
            end_idx = response.rfind('}')
            if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                json_str = response[start_idx:end_idx + 1]
                data = json.loads(json_str)
            else:
                # 如果找不到完整的JSON，尝试用正则匹配
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if not json_match:
                    raise ValueError("未找到JSON格式的响应")
                data = json.loads(json_match.group())
            
            # 解析有效列
            valid_cols = data.get('valid_cols')
            if valid_cols is None:
                # 如果为null，表示所有列都有效
                valid_cols = None
            elif isinstance(valid_cols, list):
                # 确保是整数列表
                valid_cols = [int(col) for col in valid_cols if isinstance(col, (int, str)) and str(col).isdigit()]
                # 如果列表为空或包含所有列，设为None
                max_col = self.ws.max_column
                if not valid_cols or set(valid_cols) == set(range(1, max_col + 1)):
                    valid_cols = None
            else:
                valid_cols = None
            
            # 解析起始列（默认为1）
            start_col = int(data.get('start_col', 1))
            if start_col < 1:
                start_col = 1
            
            # 构建HeaderAnalysis对象
            analysis = HeaderAnalysis(
                skip_rows=int(data.get('skip_rows', 0)),
                header_rows=int(data.get('header_rows', 1)),
                header_type=data.get('header_type', 'single'),
                data_start_row=int(data.get('data_start_row', 1)),
                start_col=start_col,
                confidence=data.get('confidence', 'medium'),
                reason=f"LLM分析: {data.get('reason', '')}",
                valid_cols=valid_cols
            )
            
            logger.info(f"✅ LLM分析完成:")
            logger.info(f"  - 跳过行数: {analysis.skip_rows}")
            logger.info(f"  - 表头行数: {analysis.header_rows}")
            logger.info(f"  - 表头类型: {analysis.header_type}")
            logger.info(f"  - 数据起始行: {analysis.data_start_row}")
            logger.info(f"  - 数据起始列: {analysis.start_col}")
            logger.info(f"  - 置信度: {analysis.confidence}")
            
            return analysis
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"❌ 解析LLM分析响应失败: {e}")
            logger.error(f"📋 响应内容: {response[:500]}")
            raise ValueError(f"解析LLM分析响应失败: {e}")
    
    def _parse_validation_response(self, response: str, rule_analysis: HeaderAnalysis) -> HeaderAnalysis:
        """解析LLM验证结果（已废弃，保留用于兼容性）"""
        if not response:
            # LLM调用失败，返回原规则分析结果
            return rule_analysis
        
        try:
            # 提取JSON部分（支持嵌套JSON）
            # 先尝试找到第一个 { 到最后一个 } 之间的内容
            start_idx = response.find('{')
            end_idx = response.rfind('}')
            if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                json_str = response[start_idx:end_idx + 1]
                data = json.loads(json_str)
            else:
                # 如果找不到完整的JSON，尝试用正则匹配
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if not json_match:
                    raise ValueError("未找到JSON格式的响应")
                data = json.loads(json_match.group())
            
            is_valid = data.get('is_valid', True)
            suggestions = data.get('suggestions', {})
            
            if is_valid:
                # LLM认为规则分析结果合理，保持原结果但更新置信度和原因
                return HeaderAnalysis(
                    skip_rows=rule_analysis.skip_rows,
                    header_rows=rule_analysis.header_rows,
                    header_type=rule_analysis.header_type,
                    data_start_row=rule_analysis.data_start_row,
                    start_col=rule_analysis.start_col,  # 保持原有的起始列
                    confidence=data.get('confidence', 'high'),  # LLM验证通过，置信度提升
                    reason=f"规则分析+LLM验证: {data.get('reason', '验证通过')}",
                    valid_cols=rule_analysis.valid_cols  # 保持原有的列过滤结果
                )
            else:
                # LLM认为不合理，使用LLM的建议
                # 注意：LLM可能建议修改表头行数，但列过滤结果仍然保留
                return HeaderAnalysis(
                    skip_rows=suggestions.get('skip_rows', rule_analysis.skip_rows),
                    header_rows=suggestions.get('header_rows', rule_analysis.header_rows),
                    header_type=suggestions.get('header_type', rule_analysis.header_type),
                    data_start_row=suggestions.get('data_start_row', rule_analysis.data_start_row),
                    start_col=suggestions.get('start_col', rule_analysis.start_col),  # 保持或使用建议的起始列
                    confidence=data.get('confidence', 'medium'),
                    reason=f"规则分析+LLM修正: {data.get('reason', 'LLM建议修正')}",
                    valid_cols=rule_analysis.valid_cols  # 保持原有的列过滤结果
                )
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"解析LLM验证响应失败: {e}，使用原规则分析结果")
        
        # 解析失败，返回原规则分析结果
        return rule_analysis
    
    # 已废弃：规则分析方法，现在必须使用LLM分析
    # def analyze_with_rules(self) -> HeaderAnalysis:
    #     """基于规则的分析（已废弃，现在必须使用LLM分析）"""
    #     max_col = self.ws.max_column
    #     skip_rows = 0
    #     header_rows = 1
    #     
    #     # 检测需要跳过的行
    #     for row in range(1, min(6, self.ws.max_row + 1)):
    #         row_values = [self.get_cell_value(row, col) for col in range(1, max_col + 1)]
    #         non_empty = sum(1 for v in row_values if v is not None)
    #         
    #         # 如果只有很少的非空单元格，可能是标题行
    #         if non_empty <= 2 and non_empty < max_col * 0.3:
    #             skip_rows = row
    #         else:
    #             break
    #     
    #     # 检测表头行数
    #     header_start = skip_rows + 1
    #     
    #     # 检查合并单元格
    #     max_merged_row = 0
    #     for merged_range in self.ws.merged_cells.ranges:
    #         if merged_range.min_row > skip_rows:
    #             if merged_range.max_row > max_merged_row:
    #                 max_merged_row = merged_range.max_row
    #     
    #     if max_merged_row > header_start:
    #         header_rows = max_merged_row - skip_rows
    #     
    #     # 检测数据行开始位置
    #     data_start = skip_rows + header_rows + 1
    #     for row in range(header_start, min(skip_rows + 10, self.ws.max_row + 1)):
    #         row_values = [self.get_cell_value(row, col) for col in range(1, max_col + 1)]
    #         non_empty = sum(1 for v in row_values if v is not None)
    #         numeric = sum(1 for v in row_values if isinstance(v, (int, float)) and not isinstance(v, bool))
    #         
    #         if non_empty > 0 and numeric / max(non_empty, 1) > 0.4:
    #             data_start = row
    #             header_rows = row - skip_rows - 1
    #             break
    #     
    #     header_type = 'multi' if header_rows > 1 else 'single'
    #     
    #     return HeaderAnalysis(
    #         skip_rows=skip_rows,
    #         header_rows=max(1, header_rows),
    #         header_type=header_type,
    #         data_start_row=data_start,
    #         confidence='medium',
    #         reason='基于规则分析',
    #         valid_cols=None
    #     )
    
    def _detect_valid_columns(self, skip_rows: int, header_rows: int, data_start_row: int) -> List[int]:
        """
        检测有效列（过滤无效列）
        
        无效列的判断标准：
        1. 表头区域完全为空
        2. 数据区域完全为空或没有数值数据
        
        返回: 有效列的索引列表（1-indexed）
        """
        max_col = self.ws.max_column
        header_start = skip_rows + 1
        header_end = skip_rows + header_rows
        valid_cols = []
        
        logger.info("🔍 开始检测无效列...")
        
        for col in range(1, max_col + 1):
            # 检查表头区域是否有内容
            has_header = False
            for row in range(header_start, header_end + 1):
                value = self.get_cell_value(row, col)
                if value is not None and str(value).strip():
                    has_header = True
                    break
            
            # 检查数据区域是否有数值数据
            has_data = False
            numeric_count = 0
            total_count = 0
            for row in range(data_start_row, min(data_start_row + 10, self.ws.max_row + 1)):
                value = self.ws.cell(row, col).value
                if value is not None:
                    total_count += 1
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        numeric_count += 1
                        has_data = True
            
            # 如果表头有内容或数据区域有数值，则认为是有效列
            if has_header or has_data:
                valid_cols.append(col)
                logger.debug(f"✅ 列 {col}: 有效 (表头: {has_header}, 数据: {has_data}, 数值: {numeric_count}/{total_count})")
            else:
                logger.info(f"❌ 列 {col}: 无效 (表头为空且数据为空)")
        
        logger.info(f"📊 列过滤结果: 总列数 {max_col}, 有效列数 {len(valid_cols)}, 无效列数 {max_col - len(valid_cols)}")
        
        # 如果所有列都有效，返回None（表示不需要过滤）
        if len(valid_cols) == max_col:
            return None
        
        return valid_cols
    
    def extract_headers(self, analysis: HeaderAnalysis) -> Tuple[List[str], Dict[str, Dict]]:
        """
        根据分析结果提取表头
        返回: (列名列表, 列结构元数据)
        """
        max_col = self.ws.max_column
        header_start = analysis.skip_rows + 1
        header_end = analysis.skip_rows + analysis.header_rows
        
        # 确定要处理的列：从 start_col 开始，如果指定了有效列，则取交集
        all_cols = list(range(analysis.start_col, max_col + 1))
        if analysis.valid_cols is not None:
            # 取交集：从 start_col 开始，且在 valid_cols 中的列
            cols_to_process = [col for col in all_cols if col in analysis.valid_cols]
        else:
            cols_to_process = all_cols
        
        logger.info(f"📋 提取表头: 处理 {len(cols_to_process)} 列")
        
        column_metadata = {}
        
        if analysis.header_type == 'single':
            # 单表头
            headers = []
            for col in cols_to_process:
                value = self.get_cell_value(header_start, col)
                col_name = str(value) if value else f'Column_{col}'
                headers.append(col_name)
                column_metadata[col_name] = {"level1": col_name}
            
            headers = self._handle_duplicate_names(headers)
            # 更新元数据的key
            column_metadata = {h: {"level1": h} for h in headers}
            return headers, column_metadata
        
        else:
            # 多表头：展平
            column_headers = []
            original_metadata_list = []  # 保存原始元数据列表，按顺序对应
            
            for col in cols_to_process:
                parts = []
                levels = {}
                for row_idx, row in enumerate(range(header_start, header_end + 1), 1):
                    value = self.get_cell_value(row, col)
                    if value is not None:
                        part = str(value).strip()
                        parts.append(part)
                        levels[f"level{row_idx}"] = part
                
                # 去重连续相同值
                unique_parts = []
                for p in parts:
                    if not unique_parts or p != unique_parts[-1]:
                        unique_parts.append(p)
                
                col_name = '_'.join(unique_parts) if unique_parts else f'Column_{col}'
                column_headers.append(col_name)
                original_metadata_list.append(levels)  # 按顺序保存元数据
            
            # 处理重复列名
            column_headers = self._handle_duplicate_names(column_headers)
            
            # 重新映射元数据：使用索引对应关系
            new_metadata = {}
            for i, header in enumerate(column_headers):
                # 使用索引直接获取对应的元数据
                if i < len(original_metadata_list):
                    new_metadata[header] = original_metadata_list[i]
                else:
                    # 如果索引超出范围，创建默认元数据
                    logger.warning(f"⚠️ 索引超出范围: i={i}, headers长度={len(column_headers)}, metadata长度={len(original_metadata_list)}")
                    new_metadata[header] = {"level1": header}
            
            return column_headers, new_metadata
    
    def _handle_duplicate_names(self, names: List[str]) -> List[str]:
        """处理重复列名"""
        counts = defaultdict(int)
        result = []
        for name in names:
            if counts[name] > 0:
                result.append(f"{name}_{counts[name]}")
            else:
                result.append(name)
            counts[name] += 1
        return result
    
    def to_dataframe(self, analysis: HeaderAnalysis = None, use_llm_validate: bool = False,
                    llm_api_key: Optional[str] = None,
                    llm_base_url: Optional[str] = None,
                    llm_model: Optional[str] = None,
                    preprocessing_timeout: Optional[int] = None) -> Tuple[pd.DataFrame, HeaderAnalysis, Dict[str, Dict], Optional[str]]:
        """
        转换为DataFrame
        
        参数:
            analysis: 预先的分析结果，如果为None则使用LLM自动分析
            use_llm_validate: 已废弃，保留用于兼容性
            llm_api_key: LLM API密钥（必填）
            llm_base_url: LLM API地址（可选）
            llm_model: LLM模型名称（可选）
            preprocessing_timeout: 预处理超时时间（秒），默认90秒
        
        返回:
            (DataFrame, 分析结果, 列结构元数据, LLM原始响应)
        """
        llm_response = None
        if analysis is None:
            # 必须使用LLM进行分析（同时分析行和列）
            logger.info("🤖 开始使用LLM分析Excel表格结构（行和列）...")
            
            # 优先使用传入的配置，否则使用全局配置
            api_key = llm_api_key if llm_api_key is not None else EXCEL_LLM_API_KEY
            if not api_key:
                raise ValueError("LLM API密钥未配置，无法进行Excel分析。请配置EXCEL_LLM_API_KEY或传入llm_api_key参数")
            
            # 使用LLM直接分析（包含行和列信息）
            analysis, llm_response = self.analyze_with_llm(
                llm_api_key=llm_api_key,
                llm_base_url=llm_base_url,
                llm_model=llm_model,
                timeout=preprocessing_timeout
            )
            logger.info("✅ LLM分析完成（已包含行和列信息）")
            # 保存LLM响应到实例变量，以便后续使用
            self._llm_analysis_response = llm_response
        
        headers, column_metadata = self.extract_headers(analysis)
        
        # 确定要读取的列：从 start_col 开始，如果指定了有效列，则取交集
        max_col = self.ws.max_column
        all_cols = list(range(analysis.start_col, max_col + 1))
        if analysis.valid_cols is not None:
            # 取交集：从 start_col 开始，且在 valid_cols 中的列
            cols_to_read = [col for col in all_cols if col in analysis.valid_cols]
        else:
            cols_to_read = all_cols
        
        logger.info(f"📊 读取数据: 从第 {analysis.start_col} 列开始，共 {len(cols_to_read)} 列")
        
        # 读取数据
        data = []
        for row in range(analysis.data_start_row, self.ws.max_row + 1):
            row_data = []
            for col in cols_to_read:
                row_data.append(self.ws.cell(row, col).value)
            if any(v is not None for v in row_data):
                data.append(row_data)
        
        df = pd.DataFrame(data, columns=headers)
        
        # 智能类型转换：尝试将数字字符串转换为数字
        logger.info("🔄 开始智能类型转换...")
        def smart_convert_value(value):
            """智能转换值：尝试将数字字符串转换为数字"""
            if value is None:
                return value
            if isinstance(value, (int, float)):
                return value
            if isinstance(value, str):
                # 去除前后空格
                value = value.strip()
                if not value:  # 空字符串
                    return None
                # 尝试转换为数字
                try:
                    # 尝试整数（支持负数）
                    if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                        return int(value)
                    # 尝试浮点数（支持科学计数法）
                    return float(value)
                except (ValueError, AttributeError):
                    # 转换失败，保持原字符串
                    return value
            return value
        
        # 对每列应用智能转换
        for col in df.columns:
            original_type = df[col].dtype
            df[col] = df[col].apply(smart_convert_value)
            new_type = df[col].dtype
            if original_type != new_type:
                logger.debug(f"  列 '{col}': {original_type} → {new_type}")
        
        # 使用 pandas 的 convert_dtypes 进一步优化类型推断
        df = df.convert_dtypes()
        
        logger.info(f"✅ DataFrame 创建完成: {len(df)} 行 x {len(df.columns)} 列")
        logger.info(f"📊 数据类型优化完成")
        return df, analysis, column_metadata, llm_response
    
    def close(self):
        """关闭工作簿并清理临时文件"""
        try:
            self.wb.close()
        except Exception:
            pass
        
        # 删除临时转换的 .xlsx 文件
        if self._temp_xlsx_path and os.path.exists(self._temp_xlsx_path):
            try:
                os.remove(self._temp_xlsx_path)
                logger.debug(f"🗑️ 已删除临时文件: {self._temp_xlsx_path}")
            except Exception as e:
                logger.warning(f"⚠️ 删除临时文件失败: {self._temp_xlsx_path}, 错误: {e}")


def process_excel_file(
    filepath: str,
    output_dir: str,
    sheet_name: str = None,
    use_llm_validate: bool = False,  # 已废弃，保留用于兼容性，现在总是使用LLM
    output_filename: str = None,
    llm_api_key: Optional[str] = None,
    llm_base_url: Optional[str] = None,
    llm_model: Optional[str] = None,
    preprocessing_timeout: Optional[int] = None,
    excel_processing_timeout: Optional[int] = None  # Excel处理超时时间（秒），在LLM分析之前
) -> ExcelProcessResult:
    """
    处理Excel文件的主函数
    
    参数:
        filepath: Excel文件路径
        output_dir: 输出目录
        sheet_name: 工作表名称
        use_llm_validate: 已废弃，保留用于兼容性。现在总是使用LLM进行分析
        output_filename: 输出文件名（不含扩展名）
        llm_api_key: LLM API密钥（必填，否则会抛出异常）
        llm_base_url: LLM API地址（可选）
        llm_model: LLM模型名称（可选）
        preprocessing_timeout: 预处理超时时间（秒），默认90秒
        excel_processing_timeout: Excel处理超时时间（秒），在LLM分析之前，默认10秒
    
    返回:
        ExcelProcessResult
    
    注意:
        现在必须使用LLM进行分析，不再支持规则分析。请确保提供llm_api_key参数。
    """
    try:
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 记录开始时间，用于超时检查
        start_time = time.time()
        excel_processing_timeout_seconds = excel_processing_timeout if excel_processing_timeout is not None else 10
        
        # 处理Excel（现在总是使用LLM分析）
        processor = SmartHeaderProcessor(filepath, sheet_name)
        
        # 检查是否超时（在LLM分析之前）
        elapsed_time = time.time() - start_time
        if elapsed_time > excel_processing_timeout_seconds:
            processor.close()
            error_msg = "Excel内容过多或格式太复杂，解析失败"
            logger.error(f"❌ Excel处理超时: 耗时 {elapsed_time:.2f}秒，超过限制 {excel_processing_timeout_seconds}秒")
            return ExcelProcessResult(
                success=False,
                header_analysis=None,
                processed_file_path=None,
                metadata_file_path=None,
                column_names=[],
                column_metadata={},
                row_count=0,
                error_message=error_msg
            )
        
        df, analysis, column_metadata, llm_response = processor.to_dataframe(
            use_llm_validate=True,  # 总是使用LLM，忽略传入的use_llm_validate参数
            llm_api_key=llm_api_key,
            llm_base_url=llm_base_url,
            llm_model=llm_model,
            preprocessing_timeout=preprocessing_timeout
        )
        processor.close()
        
        # 生成输出文件名
        if not output_filename:
            base_name = Path(filepath).stem
            output_filename = f"{base_name}_processed"
        
        # 保存CSV
        csv_path = os.path.join(output_dir, f"{output_filename}.csv")
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        # 提取字段值样本（分组聚合后的常见值）
        logger.info("📊 提取字段值样本...")
        column_value_samples = extract_column_value_samples(df, max_samples_per_column=10)
        
        # 将值样本信息合并到列元数据中
        for col_name, samples in column_value_samples.items():
            if col_name in column_metadata:
                column_metadata[col_name]["value_samples"] = samples
            else:
                # 如果列不在元数据中（理论上不应该发生），创建新的元数据项
                column_metadata[col_name] = {"value_samples": samples}
        
        # 保存元数据
        metadata = {
            "header_analysis": analysis.to_dict(),
            "column_metadata": column_metadata,
            "column_names": list(df.columns),
            "row_count": len(df),
            "original_file": os.path.basename(filepath)
        }
        metadata_path = os.path.join(output_dir, f"{output_filename}_metadata.json")
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        # 打印处理后的JSON元数据（暂时注释）
        # logger.info("=" * 80)
        # logger.info("📄 处理后的JSON元数据:")
        # logger.info("=" * 80)
        # logger.info(json.dumps(metadata, ensure_ascii=False, indent=2))
        # logger.info("=" * 80)
        
        return ExcelProcessResult(
            success=True,
            header_analysis=analysis,
            processed_file_path=csv_path,
            metadata_file_path=metadata_path,
            column_names=list(df.columns),
            column_metadata=column_metadata,
            row_count=len(df),
            error_message=None,
            llm_analysis_response=llm_response
        )
        
    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        return ExcelProcessResult(
            success=False,
            header_analysis=None,
            processed_file_path=None,
            metadata_file_path=None,
            column_names=[],
            column_metadata={},
            row_count=0,
            error_message=error_msg
        )


def get_sheet_names(filepath: str) -> List[str]:
    """获取Excel文件的所有工作表名称"""
    try:
        wb = load_workbook(filepath)
        sheets = wb.sheetnames
        wb.close()
        return sheets
    except Exception as e:
        return []


def extract_column_value_samples(
    df: pd.DataFrame,
    max_samples_per_column: int = 10,
    max_unique_ratio: float = 0.5
) -> Dict[str, Dict[str, Any]]:
    """
    提取每个字段的常见值样本（通过分组聚合）
    
    参数:
        df: 数据框
        max_samples_per_column: 每个字段最多保留的样本数量
        max_unique_ratio: 如果唯一值占比超过此比例，则只提供统计信息而不统计频率
    
    返回:
        字典，key为列名，value为包含常见值和统计信息的字典
    """
    column_samples = {}
    
    for col_name in df.columns:
        col_data = df[col_name]
        
        # 跳过完全为空的列
        if col_data.isna().all():
            continue
        
        # 计算非空值数量
        non_null_count = col_data.notna().sum()
        if non_null_count == 0:
            continue
        
        # 计算唯一值数量
        unique_count = col_data.nunique()
        unique_ratio = unique_count / non_null_count if non_null_count > 0 else 1.0
        
        sample_info = {
            "total_count": len(col_data),
            "non_null_count": int(non_null_count),
            "null_count": int(col_data.isna().sum()),
            "unique_count": int(unique_count),
            "data_type": str(col_data.dtype)
        }
        
        # 判断是否为数值类型
        is_numeric = pd.api.types.is_numeric_dtype(col_data)
        
        if is_numeric:
            # 数值类型：提供统计信息和常见值（如果唯一值不太多）
            sample_info["is_numeric"] = True
            non_null_data = col_data.dropna()
            if len(non_null_data) > 0:
                sample_info["min"] = float(non_null_data.min())
                sample_info["max"] = float(non_null_data.max())
                sample_info["mean"] = float(non_null_data.mean())
                sample_info["median"] = float(non_null_data.median())
            else:
                sample_info["min"] = None
                sample_info["max"] = None
                sample_info["mean"] = None
                sample_info["median"] = None
            
            # 如果唯一值不太多，也统计频率
            if unique_ratio <= max_unique_ratio and unique_count <= 100:
                value_counts = col_data.value_counts().head(max_samples_per_column)
                sample_info["top_values"] = [
                    {"value": float(k) if pd.notna(k) else None, "count": int(v)}
                    for k, v in value_counts.items()
                ]
            elif unique_count <= max_samples_per_column:
                # 即使唯一值比例高，但如果总数不多，也展示所有值
                value_counts = col_data.value_counts().head(max_samples_per_column)
                sample_info["top_values"] = [
                    {"value": float(k) if pd.notna(k) else None, "count": int(v)}
                    for k, v in value_counts.items()
                ]
                sample_info["note"] = f"唯一值较多（{unique_count}个），展示所有值"
        else:
            # 非数值类型：统计频率
            sample_info["is_numeric"] = False
            
            # 如果唯一值太多，只提供统计信息
            if unique_ratio > max_unique_ratio:
                sample_info["note"] = f"唯一值较多（{unique_count}个），仅展示部分常见值"
                # 仍然展示前N个最常见的值
                value_counts = col_data.value_counts().head(max_samples_per_column)
                sample_info["top_values"] = [
                    {"value": str(k) if pd.notna(k) else "空值", "count": int(v)}
                    for k, v in value_counts.items()
                ]
            else:
                # 唯一值不太多，统计所有值的频率
                value_counts = col_data.value_counts().head(max_samples_per_column)
                sample_info["top_values"] = [
                    {"value": str(k) if pd.notna(k) else "空值", "count": int(v)}
                    for k, v in value_counts.items()
                ]
        
        column_samples[col_name] = sample_info
    
    return column_samples


def _build_column_hierarchy_tree(column_metadata: Dict[str, Dict]) -> str:
    """
    构建列层级结构的树形展示
    
    参数:
        column_metadata: 列元数据字典
    
    返回:
        格式化的树形结构字符串
    """
    if not column_metadata:
        return ""
    
    # 构建树形结构
    tree = {}
    
    for col_name, meta in column_metadata.items():
        # 获取所有层级
        levels = []
        level_keys = sorted([k for k in meta.keys() if k.startswith('level')], 
                          key=lambda x: int(x.replace('level', '')))
        for level_key in level_keys:
            value = meta.get(level_key)
            if value and str(value).strip():
                levels.append(str(value).strip())
        
        # 如果没有层级信息，使用列名本身
        if not levels:
            levels = [col_name]
        
        # 构建树
        current = tree
        for i, level_value in enumerate(levels):
            if level_value not in current:
                current[level_value] = {}
            current = current[level_value]
    
    # 递归生成树形字符串
    def _format_tree(node: Dict, prefix: str = "", is_last: bool = True, depth: int = 0) -> List[str]:
        lines = []
        items = list(node.items())
        
        for idx, (key, children) in enumerate(items):
            is_last_item = (idx == len(items) - 1)
            current_prefix = "└─ " if is_last_item else "├─ "
            
            if children:
                # 有子节点
                lines.append(f"{prefix}{current_prefix}{key}")
                next_prefix = prefix + ("   " if is_last_item else "│  ")
                child_lines = _format_tree(children, next_prefix, is_last_item, depth + 1)
                lines.extend(child_lines)
            else:
                # 叶子节点
                lines.append(f"{prefix}{current_prefix}{key}")
        
        return lines
    
    tree_lines = _format_tree(tree)
    return "\n".join(tree_lines)


def generate_analysis_prompt(
    process_result: ExcelProcessResult,
    custom_prompt: str = None,
    include_metadata: bool = True
) -> str:
    """
    根据Excel处理结果生成数据分析提示词
    
    参数:
        process_result: Excel处理结果
        custom_prompt: 自定义分析提示词
        include_metadata: 是否包含列结构元数据
    
    返回:
        格式化的提示词
    """
    if not process_result.success:
        return ""
    
    # 基础信息
    prompt_parts = []
    
    # 添加语言要求（必须在最前面）
    prompt_parts.append("**重要要求：请使用中文进行所有分析和回答，包括代码注释、分析报告等所有内容。**")
    prompt_parts.append("")
    prompt_parts.append("**禁止要求：请不要生成任何图表绘制代码，包括但不限于：**")
    prompt_parts.append("- 不要使用 matplotlib、plotly、seaborn 等绘图库")
    prompt_parts.append("- 不要使用 plt.figure()、plt.plot()、plt.savefig() 等绘图函数")
    prompt_parts.append("- 不要使用 .plot()、.hist() 等 pandas 绘图方法")
    prompt_parts.append("- 不要保存任何图片文件（.png、.jpg、.svg 等）")
    prompt_parts.append("**请专注于数据分析和统计计算，不要生成可视化代码。**")
    prompt_parts.append("")
    
    if custom_prompt:
        prompt_parts.append(custom_prompt)
    else:
        prompt_parts.append("请对上传的数据进行全面分析，生成数据分析报告。")
    
    # 添加数据文件信息（重要：告诉AI需要读取CSV文件）
    if process_result.processed_file_path:
        csv_filename = os.path.basename(process_result.processed_file_path)
        prompt_parts.append(f"\n\n## 数据文件")
        prompt_parts.append(f"**重要：工作空间中已准备好处理后的CSV数据文件，文件名为：`{csv_filename}`**")
        prompt_parts.append(f"")
        prompt_parts.append(f"**请务必使用以下代码读取数据文件进行分析：**")
        prompt_parts.append(f"```python")
        prompt_parts.append(f"import pandas as pd")
        prompt_parts.append(f"")
        prompt_parts.append(f"# 读取处理后的CSV文件")
        prompt_parts.append(f"df = pd.read_csv('{csv_filename}')")
        prompt_parts.append(f"print(f'数据形状: {{df.shape}}')")
        prompt_parts.append(f"print(f'列名: {{list(df.columns)}}')")
        prompt_parts.append(f"```")
        prompt_parts.append(f"")
        prompt_parts.append(f"**注意：**")
        prompt_parts.append(f"- CSV文件已保存在当前工作空间目录中")
        prompt_parts.append(f"- 请使用 `pd.read_csv('{csv_filename}')` 读取数据")
        prompt_parts.append(f"- 不要仅根据元数据进行分析，必须读取实际数据文件进行计算")
        prompt_parts.append(f"")
    
    # 添加数据概况
    prompt_parts.append(f"\n## 数据概况")
    prompt_parts.append(f"- 数据行数: {process_result.row_count}")
    prompt_parts.append(f"- 列数: {len(process_result.column_names)}")
    
    # 添加表头类型信息（仅保留对分析有用的信息）
    if process_result.header_analysis:
        ha = process_result.header_analysis
        if ha.header_type == 'multi':
            prompt_parts.append(f"\n## 表头结构")
            prompt_parts.append(f"- 表头类型: 多级表头（{ha.header_rows}层）")
    
    # 添加列结构元数据（帮助AI理解列之间的关系）
    if include_metadata and process_result.column_metadata:
        # 检查是否有多级结构
        has_multi_level = any(
            len(meta) > 1 
            for meta in process_result.column_metadata.values()
        )
        
        if has_multi_level:
            prompt_parts.append(f"\n## 列层级结构（多级表头语义关系）")
            prompt_parts.append("以下树形结构展示了列之间的层级分组关系，有助于理解数据的业务含义：")
            prompt_parts.append("")
            hierarchy_tree = _build_column_hierarchy_tree(process_result.column_metadata)
            if hierarchy_tree:
                prompt_parts.append(hierarchy_tree)
            else:
                # 如果树形构建失败，使用分组展示
                groups = defaultdict(list)
                for col_name, meta in process_result.column_metadata.items():
                    level1 = meta.get('level1', col_name)
                    groups[level1].append(col_name)
                
                for group, cols in groups.items():
                    if len(cols) > 1:
                        prompt_parts.append(f"- {group}: {', '.join(cols)}")
    
    # 添加完整的列名列表
    prompt_parts.append(f"\n## 完整列名列表")
    if len(process_result.column_names) <= 30:
        # 如果列数不多，全部展示
        for idx, col_name in enumerate(process_result.column_names, 1):
            prompt_parts.append(f"{idx}. {col_name}")
    else:
        # 如果列数很多，展示前20个和后10个
        for idx, col_name in enumerate(process_result.column_names[:20], 1):
            prompt_parts.append(f"{idx}. {col_name}")
        prompt_parts.append(f"... (省略中间 {len(process_result.column_names) - 30} 列) ...")
        for idx, col_name in enumerate(process_result.column_names[-10:], len(process_result.column_names) - 9):
            prompt_parts.append(f"{idx}. {col_name}")
        prompt_parts.append(f"\n(共 {len(process_result.column_names)} 列)")
    
    # 添加字段值样本信息（以JSON格式提供，更结构化）
    if include_metadata and process_result.column_metadata:
        prompt_parts.append(f"\n## 字段值样本（常见值统计）")
        prompt_parts.append("以下JSON格式展示了每个字段的常见值及其出现频率，有助于理解数据的实际内容：")
        prompt_parts.append("")
        
        # 构建包含值样本的column_metadata JSON
        column_metadata_with_samples = {}
        for col_name in process_result.column_names:
            if col_name in process_result.column_metadata:
                column_metadata_with_samples[col_name] = process_result.column_metadata[col_name]
        
        # 将column_metadata转换为格式化的JSON字符串
        prompt_parts.append("```json")
        prompt_parts.append(json.dumps(column_metadata_with_samples, ensure_ascii=False, indent=2))
        prompt_parts.append("```")
        prompt_parts.append("")
        
        prompt_parts.append("**说明：**")
        prompt_parts.append("- 每个字段的元数据包含 `value_samples` 字段，其中包含该字段的统计信息和常见值")
        prompt_parts.append("- `value_samples.top_values` 数组展示了出现频率最高的值及其出现次数")
        prompt_parts.append("- 对于数值类型字段，还包含 `min`、`max`、`mean`、`median` 等统计信息")
    
    # 在末尾再次强调要求
    prompt_parts.append("\n\n**再次提醒：请务必使用中文进行所有分析、代码注释和报告撰写，且不要生成任何图表绘制代码。**")
    
    full_prompt = '\n'.join(prompt_parts)
    
    # 打印生成的提示词
    logger.info("=" * 80)
    logger.info("📝 生成的AI分析提示词:")
    logger.info("=" * 80)
    logger.info(full_prompt)
    logger.info("=" * 80)
    
    return full_prompt

