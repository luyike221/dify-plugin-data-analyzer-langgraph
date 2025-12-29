"""
Dify Plugin Tool Implementation for Excel Data Analyzer
Integrates core analysis functionality into Dify plugin tool interface
"""
import os
import asyncio
from collections.abc import Generator
from typing import Any, Optional
from pathlib import Path

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

# Import core functionality
from core.excel_analyze_api import analyze_excel
from core.config import DEFAULT_EXCEL_ANALYSIS_PROMPT


class DifyPluginDataAnalyzerTool(Tool):
    """Excel智能分析工具"""
    
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
        执行Excel数据分析
        
        参数:
        - input_file: Excel文件（必填）
        - query: 可选的分析查询语句或提示词
        """
        input_file = tool_parameters.get("input_file")
        query = tool_parameters.get("query", "")
        
        if not input_file:
            yield self.create_text_message("错误: 缺少文件参数，请上传Excel文件")
            return
        
        try:
            # 处理文件参数
            # Dify 插件中的文件参数可能是文件路径字符串或文件对象
            file_content = None
            filename = None
            
            if isinstance(input_file, str):
                # 如果是文件路径字符串
                if os.path.exists(input_file):
                    with open(input_file, "rb") as f:
                        file_content = f.read()
                    filename = os.path.basename(input_file)
                else:
                    yield self.create_text_message(f"错误: 文件不存在: {input_file}")
                    return
            elif hasattr(input_file, "read"):
                # 如果是文件对象
                file_content = input_file.read()
                filename = getattr(input_file, "filename", "uploaded_file.xlsx")
                if hasattr(input_file, "name"):
                    filename = os.path.basename(input_file.name)
            elif isinstance(input_file, dict):
                # 如果是字典，可能包含文件路径或内容
                if "path" in input_file:
                    file_path = input_file["path"]
                    if os.path.exists(file_path):
                        with open(file_path, "rb") as f:
                            file_content = f.read()
                        filename = os.path.basename(file_path)
                    else:
                        yield self.create_text_message(f"错误: 文件不存在: {file_path}")
                        return
                elif "content" in input_file:
                    file_content = input_file["content"]
                    filename = input_file.get("filename", "uploaded_file.xlsx")
                else:
                    yield self.create_text_message("错误: 无法从文件参数中提取文件内容")
                    return
            else:
                yield self.create_text_message(f"错误: 不支持的文件参数类型: {type(input_file)}")
                return
            
            if not file_content:
                yield self.create_text_message("错误: 无法读取文件内容")
                return
            
            if not filename:
                filename = "uploaded_file.xlsx"
            
            # 使用自定义查询或默认提示词
            analysis_prompt = query if query else DEFAULT_EXCEL_ANALYSIS_PROMPT
            
            # 调用分析函数（异步函数需要运行在事件循环中）
            try:
                # 检查是否已有事件循环
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # 如果没有事件循环，创建一个新的
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # 运行异步分析函数
            result = loop.run_until_complete(
                analyze_excel(
                    file_content=file_content,
                    filename=filename,
                    thread_id=None,  # 创建新会话
                    use_llm_validate=False,  # 默认不使用LLM验证表头
                    sheet_name=None,  # 使用默认工作表
                    auto_analysis=True,  # 自动分析
                    analysis_prompt=analysis_prompt,
                    stream=False,  # 不支持流式
                    model="DeepAnalyze-8B",
                    temperature=0.4
                )
            )
            
            # 格式化返回结果
            if result.get("status") == "error":
                yield self.create_text_message(f"分析失败: {result.get('error_message', '未知错误')}")
                return
            
            # 构建成功响应
            response_text = f"✅ Excel文件分析完成\n\n"
            response_text += f"📊 **文件信息**\n"
            response_text += f"- 文件名: {filename}\n"
            response_text += f"- 会话ID: {result.get('thread_id', 'N/A')}\n\n"
            
            # 表头分析结果
            if result.get("header_analysis"):
                ha = result["header_analysis"]
                response_text += f"📋 **表头分析**\n"
                response_text += f"- 表头类型: {ha.get('header_type', 'N/A')}\n"
                response_text += f"- 表头行数: {ha.get('header_rows', 'N/A')}\n"
                response_text += f"- 数据起始行: {ha.get('data_start_row', 'N/A')}\n"
                response_text += f"- 置信度: {ha.get('confidence', 'N/A')}\n\n"
            
            # 数据摘要
            if result.get("data_summary"):
                ds = result["data_summary"]
                response_text += f"📈 **数据摘要**\n"
                response_text += f"- 行数: {ds.get('row_count', 'N/A')}\n"
                response_text += f"- 列数: {ds.get('column_count', 'N/A')}\n"
                if ds.get("column_names"):
                    response_text += f"- 列名: {', '.join(ds['column_names'][:5])}"
                    if len(ds["column_names"]) > 5:
                        response_text += f" ... (共{len(ds['column_names'])}列)"
                    response_text += "\n\n"
            
            # 分析结果
            if result.get("analysis_result"):
                ar = result["analysis_result"]
                if ar.get("reasoning"):
                    response_text += f"🤖 **分析结果**\n{ar['reasoning']}\n\n"
                if ar.get("generated_files"):
                    response_text += f"📁 **生成的文件**\n"
                    for file_info in ar["generated_files"]:
                        response_text += f"- {file_info.get('name', 'N/A')}\n"
            
            # 处理后的文件信息
            if result.get("processed_file"):
                pf = result["processed_file"]
                response_text += f"\n💾 **处理后的文件**\n"
                response_text += f"- 文件名: {pf.get('filename', 'N/A')}\n"
                response_text += f"- 文件路径: {pf.get('file_path', 'N/A')}\n"
            
            yield self.create_text_message(response_text)
            
        except Exception as e:
            import traceback
            error_msg = f"错误: {str(e)}\n{traceback.format_exc()}"
            yield self.create_text_message(error_msg)
