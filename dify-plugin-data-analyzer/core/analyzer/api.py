"""
LangGraph Analyzer API

提供与现有 excel_analyze_api.py 兼容的 API 接口
"""

import os
import logging
from typing import Dict, Any, List, Optional, Generator

from .graph import DataAnalysisGraph
from .state import AnalysisResult

logger = logging.getLogger(__name__)


def get_data_preview(csv_path: str, max_rows: int = 5) -> str:
    """
    获取 CSV 文件的数据预览
    
    Args:
        csv_path: CSV 文件路径
        max_rows: 最大预览行数
        
    Returns:
        数据预览字符串
    """
    try:
        import pandas as pd
        df = pd.read_csv(csv_path, nrows=max_rows)
        return df.to_string(index=False)
    except Exception as e:
        logger.warning(f"无法读取数据预览: {e}")
        return "（无法读取数据预览）"


def run_langgraph_analysis(
    workspace_dir: str,
    thread_id: str,
    csv_path: str,
    column_names: List[str],
    column_metadata: Dict[str, Any],
    row_count: int,
    user_prompt: str,
    api_url: str,
    model: str,
    api_key: Optional[str] = None,
    temperature: float = 0.4,
) -> Dict[str, Any]:
    """
    使用 LangGraph 执行数据分析（非流式）
    
    Args:
        workspace_dir: 工作空间目录
        thread_id: 会话ID
        csv_path: CSV 文件路径
        column_names: 列名列表
        column_metadata: 列元数据
        row_count: 数据行数
        user_prompt: 用户分析需求
        api_url: LLM API 地址
        model: 模型名称
        api_key: LLM API 密钥
        temperature: 生成温度
        
    Returns:
        包含分析结果的字典
    """
    # 获取数据预览
    data_preview = get_data_preview(csv_path)
    
    # 创建分析图
    graph = DataAnalysisGraph()
    
    # 执行分析
    result = graph.analyze(
        workspace_dir=workspace_dir,
        thread_id=thread_id,
        csv_path=csv_path,
        column_names=column_names,
        column_metadata=column_metadata,
        row_count=row_count,
        data_preview=data_preview,
        user_prompt=user_prompt,
        api_url=api_url,
        model=model,
        api_key=api_key,
        temperature=temperature,
    )
    
    return {
        "success": result.success,
        "report": result.report,
        "reasoning": "\n\n".join([
            f"代码 {i+1}:\n{code}" 
            for i, code in enumerate(result.code_history)
        ]),
        "generated_files": result.generated_files,
        "error_message": result.error_message,
    }


def run_langgraph_analysis_stream(
    workspace_dir: str,
    thread_id: str,
    csv_path: str,
    column_names: List[str],
    column_metadata: Dict[str, Any],
    row_count: int,
    user_prompt: str,
    api_url: str,
    model: str,
    api_key: Optional[str] = None,
    temperature: float = 0.4,
) -> Generator[str, None, None]:
    """
    使用 LangGraph 执行数据分析（流式）
    
    Args:
        workspace_dir: 工作空间目录
        thread_id: 会话ID
        csv_path: CSV 文件路径
        column_names: 列名列表
        column_metadata: 列元数据
        row_count: 数据行数
        user_prompt: 用户分析需求
        api_url: LLM API 地址
        model: 模型名称
        api_key: LLM API 密钥
        temperature: 生成温度
        
    Yields:
        流式输出的字符串块
    """
    # 获取数据预览
    data_preview = get_data_preview(csv_path)
    
    # 创建分析图
    graph = DataAnalysisGraph()
    
    # 流式执行分析
    yield "🚀 **开始 LangGraph 数据分析工作流**\n\n"
    
    try:
        for chunk in graph.analyze_stream(
            workspace_dir=workspace_dir,
            thread_id=thread_id,
            csv_path=csv_path,
            column_names=column_names,
            column_metadata=column_metadata,
            row_count=row_count,
            data_preview=data_preview,
            user_prompt=user_prompt,
            api_url=api_url,
            model=model,
            api_key=api_key,
            temperature=temperature,
        ):
            yield chunk
        
        yield "\n\n✅ **分析完成**\n"
        
    except Exception as e:
        import traceback
        error_msg = f"\n\n❌ **分析过程出错**\n\n```\n{str(e)}\n{traceback.format_exc()}\n```\n"
        yield error_msg


def analyze_excel_with_langgraph(
    file_content: bytes,
    filename: str,
    analysis_api_url: str,
    analysis_model: str,
    thread_id: Optional[str] = None,
    use_llm_validate: bool = False,
    sheet_name: Optional[str] = None,
    analysis_prompt: Optional[str] = None,
    temperature: float = 0.4,
    llm_api_key: Optional[str] = None,
    llm_base_url: Optional[str] = None,
    llm_model: Optional[str] = None,
    analysis_api_key: Optional[str] = None,
) -> Generator[str, None, None]:
    """
    使用 LangGraph 分析 Excel 文件（流式版本）
    
    这是与 analyze_excel_stream 兼容的接口，
    可以直接替换现有的分析函数
    
    Args:
        file_content: Excel 文件内容
        filename: 文件名
        analysis_api_url: 分析 API 地址
        analysis_model: 分析模型名称
        thread_id: 会话ID
        use_llm_validate: 是否使用 LLM 验证表头
        sheet_name: 工作表名称
        analysis_prompt: 分析提示词
        temperature: 生成温度
        llm_api_key: LLM API 密钥
        llm_base_url: LLM API 地址
        llm_model: LLM 模型名称
        analysis_api_key: 分析 API 密钥
        
    Yields:
        流式输出的字符串块
    """
    # 导入必要的模块
    from ..excel_processor import process_excel_file, get_sheet_names
    from ..storage import storage
    from ..utils import get_thread_workspace
    from ..config import DEFAULT_EXCEL_ANALYSIS_PROMPT, EXCEL_LLM_API_KEY
    
    import time
    import uuid
    
    file_size = len(file_content)
    
    # 文件验证
    from pathlib import Path
    from ..config import EXCEL_VALID_EXTENSIONS, EXCEL_MAX_FILE_SIZE_MB
    
    ext = Path(filename).suffix.lower()
    if ext not in EXCEL_VALID_EXTENSIONS:
        yield f"❌ 不支持的文件格式: {ext}\n"
        return
    
    max_size_bytes = EXCEL_MAX_FILE_SIZE_MB * 1024 * 1024
    if file_size > max_size_bytes:
        yield f"❌ 文件过大: {file_size / 1024 / 1024:.2f}MB\n"
        return
    
    # 创建或获取会话
    if thread_id:
        current_thread_id = thread_id
    else:
        current_thread_id = f"thread-{uuid.uuid4().hex[:24]}"
    
    workspace_dir = get_thread_workspace(current_thread_id)
    os.makedirs(workspace_dir, exist_ok=True)
    
    try:
        # 保存文件
        excel_path = os.path.join(workspace_dir, filename)
        with open(excel_path, "wb") as f:
            f.write(file_content)
        
        yield f"📁 文件已保存: {filename}\n\n"
        
        # 获取工作表
        available_sheets = get_sheet_names(excel_path)
        if available_sheets:
            yield f"📋 可用工作表: {', '.join(available_sheets)}\n"
        
        # 处理表头
        api_key = llm_api_key if llm_api_key else EXCEL_LLM_API_KEY
        actual_use_llm = use_llm_validate and bool(api_key)
        
        yield "🔍 正在分析表头结构...\n"
        
        process_result = process_excel_file(
            filepath=excel_path,
            output_dir=workspace_dir,
            sheet_name=sheet_name,
            use_llm_validate=actual_use_llm,
            llm_api_key=llm_api_key,
            llm_base_url=llm_base_url,
            llm_model=llm_model,
        )
        
        if not process_result.success:
            yield f"❌ Excel 处理失败: {process_result.error_message}\n"
            return
        
        yield f"✅ 表头分析完成，数据行数: {process_result.row_count}\n\n"
        
        # 使用 LangGraph 执行分析
        prompt = analysis_prompt or DEFAULT_EXCEL_ANALYSIS_PROMPT
        
        yield "🧠 **开始 AI 数据分析**\n\n"
        
        for chunk in run_langgraph_analysis_stream(
            workspace_dir=workspace_dir,
            thread_id=current_thread_id,
            csv_path=process_result.processed_file_path,
            column_names=process_result.column_names,
            column_metadata=process_result.column_metadata,
            row_count=process_result.row_count,
            user_prompt=prompt,
            api_url=analysis_api_url,
            model=analysis_model,
            api_key=analysis_api_key,
            temperature=temperature,
        ):
            yield chunk
        
    except Exception as e:
        import traceback
        yield f"\n❌ 处理过程出错: {str(e)}\n{traceback.format_exc()}\n"

