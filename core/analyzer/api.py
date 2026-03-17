"""
LangGraph Analyzer API

提供与现有 excel_analyze_api.py 兼容的 API 接口
"""

import os
import sys
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


def analyze_excel_files_with_langgraph(
    files_data: List[Dict[str, Any]],  # 每个元素包含 file_content, filename
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
    preprocessing_timeout: Optional[int] = None,
    analysis_timeout: Optional[int] = None,
    debug_print_execution_output: bool = False,
    debug_print_header_analysis: bool = False,
    max_analysis_rounds: int = 3,
    max_file_size_mb: Optional[int] = None,
    excel_processing_timeout: Optional[int] = None,
    enable_deep_analysis: bool = True,
    max_rows: Optional[int] = None,
) -> Generator[str, None, None]:
    """
    使用 LangGraph 分析 Excel 文件（流式版本）
    
    统一处理单文件和多文件场景，不再区分。
    
    流程：
    1. 预处理所有文件（表头分析+CSV转换）
    2. 收集所有文件的元数据
    3. 在策略制定时让LLM选择文件（单文件时自动选择）
    4. 执行分析
    
    Args:
        files_data: 文件数据列表，每个元素包含：
            - file_content: 文件内容（bytes）
            - filename: 文件名
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
        preprocessing_timeout: 预处理超时时间（秒）
        analysis_timeout: 分析超时时间（秒）
        debug_print_execution_output: 是否打印代码执行输出（用于调试）
        debug_print_header_analysis: 是否打印表头分析结果（用于调试）
        max_analysis_rounds: 最大分析轮数（默认3轮），防止无限循环
        max_file_size_mb: 最大文件大小（MB），如果为None则使用默认值
        excel_processing_timeout: Excel处理超时时间（秒），在LLM分析之前
        
    Yields:
        流式输出的字符串块
    """
    from ..storage import storage
    from ..utils import get_thread_workspace
    from ..config import DEFAULT_EXCEL_ANALYSIS_PROMPT, EXCEL_LLM_API_KEY
    from ..excel_processor import process_excel_file
    from ..excel_analyze_api import validate_excel_file
    
    import time
    import uuid
    
    total_files = len(files_data)
    
    # 创建或获取会话
    if thread_id:
        current_thread_id = thread_id
    else:
        current_thread_id = f"thread-{uuid.uuid4().hex[:24]}"
    
    workspace_dir = get_thread_workspace(current_thread_id)
    os.makedirs(workspace_dir, exist_ok=True)
    
    try:
        yield f"🚀 **开始处理 {total_files} 个Excel文件...**\n\n"
        
        # === 第一阶段：预处理所有文件 ===
        yield "📋 **第一阶段：预处理所有文件（表头分析和CSV转换）**\n\n"
        
        processed_files_info = []
        
        for file_index, file_data in enumerate(files_data, 1):
            file_content = file_data.get("file_content")
            filename = file_data.get("filename", f"file_{file_index}.xlsx")
            
            yield f"📄 **处理文件 {file_index}/{total_files}: {filename}**\n"
            
            # 文件验证
            file_size = len(file_content)
            try:
                validate_excel_file(filename, file_size, max_file_size_mb=max_file_size_mb)
            except ValueError as e:
                yield f"❌ 该文件未通过验证（{str(e)}），已跳过。\n\n"
                continue
            
            # 保存文件
            excel_path = os.path.join(workspace_dir, filename)
            with open(excel_path, "wb") as f:
                f.write(file_content)
            
            yield f"📁 文件已保存: {filename}\n"
            
            # 处理表头
            api_key = llm_api_key if llm_api_key else EXCEL_LLM_API_KEY
            actual_use_llm = use_llm_validate and bool(api_key)
            
            yield "🔍 正在分析表头结构...\n"
            
            # 处理Excel文件
            process_result = process_excel_file(
                filepath=excel_path,
                output_dir=workspace_dir,
                sheet_name=sheet_name,
                use_llm_validate=actual_use_llm,
                llm_api_key=llm_api_key,
                llm_base_url=llm_base_url,
                llm_model=llm_model,
                preprocessing_timeout=preprocessing_timeout,
                excel_processing_timeout=excel_processing_timeout,
                debug_print_header_analysis=debug_print_header_analysis,
                thinking_callback=None,
                max_file_size_mb=max_file_size_mb,
                # 将最大行数限制传递给底层处理器，如果为 None 则在处理器中使用默认值
                max_rows=max_rows,
            )
            
            if not process_result.success:
                yield f"❌ 该文件处理失败，已跳过。请检查格式是否为支持的 Excel（.xlsx/.xls 等）。\n\n"
                continue
            
            # 获取数据预览
            data_preview = ""
            if process_result.processed_file_path:
                data_preview = get_data_preview(process_result.processed_file_path, max_rows=5)
            
            # 收集文件信息
            processed_files_info.append({
                "filename": filename,
                "csv_path": process_result.processed_file_path,
                "row_count": process_result.row_count,
                "column_names": process_result.column_names,
                "column_metadata": process_result.column_metadata,
                "data_preview": data_preview,
            })
            
            yield f"✅ 文件 {file_index} 预处理完成（数据行数: {process_result.row_count}）\n\n"
        
        if not processed_files_info:
            yield "❌ 暂无成功处理的文件，无法继续分析。请检查文件格式或重新上传后重试。\n"
            return
        
        yield f"✅ **所有文件预处理完成**（共 {len(processed_files_info)} 个文件）\n\n"
        
        # === 第二阶段：使用 LangGraph 执行分析 ===
        yield "🧠 **第二阶段：AI 数据分析**\n\n"
        
        prompt = analysis_prompt or DEFAULT_EXCEL_ANALYSIS_PROMPT
        
        # 使用第一个文件作为主文件（用于初始化状态）
        first_file = processed_files_info[0]
        
        # 调用统一的分析函数
        for chunk in run_langgraph_analysis_stream(
            workspace_dir=workspace_dir,
            thread_id=current_thread_id,
            csv_path=first_file["csv_path"],  # 主文件路径
            column_names=first_file["column_names"],
            column_metadata=first_file["column_metadata"],
            row_count=first_file["row_count"],
            data_preview=first_file["data_preview"],
            user_prompt=prompt,
            api_url=analysis_api_url,
            model=analysis_model,
            api_key=analysis_api_key,
            temperature=temperature,
            analysis_timeout=analysis_timeout,
            debug_print_execution_output=debug_print_execution_output,
            max_analysis_rounds=max_analysis_rounds,
            available_files=processed_files_info,  # 传递所有文件信息
            enable_deep_analysis=enable_deep_analysis,
        ):
            yield chunk
        
    except Exception as e:
        logger.error("处理过程出错: %s", e, exc_info=True)
        yield "\n❌ 处理过程出错，请稍后重试或联系管理员。\n"


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
    data_preview: str,
    user_prompt: str,
    api_url: str,
    model: str,
    api_key: Optional[str] = None,
    temperature: float = 0.4,
    analysis_timeout: Optional[int] = None,
    debug_print_execution_output: bool = False,
    max_analysis_rounds: int = 3,
    available_files: Optional[List[Dict[str, Any]]] = None,
    enable_deep_analysis: bool = True,
) -> Generator[str, None, None]:
    """
    使用 LangGraph 执行数据分析（流式输出）
    委托给 DataAnalysisGraph.analyze_stream，供多文件与单文件分析统一调用。
    """
    graph = DataAnalysisGraph()
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
        analysis_timeout=analysis_timeout,
        debug_print_execution_output=debug_print_execution_output,
        max_analysis_rounds=max_analysis_rounds,
        available_files=available_files,
        enable_deep_analysis=enable_deep_analysis,
    ):
        yield chunk


def run_langgraph_analysis_stream_legacy(
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
    analysis_timeout: Optional[int] = None,
    debug_print_execution_output: bool = False,
    max_analysis_rounds: int = 3,
) -> Generator[str, None, None]:
    """
    使用 LangGraph 执行数据分析（流式，兼容旧接口）
    
    此函数用于兼容旧的单文件接口，内部转换为统一的多文件格式。
    """
    # 获取数据预览
    data_preview = get_data_preview(csv_path)
    
    # 构建单文件信息列表（统一格式）
    available_files = [{
        "filename": os.path.basename(csv_path),
        "csv_path": csv_path,
        "row_count": row_count,
        "column_names": column_names,
        "column_metadata": column_metadata,
        "data_preview": data_preview,
    }]
    
    # 调用统一的分析函数
    for chunk in run_langgraph_analysis_stream(
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
        analysis_timeout=analysis_timeout,
        debug_print_execution_output=debug_print_execution_output,
        max_analysis_rounds=max_analysis_rounds,
        available_files=available_files,
    ):
        yield chunk


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
    preprocessing_timeout: Optional[int] = None,
    analysis_timeout: Optional[int] = None,
    debug_print_execution_output: bool = False,
    debug_print_header_analysis: bool = False,
    max_analysis_rounds: int = 3,
    max_file_size_mb: Optional[int] = None,
    excel_processing_timeout: Optional[int] = None,
    max_rows: Optional[int] = None,
) -> Generator[str, None, None]:
    """
    使用 LangGraph 分析 Excel 文件（流式版本，兼容旧接口）
    
    此函数用于兼容旧的单文件接口，内部转换为统一的多文件格式。
    
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
        preprocessing_timeout: 预处理超时时间（秒）
        analysis_timeout: 分析超时时间（秒）
        debug_print_execution_output: 是否打印代码执行输出（用于调试）
        debug_print_header_analysis: 是否打印表头分析结果（用于调试）
        max_analysis_rounds: 最大分析轮数（默认3轮），防止无限循环
        
    Yields:
        流式输出的字符串块
    """
    # 转换为统一的多文件格式
    files_data = [{
        "file_content": file_content,
        "filename": filename,
    }]
    
    # 调用统一的分析函数
    for chunk in analyze_excel_files_with_langgraph(
        files_data=files_data,
        analysis_api_url=analysis_api_url,
        analysis_model=analysis_model,
        thread_id=thread_id,
        use_llm_validate=use_llm_validate,
        sheet_name=sheet_name,
        analysis_prompt=analysis_prompt,
        temperature=temperature,
        llm_api_key=llm_api_key,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        analysis_api_key=analysis_api_key,
        preprocessing_timeout=preprocessing_timeout,
        analysis_timeout=analysis_timeout,
        debug_print_execution_output=debug_print_execution_output,
        debug_print_header_analysis=debug_print_header_analysis,
        max_analysis_rounds=max_analysis_rounds,
        max_file_size_mb=max_file_size_mb,
        excel_processing_timeout=excel_processing_timeout,
        max_rows=max_rows,
    ):
        yield chunk
    # 导入必要的模块
    from ..storage import storage
    from ..utils import get_thread_workspace
    from ..config import DEFAULT_EXCEL_ANALYSIS_PROMPT, EXCEL_LLM_API_KEY
    
    import time
    import uuid
    
    file_size = len(file_content)
    
    # 文件验证
    from pathlib import Path
    from ..config import EXCEL_VALID_EXTENSIONS, EXCEL_MAX_FILE_SIZE_MB
    from ..excel_analyze_api import validate_excel_file
    
    try:
        validate_excel_file(filename, file_size, max_file_size_mb=max_file_size_mb)
    except ValueError as e:
        yield f"❌ 该文件未通过验证（{str(e)}）。\n"
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
        
        # 打印最初传入的Excel原始数据
        logger.info(f"📊 [DEBUG] [LangGraph] 准备打印Excel原始数据: {excel_path}")
        print("🔍 [DEBUG] [LangGraph] 调用 print_excel_raw_data 前（使用print输出）")
        import sys
        sys.stdout.flush()
        from ..excel_processor import print_excel_raw_data
        try:
            print_excel_raw_data(excel_path, sheet_name=sheet_name)
            print("🔍 [DEBUG] [LangGraph] print_excel_raw_data 函数已返回（使用print输出）")
            sys.stdout.flush()
        except Exception as e:
            print(f"❌ [DEBUG] [LangGraph] print_excel_raw_data 调用异常: {e}（使用print输出）")
            sys.stdout.flush()
            raise
        logger.info(f"✅ [DEBUG] [LangGraph] print_excel_raw_data 函数已返回")
        
        # 处理表头
        api_key = llm_api_key if llm_api_key else EXCEL_LLM_API_KEY
        actual_use_llm = use_llm_validate and bool(api_key)
        
        yield "🔍 正在分析表头结构...\n"
        
        import threading
        
        # 导入必要的模块
        from ..excel_processor import process_excel_file
        
        # 处理Excel文件
        process_result = process_excel_file(
            filepath=excel_path,
            output_dir=workspace_dir,
            sheet_name=sheet_name,
            use_llm_validate=actual_use_llm,
            llm_api_key=llm_api_key,
            llm_base_url=llm_base_url,
            llm_model=llm_model,
            preprocessing_timeout=preprocessing_timeout,
            excel_processing_timeout=excel_processing_timeout,
            debug_print_header_analysis=debug_print_header_analysis,
            thinking_callback=None,  # 不输出 thinking 内容
            max_file_size_mb=max_file_size_mb  # 传递文件大小限制
        )
        
        if not process_result.success:
            yield f"❌ 该文件处理失败。请检查格式是否为支持的 Excel（.xlsx/.xls 等）。\n"
            return
        
        yield f"✅ 表头分析完成，数据行数: {process_result.row_count}\n\n"
        
        # 根据调试开关决定是否输出LLM分析响应
        if debug_print_header_analysis and process_result.llm_analysis_response:
            yield "\n📋 **LLM表头分析原始响应（调试信息）：**\n\n"
            yield "```json\n"
            yield process_result.llm_analysis_response
            yield "\n```\n\n"
        
        # 使用 LangGraph 执行分析
        prompt = analysis_prompt or DEFAULT_EXCEL_ANALYSIS_PROMPT
        
        yield "🧠 **开始 AI 数据分析**\n\n"
        
        # 构建单文件信息列表（统一格式）
        available_files = [{
            "filename": filename,
            "csv_path": process_result.processed_file_path,
            "row_count": process_result.row_count,
            "column_names": process_result.column_names,
            "column_metadata": process_result.column_metadata,
            "data_preview": get_data_preview(process_result.processed_file_path, max_rows=5),
        }]
        
        for chunk in run_langgraph_analysis_stream(
            workspace_dir=workspace_dir,
            thread_id=current_thread_id,
            csv_path=process_result.processed_file_path,
            column_names=process_result.column_names,
            column_metadata=process_result.column_metadata,
            row_count=process_result.row_count,
            data_preview=get_data_preview(process_result.processed_file_path),
            user_prompt=prompt,
            api_url=analysis_api_url,
            model=analysis_model,
            api_key=analysis_api_key,
            temperature=temperature,
            analysis_timeout=analysis_timeout,
            debug_print_execution_output=debug_print_execution_output,
            max_analysis_rounds=max_analysis_rounds,
            available_files=available_files,
        ):
            yield chunk
        
    except Exception as e:
        logger.error("处理过程出错: %s", e, exc_info=True)
        yield "\n❌ 处理过程出错，请稍后重试或联系管理员。\n"

