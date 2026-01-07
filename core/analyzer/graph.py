"""
LangGraph Data Analysis Workflow

基于 LangGraph 1.0.0+ 实现的数据分析工作流图
支持：代码生成 → 执行 → 错误修复 → 报告生成
"""

import re
import os
import shutil
import logging
import threading
import queue
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Generator, Literal

from langgraph.graph import StateGraph, START, END

from .state import AnalysisState, AnalysisPhase, CodeExecution, create_initial_state, AnalysisResult
from .prompts import PromptTemplates

# 配置日志
logger = logging.getLogger(__name__)

# ============================================================================
# 请求级别的队列管理（解决多线程并发问题）
# ============================================================================
# 使用字典存储每个请求的独立队列，避免全局队列被多个请求共享导致的竞态条件
_request_queues: Dict[str, queue.Queue] = {}
_queues_lock = threading.Lock()


def _create_request_queue(request_id: str) -> queue.Queue:
    """为请求创建独立的队列"""
    with _queues_lock:
        q = queue.Queue(maxsize=1000)
        _request_queues[request_id] = q
        logger.debug(f"🔧 创建请求队列: {request_id}")
        return q


def _get_request_queue(request_id: str) -> Optional[queue.Queue]:
    """获取请求的队列"""
    with _queues_lock:
        return _request_queues.get(request_id)


def _remove_request_queue(request_id: str):
    """移除请求的队列"""
    with _queues_lock:
        if request_id in _request_queues:
            # 清空队列
            q = _request_queues[request_id]
            while not q.empty():
                try:
                    q.get_nowait()
                except queue.Empty:
                    break
            del _request_queues[request_id]
            logger.debug(f"🧹 移除请求队列: {request_id}")


def _push_to_request_queue(request_id: str, chunk: Optional[str]):
    """推送到指定请求的队列（chunk 为 None 表示结束标记）"""
    q = _get_request_queue(request_id)
    if q is not None:
        try:
            q.put(chunk, timeout=0.1)
        except queue.Full:
            # 队列已满，跳过（避免阻塞）
            if chunk is not None:
                logger.warning(f"⚠️ 请求 {request_id} 的队列已满，跳过 chunk")
            pass


# ============================================================================
# LLM 客户端辅助函数
# ============================================================================

def extract_api_base(api_url: str) -> str:
    """从完整的API URL中提取base URL"""
    if api_url.endswith("/chat/completions"):
        return api_url.rsplit("/chat/completions", 1)[0]
    elif "/v1" in api_url:
        return api_url.rsplit("/v1", 1)[0] + "/v1"
    else:
        return api_url


def create_llm_client(api_url: str, api_key: Optional[str] = None):
    """创建 OpenAI 兼容的 LLM 客户端"""
    import openai
    
    api_base = extract_api_base(api_url)
    return openai.OpenAI(
        base_url=api_base,
        api_key=api_key or "dummy",
        timeout=120.0,
    )


def call_llm(
    client,
    messages: List[Dict[str, str]],
    model: str,
    temperature: float = 0.4,
    stream: bool = False,
    stream_callback: Optional[callable] = None,
    push_to_queue: bool = True,
    request_id: Optional[str] = None,  # 新增：请求ID，用于定位独立队列
) -> str:
    """
    调用 LLM 并返回响应内容
    
    Args:
        client: LLM 客户端
        messages: 消息列表
        model: 模型名称
        temperature: 生成温度
        stream: 是否流式输出
        stream_callback: 流式输出回调函数，接收每个 token (chunk: str) -> None
        push_to_queue: 是否推送到流式输出队列（默认True）
        request_id: 请求ID，用于定位该请求的独立队列（多线程安全）
    
    Returns:
        完整的响应内容
    """
    if stream:
        # 流式调用，实时回调
        # 流式调用时启用 thinking 功能，使用流式调用避免阻塞
        # 优先尝试 extra_body 方式（兼容更多 API）
        try:
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                stream=True,  # 必须使用流式调用
                extra_body={"enable_thinking": True},  # 流式调用时启用 thinking
            )
        except Exception:
            # 如果 extra_body 方式失败，尝试直接传递参数
            try:
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=temperature,
                    stream=True,  # 必须使用流式调用
                    enable_thinking=True,  # 尝试直接传递参数
                )
            except Exception:
                # 如果启用 thinking 失败，仍然使用流式调用（不启用 thinking）
                # 这样可以避免阻塞，保证系统正常运行
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=temperature,
                    stream=True,  # 必须保持流式调用
                )
        
        full_content = ""
        for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content:
                delta = chunk.choices[0].delta.content
                full_content += delta
                
                # 实时回调（如果提供）
                if stream_callback:
                    stream_callback(delta)
                
                # 推送到请求独立的队列（如果启用且提供了 request_id）
                if push_to_queue and request_id:
                    _push_to_request_queue(request_id, delta)
        
        return full_content
    else:
        # 非流式调用已被禁用，因为必须启用 thinking 功能
        # thinking 功能只能在流式调用时启用，非流式调用不支持
        # 强制使用流式调用以确保 thinking 功能可用
        raise ValueError(
            "非流式调用已被禁用。必须使用流式调用（stream=True）以启用 thinking 功能。"
            "请确保所有 call_llm 调用都使用 stream=True 参数。"
        )


def call_llm_stream(
    client,
    messages: List[Dict[str, str]],
    model: str,
    temperature: float = 0.4,
) -> Generator[str, None, str]:
    """流式调用 LLM，yield 每个 token，最后返回完整内容"""
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        stream=True,
    )
    
    full_content = ""
    for chunk in response:
        if chunk.choices and chunk.choices[0].delta.content:
            delta = chunk.choices[0].delta.content
            full_content += delta
            yield delta
    
    return full_content


# ============================================================================
# 代码提取辅助函数
# ============================================================================

def extract_python_code(text: str) -> Optional[str]:
    """从 LLM 响应中提取 Python 代码块"""
    # 匹配 ```python ... ``` 格式
    pattern = r"```python\s*(.*?)```"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    # 匹配 ``` ... ``` 格式（无语言标记）
    pattern2 = r"```\s*(.*?)```"
    match2 = re.search(pattern2, text, re.DOTALL)
    if match2:
        code = match2.group(1).strip()
        # 简单判断是否像 Python 代码
        if "import " in code or "print(" in code or "def " in code:
            return code
    
    return None


def has_python_code(text: str) -> bool:
    """检查文本中是否包含 Python 代码块"""
    return extract_python_code(text) is not None


def is_execution_error(output: str) -> bool:
    """检查执行输出是否包含错误"""
    error_indicators = [
        "[Error]",
        "[Timeout]",
        "Traceback (most recent call last)",
        "Error:",
        "Exception:",
        "SyntaxError:",
        "NameError:",
        "TypeError:",
        "ValueError:",
        "KeyError:",
        "IndexError:",
        "FileNotFoundError:",
        "ModuleNotFoundError:",
    ]
    return any(indicator in output for indicator in error_indicators)


# ============================================================================
# 工作流节点函数
# ============================================================================

def analyze_intent_node(state: AnalysisState) -> Dict[str, Any]:
    """
    意图分析和策略制定节点
    
    功能：
    1. 判断用户输入与数据的相关性
    2. 如果无关，返回澄清消息
    3. 如果相关，重写用户需求并制定分析策略
    """
    logger.info("🔍 [Node] 意图分析节点开始执行")
    
    # 获取请求ID（用于多线程隔离）
    request_id = state.get("request_id", "")
    
    # 创建 LLM 客户端
    client = create_llm_client(state["api_url"], state.get("api_key"))
    
    # 构建意图分析 prompt
    messages = PromptTemplates.format_intent_analysis_prompt(
        csv_path=state["csv_path"],
        row_count=state["row_count"],
        column_names=state["column_names"],
        column_metadata=state["column_metadata"],
        data_preview=state["data_preview"],
        user_prompt=state["user_prompt"],
    )
    
    # 收集流式输出的列表（用于后续格式化）
    stream_chunks = []
    
    def stream_callback(chunk: str):
        """流式输出回调，只收集 token，不推送到队列（避免输出JSON）"""
        stream_chunks.append(chunk)
        # 注意：不推送到队列，避免直接输出JSON内容
    
    # 流式调用 LLM，收集输出但不实时推送（避免输出JSON）
    # 使用流式调用以支持 think 功能，但不直接输出内容
    response = call_llm(
        client=client,
        messages=messages,
        model=state["model"],
        temperature=state["temperature"],
        stream=True,
        stream_callback=stream_callback,
        push_to_queue=False,  # 不推送到队列，避免输出JSON
        request_id=request_id,
    )
    
    # 在控制台打印LLM的完整响应
    logger.info("=" * 80)
    logger.info("🔍 [意图分析] LLM 完整响应:")
    logger.info("=" * 80)
    logger.info(response)
    logger.info("=" * 80)
    
    # 解析 JSON 响应
    import json
    try:
        # 尝试提取 JSON（可能包含 markdown 代码块）
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            # 尝试直接解析整个响应
            json_str = response
        
        intent_result = json.loads(json_str.strip())
    except (json.JSONDecodeError, AttributeError) as e:
        logger.warning(f"⚠️ [Node] 无法解析意图分析结果: {e}")
        # 如果解析失败，默认继续分析
        intent_result = {
            "is_relevant": True,
            "needs_clarification": False,
            "refined_prompt": state["user_prompt"],
            "analysis_strategy": "基于用户需求进行数据分析",
            "research_directions": ["数据概览", "统计分析"],
        }
    
    # 判断是否需要用户澄清
    is_relevant = intent_result.get("is_relevant", True)
    needs_clarification = intent_result.get("needs_clarification", False)
    
    if not is_relevant:
        # 数据与用户输入无关
        clarification_msg = intent_result.get(
            "clarification_message",
            "您的问题与当前数据文件不相关。请提供与数据相关的分析需求，或上传正确的数据文件。"
        )
        logger.warning(f"⚠️ [Node] 用户输入与数据无关: {clarification_msg}")
        _push_to_request_queue(request_id, f"\n\n⚠️ **需要澄清**\n\n{clarification_msg}\n\n")
        # 注意：澄清消息已经在节点执行时通过队列实时推送过了
        # stream_output 保留为空，避免重复推送
        return {
            "phase": AnalysisPhase.USER_CLARIFICATION_NEEDED.value,
            "needs_clarification": True,
            "clarification_message": clarification_msg,
            "intent_analysis_result": response,
            "stream_output": [],  # 避免重复推送
        }
    
    if needs_clarification:
        # 需要用户澄清
        clarification_msg = intent_result.get(
            "clarification_message",
            "您的分析需求不够明确，请提供更具体的分析要求。"
        )
        logger.info(f"ℹ️ [Node] 需要用户澄清: {clarification_msg}")
        _push_to_request_queue(request_id, f"\n\n❓ **需要澄清**\n\n{clarification_msg}\n\n")
        # 注意：澄清消息已经在节点执行时通过队列实时推送过了
        # stream_output 保留为空，避免重复推送
        return {
            "phase": AnalysisPhase.USER_CLARIFICATION_NEEDED.value,
            "needs_clarification": True,
            "clarification_message": clarification_msg,
            "intent_analysis_result": response,
            "stream_output": [],  # 避免重复推送
        }
    
    # 可以继续分析
    refined_prompt = intent_result.get("refined_prompt", state["user_prompt"])
    analysis_strategy = intent_result.get("analysis_strategy", "")
    research_directions = intent_result.get("research_directions", [])
    
    logger.info(f"✅ [Node] 意图分析完成")
    logger.info(f"   - 重写后的需求: {refined_prompt[:100]}...")
    logger.info(f"   - 分析策略: {analysis_strategy[:100]}...")
    logger.info(f"   - 研究方向: {research_directions}")
    
    # 只输出分析策略和研究方向，不输出标题和重写后的需求
    if analysis_strategy:
        _push_to_request_queue(request_id, f"**分析策略：**\n{analysis_strategy}\n\n")
    
    if research_directions:
        _push_to_request_queue(request_id, f"**研究方向：**\n")
        for i, direction in enumerate(research_directions, 1):
            _push_to_request_queue(request_id, f"{i}. {direction}\n")
        _push_to_request_queue(request_id, "\n")
    
    # 构建流式输出（用于状态记录）
    # 注意：所有内容（标题、流式token、格式化结果）都已经在节点执行时实时推送过了
    # stream_output 保留为空，避免重复推送
    stream_output = []
    
    return {
        "phase": AnalysisPhase.CODE_GENERATION.value,
        "refined_prompt": refined_prompt,
        "analysis_strategy": analysis_strategy,
        "research_directions": research_directions,
        "intent_analysis_result": response,
        "needs_clarification": False,
        "messages": messages + [{"role": "assistant", "content": response}],
        "stream_output": stream_output,  # 流式输出列表，每个元素都会实时传递
    }


def generate_code_node(state: AnalysisState) -> Dict[str, Any]:
    """
    代码生成节点
    
    根据用户需求和数据信息，调用 LLM 生成 Python 分析代码
    """
    logger.info("📝 [Node] 代码生成节点开始执行")
    
    # 获取请求ID（用于多线程隔离）
    request_id = state.get("request_id", "")
    
    # 创建 LLM 客户端
    client = create_llm_client(state["api_url"], state.get("api_key"))
    
    # 使用重写后的用户需求（如果存在），否则使用原始需求
    user_prompt = state.get("refined_prompt") or state["user_prompt"]
    
    # 构建 prompt
    messages = PromptTemplates.format_code_generation_prompt(
        csv_path=state["csv_path"],
        row_count=state["row_count"],
        column_names=state["column_names"],
        column_metadata=state["column_metadata"],
        data_preview=state["data_preview"],
        user_prompt=user_prompt,
    )
    
    # 收集流式输出的列表（用于后续格式化）
    stream_chunks = []
    
    def stream_callback(chunk: str):
        """流式输出回调，收集 token（同时会通过队列实时传递）"""
        stream_chunks.append(chunk)
    
    # 先输出标题（实时传递）
    _push_to_request_queue(request_id, "\n📝 **正在生成分析代码...**\n\n")
    
    # 流式调用 LLM，实时收集输出（每个 token 会通过队列实时传递）
    response = call_llm(
        client=client,
        messages=messages,
        model=state["model"],
        temperature=state["temperature"],
        stream=True,
        stream_callback=stream_callback,
        request_id=request_id,
    )
    
    # 在控制台打印LLM的完整响应
    logger.info("=" * 80)
    logger.info("📝 [代码生成] LLM 完整响应:")
    logger.info("=" * 80)
    logger.info(response)
    logger.info("=" * 80)
    
    # 提取代码
    code = extract_python_code(response)
    
    if code:
        logger.info(f"✅ [Node] 成功生成代码，长度: {len(code)} 字符")
        # 注意：代码已经在流式调用时实时推送过了，不需要再次推送格式化代码
        
        # 构建流式输出（用于状态记录）
        # 注意：所有内容（标题、流式token）都已经在节点执行时实时推送过了
        # stream_output 保留为空，避免重复推送
        stream_output = []
        
        return {
            "phase": AnalysisPhase.CODE_EXECUTION.value,
            "current_code": code,
            "code_history": [code],
            "messages": messages + [{"role": "assistant", "content": response}],
            "stream_output": stream_output,
        }
    else:
        logger.warning("⚠️ [Node] 未能从 LLM 响应中提取代码")
        _push_to_request_queue(request_id, f"\n\n⚠️ 未生成代码，LLM 直接返回：\n\n{response}\n\n")
        
        # 注意：所有内容都已经在节点执行时实时推送过了
        # stream_output 保留为空，避免重复推送
        stream_output = []
        
        return {
            "phase": AnalysisPhase.REPORT_GENERATION.value,
            "current_output": response,
            "messages": messages + [{"role": "assistant", "content": response}],
            "stream_output": stream_output,
        }


def execute_code_node(state: AnalysisState) -> Dict[str, Any]:
    """
    代码执行节点
    
    在本地安全环境中执行生成的 Python 代码
    """
    logger.info("▶️ [Node] 代码执行节点开始执行")
    
    # 获取请求ID（用于多线程隔离）
    request_id = state.get("request_id", "")
    
    # 导入执行函数
    from ..utils import execute_code_safe
    
    code = state["current_code"]
    workspace_dir = state["workspace_dir"]
    
    # 添加 matplotlib 中文支持
    chinese_matplot_setup = '''
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import warnings

chinese_fonts = ['SimHei', 'Microsoft YaHei', 'WenQuanYi Micro Hei', 'Noto Sans CJK SC']
available_fonts = [f.name for f in fm.fontManager.ttflist]
chinese_font = next((f for f in chinese_fonts if f in available_fonts), None)

if chinese_font:
    plt.rcParams['font.sans-serif'] = [chinese_font] + plt.rcParams['font.sans-serif']
else:
    warnings.filterwarnings('ignore', category=UserWarning, message='.*Glyph.*missing.*')
plt.rcParams['axes.unicode_minus'] = False
'''
    
    full_code = chinese_matplot_setup + "\n" + code
    
    # 在执行代码前，记录已有的CSV文件（用于检测新生成的文件）
    workspace_path = Path(workspace_dir)
    existing_csv_files = set()
    if workspace_path.exists():
        for csv_file in workspace_path.rglob("*.csv"):
            existing_csv_files.add(csv_file.resolve())
    
    # 执行代码
    logger.info(f"⏳ 执行代码，工作目录: {workspace_dir}")
    output = execute_code_safe(full_code, workspace_dir)
    logger.info(f"📊 代码执行完成，输出长度: {len(output)} 字符")
    
    # 检查执行结果
    success = not is_execution_error(output)
    
    # 创建执行记录
    execution = CodeExecution(
        code=code,
        output=output,
        success=success,
        error_message=output if not success else None,
        attempt=state.get("retry_count", 0) + 1,
    )
    
    if success:
        logger.info("✅ [Node] 代码执行成功")
        
        # 检查是否有新生成的CSV文件，并复制到/tmp（特别是修复后的代码执行）
        retry_count = state.get("retry_count", 0)
        if retry_count > 0:  # 如果是修复后的代码执行
            try:
                new_csv_files = []
                if workspace_path.exists():
                    for csv_file in workspace_path.rglob("*.csv"):
                        csv_resolved = csv_file.resolve()
                        if csv_resolved not in existing_csv_files:
                            new_csv_files.append(csv_resolved)
                
                if new_csv_files:
                    tmp_dir = Path("/tmp")
                    tmp_dir.mkdir(exist_ok=True)
                    logger.info(f"📁 检测到 {len(new_csv_files)} 个新生成的CSV文件，复制到 /tmp 目录...")
                    
                    for csv_file in new_csv_files:
                        try:
                            # 生成唯一的文件名（包含时间戳和原始文件名）
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            base_name = csv_file.stem
                            dest_name = f"{base_name}_{timestamp}.csv"
                            dest_path = tmp_dir / dest_name
                            
                            # 如果文件已存在，添加序号
                            counter = 1
                            while dest_path.exists():
                                dest_name = f"{base_name}_{timestamp}_{counter}.csv"
                                dest_path = tmp_dir / dest_name
                                counter += 1
                            
                            shutil.copy2(str(csv_file), str(dest_path))
                            logger.info(f"   ✅ 已复制: {csv_file.name} → /tmp/{dest_name}")
                        except Exception as e:
                            logger.warning(f"   ⚠️ 复制文件失败 {csv_file.name}: {e}")
            except Exception as e:
                logger.warning(f"⚠️ 检查并复制CSV文件时出错: {e}")
        
        # 根据配置决定是否输出执行结果
        debug_print = state.get("debug_print_execution_output", False)
        if debug_print:
            _push_to_request_queue(request_id, "\n✅ **代码执行完毕**\n\n")
            _push_to_request_queue(request_id, "📊 **执行结果：**\n\n")
            _push_to_request_queue(request_id, f"```\n{output}\n```\n\n")
            _push_to_request_queue(request_id, "正在生成分析报告...\n\n")
        else:
            # 默认不显示具体执行结果
            _push_to_request_queue(request_id, "\n✅ **代码执行完毕，正在生成分析报告...**\n\n")
        return {
            "phase": AnalysisPhase.REPORT_GENERATION.value,
            "current_output": output,
            "execution_success": True,
            "execution_history": [execution],
            "round_count": state.get("round_count", 0) + 1,
            "stream_output": [],
        }
    else:
        logger.warning(f"❌ [Node] 代码执行失败: {output[:200]}...")
        return {
            "phase": AnalysisPhase.ERROR_FIXING.value,
            "current_output": output,
            "execution_success": False,
            "error_message": output,
            "execution_history": [execution],
            "stream_output": [f"\n❌ **执行出错：**\n\n```\n{output}\n```\n\n"],
        }


def fix_code_node(state: AnalysisState) -> Dict[str, Any]:
    """
    代码修复节点
    
    当代码执行失败时，调用 LLM 修复代码
    """
    logger.info("🔧 [Node] 代码修复节点开始执行")
    
    # 获取请求ID（用于多线程隔离）
    request_id = state.get("request_id", "")
    
    retry_count = state.get("retry_count", 0) + 1
    max_retries = 3
    
    if retry_count > max_retries:
        logger.error(f"❌ [Node] 已达到最大重试次数 ({max_retries})")
        return {
            "phase": AnalysisPhase.REPORT_GENERATION.value,
            "retry_count": retry_count,
            "stream_output": [f"\n⚠️ 已达到最大重试次数 ({max_retries})，跳过代码执行，直接生成报告\n\n"],
        }
    
    # 创建 LLM 客户端
    client = create_llm_client(state["api_url"], state.get("api_key"))
    
    # 构建修复 prompt
    messages = PromptTemplates.format_code_fix_prompt(
        original_code=state["current_code"],
        error_message=state.get("error_message", "未知错误"),
        csv_path=state["csv_path"],
        column_names=state["column_names"],
    )
    
    # 收集流式输出的列表（用于后续格式化）
    stream_chunks = []
    
    def stream_callback(chunk: str):
        """流式输出回调，收集 token（同时会通过队列实时传递）"""
        stream_chunks.append(chunk)
    
    # 先输出标题（实时传递）
    _push_to_request_queue(request_id, f"\n🔧 **正在修复代码（尝试 {retry_count}/{max_retries}）...**\n\n")
    
    # 流式调用 LLM 修复（每个 token 会通过队列实时传递）
    response = call_llm(
        client=client,
        messages=messages,
        model=state["model"],
        temperature=state["temperature"],
        stream=True,
        stream_callback=stream_callback,
        request_id=request_id,
    )
    
    # 在控制台打印LLM的完整响应
    logger.info("=" * 80)
    logger.info(f"🔧 [代码修复] LLM 完整响应 (尝试 {retry_count}/{max_retries}):")
    logger.info("=" * 80)
    logger.info(response)
    logger.info("=" * 80)
    
    # 提取修复后的代码
    fixed_code = extract_python_code(response)
    
    if fixed_code:
        logger.info(f"✅ [Node] 成功获取修复代码，重试次数: {retry_count}")
        # 注意：代码已经在流式调用时实时推送过了，不需要再次推送格式化代码
        
        # 注意：所有内容（标题、流式token）都已经在节点执行时实时推送过了
        # stream_output 保留为空，避免重复推送
        stream_output = []
        
        return {
            "phase": AnalysisPhase.CODE_EXECUTION.value,
            "current_code": fixed_code,
            "code_history": [fixed_code],
            "retry_count": retry_count,
            "stream_output": stream_output,
        }
    else:
        logger.warning("⚠️ [Node] 未能从修复响应中提取代码")
        _push_to_request_queue(request_id, f"\n\n⚠️ 无法修复代码，跳过执行，直接生成报告\n\n")
        
        # 注意：所有内容都已经在节点执行时实时推送过了
        # stream_output 保留为空，避免重复推送
        stream_output = []
        
        return {
            "phase": AnalysisPhase.REPORT_GENERATION.value,
            "retry_count": retry_count,
            "stream_output": stream_output,
        }


def generate_report_node(state: AnalysisState) -> Dict[str, Any]:
    """
    报告生成节点
    
    根据代码执行结果，调用 LLM 生成分析报告
    """
    logger.info("📄 [Node] 报告生成节点开始执行")
    
    # 获取请求ID（用于多线程隔离）
    request_id = state.get("request_id", "")
    
    # 创建 LLM 客户端
    client = create_llm_client(state["api_url"], state.get("api_key"))
    
    # 获取最后执行的代码和输出
    code = state.get("current_code", "")
    output = state.get("current_output", "")
    
    # 如果没有执行输出，使用代码历史中的最后一个
    if not output and state.get("execution_history"):
        last_execution = state["execution_history"][-1]
        code = last_execution.code
        output = last_execution.output
    
    # 构建报告 prompt（包含表头元数据）
    messages = PromptTemplates.format_report_generation_prompt(
        user_prompt=state["user_prompt"],
        code=code,
        execution_output=output,
        column_names=state.get("column_names", []),
        column_metadata=state.get("column_metadata", {}),
    )
    
    # 收集流式输出的列表（用于后续格式化）
    stream_chunks = []
    
    def stream_callback(chunk: str):
        """流式输出回调，收集 token（同时会通过队列实时传递）"""
        stream_chunks.append(chunk)
    
    # 先输出标题（实时传递）
    _push_to_request_queue(request_id, "\n📄 **正在生成分析报告...**\n\n")
    
    # 流式调用 LLM 生成报告（每个 token 会通过队列实时传递）
    report = call_llm(
        client=client,
        messages=messages,
        model=state["model"],
        temperature=state["temperature"],
        stream=True,
        stream_callback=stream_callback,
        request_id=request_id,
    )
    
    # 在控制台打印LLM的完整响应
    logger.info("=" * 80)
    logger.info("📄 [报告生成] LLM 完整响应:")
    logger.info("=" * 80)
    logger.info(report)
    logger.info("=" * 80)
    
    logger.info(f"✅ [Node] 成功生成报告，长度: {len(report)} 字符")
    
    # 构建流式输出（用于状态记录）
    # 注意：所有内容（标题、流式token）都已经在节点执行时实时推送过了
    # stream_output 保留为空，避免重复推送
    stream_output = []
    
    return {
        "phase": AnalysisPhase.COMPLETED.value,
        "report": report,
        "stream_output": stream_output,
    }


# ============================================================================
# 条件路由函数
# ============================================================================

def route_after_execution(state: AnalysisState) -> Literal["fix_code", "generate_report"]:
    """
    执行后路由决策
    
    根据执行结果决定下一步：
    - 执行成功 → 生成报告
    - 执行失败 → 修复代码
    """
    if state.get("execution_success", False):
        return "generate_report"
    else:
        return "fix_code"


def route_after_fix(state: AnalysisState) -> Literal["execute_code", "generate_report"]:
    """
    修复后路由决策
    
    根据修复结果决定下一步：
    - 有新代码 → 重新执行
    - 无法修复 → 生成报告
    """
    if state.get("phase") == AnalysisPhase.CODE_EXECUTION.value:
        return "execute_code"
    else:
        return "generate_report"


# ============================================================================
# 工作流图构建
# ============================================================================

def create_analysis_graph() -> StateGraph:
    """
    创建数据分析工作流图
    
    工作流结构：
    
    START → analyze_intent ─┬─(需要澄清)─→ END
                            │
                            └─(可以分析)─→ generate_code → execute_code ─┬─(成功)─→ generate_report → END
                                                                          │
                                                                          └─(失败)─→ fix_code ─┬─(有修复)─→ execute_code
                                                                                              │
                                                                                              └─(无法修复)─→ generate_report
    
    Returns:
        编译后的 StateGraph
    """
    # 创建状态图
    workflow = StateGraph(AnalysisState)
    
    # 添加节点
    workflow.add_node("analyze_intent", analyze_intent_node)
    workflow.add_node("generate_code", generate_code_node)
    workflow.add_node("execute_code", execute_code_node)
    workflow.add_node("fix_code", fix_code_node)
    workflow.add_node("generate_report", generate_report_node)
    
    # 添加边
    # START → analyze_intent
    workflow.add_edge(START, "analyze_intent")
    
    # analyze_intent → generate_code 或 END（需要澄清）
    def route_after_intent(state: AnalysisState) -> Literal["generate_code", "end"]:
        """意图分析后的路由决策"""
        phase = state.get("phase", "")
        if phase == AnalysisPhase.CODE_GENERATION.value:
            return "generate_code"
        else:
            # 需要澄清或其他情况，直接结束
            return "end"
    
    workflow.add_conditional_edges(
        "analyze_intent",
        route_after_intent,
        {
            "generate_code": "generate_code",
            "end": END,
        }
    )
    
    # generate_code → execute_code (如果生成了代码)
    workflow.add_conditional_edges(
        "generate_code",
        lambda s: "execute_code" if s.get("phase") == AnalysisPhase.CODE_EXECUTION.value else "generate_report",
        {
            "execute_code": "execute_code",
            "generate_report": "generate_report",
        }
    )
    
    # 处理需要澄清的情况（直接结束）
    # 注意：analyze_intent 节点如果返回 USER_CLARIFICATION_NEEDED，会通过条件边路由到 END
    
    # execute_code → fix_code 或 generate_report
    workflow.add_conditional_edges(
        "execute_code",
        route_after_execution,
        {
            "fix_code": "fix_code",
            "generate_report": "generate_report",
        }
    )
    
    # fix_code → execute_code 或 generate_report
    workflow.add_conditional_edges(
        "fix_code",
        route_after_fix,
        {
            "execute_code": "execute_code",
            "generate_report": "generate_report",
        }
    )
    
    # generate_report → END
    workflow.add_edge("generate_report", END)
    
    return workflow


# ============================================================================
# 高级封装类
# ============================================================================

class DataAnalysisGraph:
    """
    数据分析图封装类
    
    提供简化的 API 用于执行数据分析工作流
    """
    
    def __init__(self):
        """初始化工作流图"""
        self._workflow = create_analysis_graph()
        self._graph = self._workflow.compile()
    
    def analyze(
        self,
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
    ) -> AnalysisResult:
        """
        执行数据分析（非流式）
        
        Args:
            workspace_dir: 工作空间目录
            thread_id: 会话ID
            csv_path: CSV文件路径
            column_names: 列名列表
            column_metadata: 列元数据
            row_count: 数据行数
            data_preview: 数据预览
            user_prompt: 用户分析需求
            api_url: LLM API 地址
            model: 模型名称
            api_key: LLM API 密钥
            temperature: 生成温度
            
        Returns:
            AnalysisResult 分析结果
        """
        # 创建初始状态
        initial_state = create_initial_state(
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
        
        # 执行工作流
        final_state = self._graph.invoke(initial_state)
        
        # 构建结果
        return AnalysisResult(
            success=final_state.get("phase") == AnalysisPhase.COMPLETED.value,
            report=final_state.get("report", ""),
            code_history=final_state.get("code_history", []),
            execution_outputs=[
                e.output for e in final_state.get("execution_history", [])
            ],
            generated_files=final_state.get("generated_files", []),
            error_message=final_state.get("error_message"),
            total_rounds=final_state.get("round_count", 0),
        )
    
    def analyze_stream(
        self,
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
    ) -> Generator[str, None, AnalysisResult]:
        """
        执行数据分析（流式输出）
        
        使用 LangGraph 的 stream 模式 + 线程队列实现真正的实时流式输出
        在节点执行过程中，LLM 的每个 token 都会实时传递
        
        每个请求使用独立的队列，确保多线程安全。
        
        Yields:
            str: 流式输出的字符串块
            
        Returns:
            AnalysisResult 分析结果
        """
        import uuid
        
        # 为每个请求生成唯一的 request_id（用于队列隔离）
        request_id = f"req-{uuid.uuid4().hex[:16]}"
        logger.info(f"🚀 开始分析请求: {request_id}")
        
        # 为该请求创建独立的队列（多线程安全）
        request_queue = _create_request_queue(request_id)
        
        try:
            # 创建初始状态（包含 request_id）
            initial_state = create_initial_state(
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
                request_id=request_id,  # 传递请求ID
                debug_print_execution_output=debug_print_execution_output,  # 传递调试配置
            )
            
            # 在后台线程中执行工作流
            final_state = None
            execution_done = threading.Event()
            execution_error = [None]  # 使用列表以便在线程间共享
            
            def run_graph():
                """在后台线程中执行工作流"""
                nonlocal final_state
                try:
                    for state_update in self._graph.stream(initial_state):
                        # state_update 是 {node_name: node_output} 的字典
                        for node_name, node_output in state_update.items():
                            logger.debug(f"📊 节点 {node_name} 完成，输出状态更新 (request_id={request_id})")
                            
                            # 输出节点完成后的格式化内容
                            if "stream_output" in node_output:
                                stream_output_list = node_output["stream_output"]
                                # 如果是列表，将格式化内容推送到队列
                                if isinstance(stream_output_list, list):
                                    for chunk in stream_output_list:
                                        if chunk and chunk.strip():
                                            _push_to_request_queue(request_id, chunk)
                                elif stream_output_list:
                                    _push_to_request_queue(request_id, stream_output_list)
                            
                            # 更新最终状态
                            final_state = node_output
                except Exception as e:
                    execution_error[0] = e
                    logger.error(f"❌ 工作流执行出错 (request_id={request_id}): {e}", exc_info=True)
                finally:
                    execution_done.set()
                    # 发送结束标记到该请求的队列
                    _push_to_request_queue(request_id, None)
            
            # 启动后台线程执行工作流
            graph_thread = threading.Thread(target=run_graph, daemon=True)
            graph_thread.start()
            
            # 实时从该请求的队列中读取并 yield token
            while True:
                try:
                    # 从该请求的队列中获取 token（超时0.1秒，避免阻塞太久）
                    chunk = request_queue.get(timeout=0.1)
                    
                    # None 表示结束
                    if chunk is None:
                        break
                    
                    # 实时 yield token
                    yield chunk
                    
                except queue.Empty:
                    # 检查工作流是否已完成
                    if execution_done.is_set():
                        # 清空队列中剩余的内容
                        while True:
                            try:
                                chunk = request_queue.get_nowait()
                                if chunk is None:
                                    break
                                yield chunk
                            except queue.Empty:
                                break
                        break
                    # 继续等待
                    continue
            
            # 等待工作流线程完成
            # 使用传入的超时时间，默认360秒
            timeout_seconds = analysis_timeout if analysis_timeout is not None else 360
            graph_thread.join(timeout=timeout_seconds)
            
            # 如果线程仍在运行，说明超时了
            if graph_thread.is_alive():
                logger.warning(f"⚠️ 分析超时（{timeout_seconds}秒），强制结束 (request_id={request_id})")
                yield f"\n\n⚠️ **分析超时**\n\n分析过程超过 {timeout_seconds} 秒，已自动终止。\n\n"
                # 注意：daemon 线程会在主线程退出时自动终止
            
            # 检查是否有错误
            if execution_error[0]:
                raise execution_error[0]
            
            # 构建最终结果
            if final_state:
                return AnalysisResult(
                    success=final_state.get("phase") == AnalysisPhase.COMPLETED.value,
                    report=final_state.get("report", ""),
                    code_history=final_state.get("code_history", []),
                    execution_outputs=[
                        e.output for e in final_state.get("execution_history", [])
                    ] if final_state.get("execution_history") else [],
                    generated_files=final_state.get("generated_files", []),
                    error_message=final_state.get("error_message"),
                    total_rounds=final_state.get("round_count", 0),
                )
            else:
                return AnalysisResult(
                    success=False,
                    error_message="工作流执行失败",
                )
        finally:
            # 清理该请求的队列（不影响其他请求）
            _remove_request_queue(request_id)
            logger.info(f"🏁 分析请求完成: {request_id}")

