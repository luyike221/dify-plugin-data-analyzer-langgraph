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
# 队列状态标记：记录消费方是否断开连接
_queue_disconnected: Dict[str, bool] = {}


def _create_request_queue(request_id: str) -> queue.Queue:
    """为请求创建独立的队列"""
    with _queues_lock:
        q = queue.Queue(maxsize=1000)
        _request_queues[request_id] = q
        _queue_disconnected[request_id] = False  # 初始化为未断开
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
        # 移除断开标记
        if request_id in _queue_disconnected:
            del _queue_disconnected[request_id]
        logger.debug(f"🧹 移除请求队列: {request_id}")


def _mark_queue_disconnected(request_id: str):
    """标记队列的消费方已断开连接"""
    with _queues_lock:
        _queue_disconnected[request_id] = True
        logger.warning(f"⚠️ 标记队列 {request_id} 为已断开，将停止推送数据")


def _is_queue_disconnected(request_id: str) -> bool:
    """检查队列的消费方是否已断开连接"""
    with _queues_lock:
        return _queue_disconnected.get(request_id, False)


def _push_to_request_queue(request_id: str, chunk: Optional[str]):
    """推送到指定请求的队列（chunk 为 None 表示结束标记）
    
    如果消费方已断开或队列接近满，将停止推送以避免资源浪费
    """
    # 检查消费方是否已断开
    if _is_queue_disconnected(request_id):
        # 结束标记仍然推送，以便后台线程知道可以结束
        if chunk is None:
            # 结束标记仍然尝试推送（使用非阻塞方式）
            q = _get_request_queue(request_id)
            if q is not None:
                try:
                    q.put_nowait(chunk)
                except queue.Full:
                    pass
        # 非结束标记直接返回，不推送
        return
    
    q = _get_request_queue(request_id)
    if q is not None:
        # 检查队列大小，如果接近满（>90%），停止推送非关键数据
        queue_size = q.qsize()
        queue_maxsize = q.maxsize
        if queue_size > queue_maxsize * 0.9:
            # 队列接近满，只推送结束标记，其他数据跳过
            if chunk is None:
                # 结束标记仍然尝试推送
                try:
                    q.put_nowait(chunk)
                except queue.Full:
                    pass
            else:
                # 非结束标记跳过，避免队列满
                logger.warning(f"⚠️ 请求 {request_id} 的队列接近满 ({queue_size}/{queue_maxsize})，跳过 chunk")
            return
        
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
        try:
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
        except Exception as e:
            # 检查是否是超时错误
            error_type_name = type(e).__name__
            error_str = str(e).lower()
            is_timeout_error = (
                'timeout' in error_type_name.lower() or 
                'timeout' in error_str or 
                'timed out' in error_str or
                ('httpcore' in str(type(e).__module__) and 'timeout' in error_type_name.lower()) or
                ('httpx' in str(type(e).__module__) and 'timeout' in error_type_name.lower())
            )
            
            if is_timeout_error:
                # 超时错误：只记录警告，不输出完整堆栈
                logger.warning(f"⏰ LLM调用超时 (request_id={request_id}): {error_type_name} - {str(e)}")
            else:
                # 其他错误：正常记录
                logger.error(f"❌ LLM调用出错 (request_id={request_id}): {e}", exc_info=True)
            # 重新抛出异常，让上层处理
            raise
        
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

def plan_strategy_node(state: AnalysisState) -> Dict[str, Any]:
    """
    策略制定节点
    
    职责：制定数据分析策略，包括分析方法选择、任务分解、优先级排序
    """
    logger.info("🎯 [Node] 策略制定节点开始执行")
    
    request_id = state.get("request_id", "")
    client = create_llm_client(state["api_url"], state.get("api_key"))
    
    # 构建策略制定 prompt
    messages = PromptTemplates.format_strategy_planning_prompt(
        csv_path=state["csv_path"],
        row_count=state["row_count"],
        column_names=state["column_names"],
        column_metadata=state["column_metadata"],
        data_preview=state["data_preview"],
        user_prompt=state["user_prompt"],
    )
    
    # 流式调用 LLM（不输出 JSON 到用户）
    response = call_llm(
        client=client,
        messages=messages,
        model=state["model"],
        temperature=state["temperature"],
        stream=True,
        push_to_queue=False,
        request_id=request_id,
    )
    
    logger.info("=" * 80)
    logger.info("🎯 [策略制定] LLM 完整响应:")
    logger.info(response)
    logger.info("=" * 80)
    
    # 解析 JSON 响应
    import json
    try:
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
        json_str = json_match.group(1) if json_match else response
        strategy_result = json.loads(json_str.strip())
    except (json.JSONDecodeError, AttributeError) as e:
        logger.warning(f"⚠️ [Node] 无法解析策略制定结果: {e}")
        strategy_result = {
            "is_relevant": True,
            "needs_clarification": False,
            "type": "overview",
            "refined_query": state["user_prompt"],
            "tasks": ["数据概览分析"],
            "first_task": state["user_prompt"],
        }
    
    # 判断是否需要澄清
    is_relevant = strategy_result.get("is_relevant", True)
    needs_clarification = strategy_result.get("needs_clarification", False)
    
    if not is_relevant or needs_clarification:
        clarification_msg = strategy_result.get(
            "clarification_message",
            "您的分析需求不够明确，请提供更具体的要求。"
        )
        _push_to_request_queue(request_id, f"\n\n❓ **需要澄清**\n\n{clarification_msg}\n\n")
        return {
            "phase": AnalysisPhase.USER_CLARIFICATION_NEEDED.value,
            "analysis_strategy": {
                "type": "",
                "refined_query": state["user_prompt"],
                "tasks": [],
                "current_task": "",
                "completed_tasks": [],
                "needs_clarification": True,
                "clarification_message": clarification_msg,
            },
            "stream_output": [],
        }
    
    # 构建统一策略对象
    analysis_type = strategy_result.get("type", "overview")
    refined_query = strategy_result.get("refined_query", state["user_prompt"])
    tasks = strategy_result.get("tasks", [refined_query])
    first_task = strategy_result.get("first_task", tasks[0] if tasks else refined_query)
    
    strategy = {
        "type": analysis_type,
        "refined_query": refined_query,
        "tasks": tasks,
        "current_task": first_task,
        "completed_tasks": [],
        "needs_clarification": False,
        "clarification_message": None,
    }
    
    logger.info(f"✅ [Node] 策略制定完成")
    logger.info(f"   - 分析类型: {analysis_type}")
    logger.info(f"   - 分析任务: {tasks}")
    logger.info(f"   - 首个任务: {first_task}")
    
    # 输出分析策略
    _push_to_request_queue(request_id, f"**分析类型：** {analysis_type}\n\n")
    if tasks:
        _push_to_request_queue(request_id, "**分析策略：**\n")
        for i, task in enumerate(tasks, 1):
            _push_to_request_queue(request_id, f"{i}. {task}\n")
        _push_to_request_queue(request_id, "\n")
    
    return {
        "phase": AnalysisPhase.CODE_GENERATION.value,
        "analysis_strategy": strategy,
        "messages": messages + [{"role": "assistant", "content": response}],
        "stream_output": [],
    }


def generate_code_node(state: AnalysisState) -> Dict[str, Any]:
    """
    代码生成节点
    
    职责：根据当前分析任务生成 Python 代码
    支持多轮分析，后续轮会传入之前的分析结果
    """
    logger.info("📝 [Node] 代码生成节点开始执行")
    
    request_id = state.get("request_id", "")
    client = create_llm_client(state["api_url"], state.get("api_key"))
    
    # 确定当前任务和是否是首轮
    round_count = state.get("round_count", 0)
    is_first_round = round_count == 0
    
    # 从策略对象获取当前任务
    strategy = state.get("analysis_strategy", {})
    current_task = strategy.get("current_task") or strategy.get("refined_query") or state["user_prompt"]
    
    # 获取之前的分析结果（用于后续轮）
    all_outputs = state.get("all_execution_outputs", [])
    previous_results = None
    if not is_first_round and all_outputs:
        previous_results = "\n\n---\n\n".join([
            f"【第 {i+1} 轮分析】\n{output}"
            for i, output in enumerate(all_outputs)
        ])
    
    logger.info(f"   - 轮次: {round_count + 1}")
    logger.info(f"   - 首轮: {is_first_round}")
    logger.info(f"   - 当前任务: {current_task[:100]}...")
    
    # 构建 prompt（区分首轮和后续轮）
    messages = PromptTemplates.format_code_generation_prompt(
        csv_path=state["csv_path"],
        row_count=state["row_count"],
        column_names=state["column_names"],
        column_metadata=state["column_metadata"],
        data_preview=state["data_preview"],
        user_prompt=current_task,
        previous_results=previous_results,
        is_first_round=is_first_round,
    )
    
    # 输出标题
    if is_first_round:
        _push_to_request_queue(request_id, "\n📝 **正在生成分析代码...**\n\n")
    else:
        _push_to_request_queue(request_id, f"\n📝 **正在生成第 {round_count + 1} 轮分析代码...**\n\n")
    
    # 流式调用 LLM
    response = call_llm(
        client=client,
        messages=messages,
        model=state["model"],
        temperature=state["temperature"],
        stream=True,
        request_id=request_id,
    )
    
    logger.info("=" * 80)
    logger.info(f"📝 [代码生成] LLM 完整响应 (轮次 {round_count + 1}):")
    logger.info(response)
    logger.info("=" * 80)
    
    # 提取代码
    code = extract_python_code(response)
    
    if code:
        logger.info(f"✅ [Node] 成功生成代码，长度: {len(code)} 字符")
        return {
            "phase": AnalysisPhase.CODE_EXECUTION.value,
            "current_code": code,
            "code_history": [code],
            "messages": messages + [{"role": "assistant", "content": response}],
            "stream_output": [],
        }
    else:
        logger.warning("⚠️ [Node] 未能从 LLM 响应中提取代码")
        _push_to_request_queue(request_id, f"\n\n⚠️ 未生成代码，直接返回分析结果\n\n")
        return {
            "phase": AnalysisPhase.REPORT_GENERATION.value,
            "current_output": response,
            "messages": messages + [{"role": "assistant", "content": response}],
            "stream_output": [],
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
            _push_to_request_queue(request_id, "正在评估分析完整性...\n\n")
        else:
            # 默认不显示具体执行结果
            _push_to_request_queue(request_id, "\n✅ **代码执行完毕，正在评估分析完整性...**\n\n")
        
        # 成功后进入分析完整性评估节点（新流程）
        return {
            "phase": AnalysisPhase.EVALUATE_COMPLETENESS.value,
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


def evaluate_completeness_node(state: AnalysisState) -> Dict[str, Any]:
    """
    分析完整性评估节点（新增）
    
    判断当前分析是否已经充分回答了用户的问题，是否需要进行更多的深入分析。
    
    功能：
    1. 评估当前分析结果的覆盖度和深度
    2. 判断是否需要进行更多分析
    3. 如果需要，生成下一轮分析的具体方向
    """
    logger.info("🔍 [Node] 分析完整性评估节点开始执行")
    
    # 获取请求ID（用于多线程隔离）
    request_id = state.get("request_id", "")
    
    # 获取当前轮次和最大轮次
    current_round = state.get("round_count", 1)
    max_rounds = state.get("max_analysis_rounds", 3)
    
    logger.info(f"   - 当前轮次: {current_round}/{max_rounds}")
    
    # 如果已达到最大轮数，直接去报告生成
    if current_round >= max_rounds:
        logger.info(f"📊 已达到最大分析轮数 ({max_rounds})，进入报告生成")
        _push_to_request_queue(request_id, f"\n📊 已完成 {current_round} 轮分析（达到最大轮数），正在生成最终报告...\n\n")
        return {
            "phase": AnalysisPhase.REPORT_GENERATION.value,
            "need_more_analysis": False,
            "stream_output": [],
        }
    
    # 创建 LLM 客户端
    client = create_llm_client(state["api_url"], state.get("api_key"))
    
    # 获取之前的输出
    previous_outputs = state.get("all_execution_outputs", [])
    current_output = state.get("current_output", "")
    
    # 从策略对象获取任务信息
    strategy = state.get("analysis_strategy", {})
    analysis_tasks = strategy.get("tasks", [])
    completed_tasks = strategy.get("completed_tasks", [])
    
    # 构建评估 prompt
    messages = PromptTemplates.format_evaluate_completeness_prompt(
        user_prompt=state["user_prompt"],
        analysis_tasks=analysis_tasks,
        current_output=current_output,
        previous_outputs=previous_outputs,
        completed_tasks=completed_tasks,
        current_round=current_round,
        max_rounds=max_rounds,
    )
    
    # 流式调用 LLM 评估（不实时输出，避免显示 JSON）
    response = call_llm(
        client=client,
        messages=messages,
        model=state["model"],
        temperature=0.3,  # 较低温度，更确定性的判断
        stream=True,
        push_to_queue=False,  # 不推送到队列，避免输出JSON
        request_id=request_id,
    )
    
    # 在控制台打印LLM的完整响应
    logger.info("=" * 80)
    logger.info(f"🔍 [分析完整性评估] LLM 完整响应 (轮次 {current_round}/{max_rounds}):")
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
        
        eval_result = json.loads(json_str.strip())
    except (json.JSONDecodeError, AttributeError) as e:
        logger.warning(f"⚠️ [Node] 无法解析评估结果: {e}")
        # 如果解析失败，默认不需要更多分析
        eval_result = {
            "need_more_analysis": False,
            "reason": "评估结果解析失败，默认结束分析",
            "completed_aspects": [],
            "missing_aspects": [],
            "next_direction": "",
        }
    
    # 获取评估结果（使用新字段名）
    need_more = eval_result.get("need_more_analysis", False)
    reason = eval_result.get("reason", "")
    completed_tasks_new = eval_result.get("completed_tasks", [])
    next_task = eval_result.get("next_task", "")
    
    logger.info(f"   - 需要更多分析: {need_more}")
    logger.info(f"   - 理由: {reason}")
    if need_more:
        logger.info(f"   - 下一步任务: {next_task}")
    
    if need_more and next_task:
        # 需要继续分析
        logger.info(f"🔄 需要更多分析，下一任务: {next_task}")
        _push_to_request_queue(request_id, f"\n🔄 **继续分析（第 {current_round + 1} 轮）**\n\n")
        _push_to_request_queue(request_id, f"**任务：** {next_task}\n\n")
        
        # 更新策略对象
        strategy = state.get("analysis_strategy", {}).copy()
        strategy["current_task"] = next_task
        strategy["completed_tasks"] = completed_tasks_new
        
        return {
            "phase": AnalysisPhase.CODE_GENERATION.value,
            "need_more_analysis": True,
            "analysis_strategy": strategy,
            "all_execution_outputs": [current_output],  # 累积执行输出
            "stream_output": [],
        }
    else:
        # 分析已完成
        logger.info("✅ 分析已完成，进入报告生成")
        _push_to_request_queue(request_id, f"\n✅ **分析完成**（共 {current_round} 轮），正在生成报告...\n\n")
        
        # 更新策略对象
        strategy = state.get("analysis_strategy", {}).copy()
        strategy["completed_tasks"] = completed_tasks_new
        
        return {
            "phase": AnalysisPhase.REPORT_GENERATION.value,
            "need_more_analysis": False,
            "analysis_strategy": strategy,
            "all_execution_outputs": [current_output],  # 累积执行输出
            "stream_output": [],
        }


def generate_report_node(state: AnalysisState) -> Dict[str, Any]:
    """
    报告生成节点
    
    职责：综合所有分析结果，生成最终报告
    """
    logger.info("📄 [Node] 报告生成节点开始执行")
    
    request_id = state.get("request_id", "")
    client = create_llm_client(state["api_url"], state.get("api_key"))
    
    # 从策略对象获取分析类型和轮次
    strategy = state.get("analysis_strategy", {})
    analysis_type = strategy.get("type", "overview")
    round_count = state.get("round_count", 1)
    
    logger.info(f"   - 分析类型: {analysis_type}")
    logger.info(f"   - 共完成 {round_count} 轮分析")
    
    # 整合所有分析结果
    all_outputs = state.get("all_execution_outputs", [])
    current_output = state.get("current_output", "")
    
    # 构建结果文本
    all_results_parts = []
    
    # 添加之前轮次的输出
    for i, out in enumerate(all_outputs, 1):
        if out and out.strip():
            all_results_parts.append(f"### 第 {i} 轮分析结果\n\n```\n{out}\n```")
    
    # 添加当前输出
    if current_output and current_output not in all_outputs:
        round_num = len(all_outputs) + 1
        all_results_parts.append(f"### 第 {round_num} 轮分析结果\n\n```\n{current_output}\n```")
    
    all_results = "\n\n".join(all_results_parts) if all_results_parts else f"```\n{current_output}\n```"
    
    # 如果没有输出，尝试从执行历史获取
    if not all_results.strip() or all_results == "```\n\n```":
        if state.get("execution_history"):
            last_execution = state["execution_history"][-1]
            all_results = f"```\n{last_execution.output}\n```"
    
    # 构建报告 prompt（使用新格式）
    messages = PromptTemplates.format_report_generation_prompt(
        user_prompt=state["user_prompt"],
        analysis_type=analysis_type,
        total_rounds=round_count,
        all_results=all_results,
        column_names=state.get("column_names", []),
        column_metadata=state.get("column_metadata", {}),
    )
    
    # 输出标题
    _push_to_request_queue(request_id, "\n📄 **正在生成分析报告...**\n\n")
    
    # 流式调用 LLM 生成报告
    report = call_llm(
        client=client,
        messages=messages,
        model=state["model"],
        temperature=state["temperature"],
        stream=True,
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

def route_after_execution(state: AnalysisState) -> Literal["fix_code", "evaluate_completeness"]:
    """
    执行后路由决策
    
    根据执行结果决定下一步：
    - 执行成功 → 评估分析完整性（新流程）
    - 执行失败 → 修复代码
    """
    if state.get("execution_success", False):
        return "evaluate_completeness"
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


def route_after_evaluation(state: AnalysisState) -> Literal["generate_code", "generate_report"]:
    """
    评估后路由决策（新增）
    
    根据评估结果决定下一步：
    - 需要更多分析 → 回到代码生成（形成循环）
    - 分析已完成 → 生成报告
    """
    if state.get("need_more_analysis", False):
        return "generate_code"
    else:
        return "generate_report"


# ============================================================================
# 工作流图构建
# ============================================================================

def create_analysis_graph() -> StateGraph:
    """
    创建数据分析工作流图
    
    工作流结构（支持多轮分析）：
    
    START → plan_strategy ─┬─(需要澄清)─→ END
                           │
                           └─(可以分析)─→ generate_code → execute_code ─┬─(成功)─→ evaluate_completeness ─┬─(需要更多)─→ generate_code (循环)
                                             ↑                          │                                 │
                                             │                          │                                 └─(完成)─→ generate_report → END
                                             │                          │
                                             │                          └─(失败)─→ fix_code ─┬─(有修复)─→ execute_code
                                             │                                              │
                                             └──────────────────────────────────────────────┴─(无法修复)─→ generate_report
    
    Returns:
        编译后的 StateGraph
    """
    # 创建状态图
    workflow = StateGraph(AnalysisState)
    
    # 添加节点
    workflow.add_node("plan_strategy", plan_strategy_node)
    workflow.add_node("generate_code", generate_code_node)
    workflow.add_node("execute_code", execute_code_node)
    workflow.add_node("fix_code", fix_code_node)
    workflow.add_node("evaluate_completeness", evaluate_completeness_node)  # 分析完整性评估节点
    workflow.add_node("generate_report", generate_report_node)
    
    # 添加边
    # START → plan_strategy
    workflow.add_edge(START, "plan_strategy")
    
    # plan_strategy → generate_code 或 END（需要澄清）
    def route_after_strategy(state: AnalysisState) -> Literal["generate_code", "end"]:
        """策略制定后的路由决策"""
        phase = state.get("phase", "")
        if phase == AnalysisPhase.CODE_GENERATION.value:
            return "generate_code"
        else:
            # 需要澄清或其他情况，直接结束
            return "end"
    
    workflow.add_conditional_edges(
        "plan_strategy",
        route_after_strategy,
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
    # 注意：plan_strategy 节点如果返回 USER_CLARIFICATION_NEEDED，会通过条件边路由到 END
    
    # execute_code → fix_code 或 evaluate_completeness（新流程）
    workflow.add_conditional_edges(
        "execute_code",
        route_after_execution,
        {
            "fix_code": "fix_code",
            "evaluate_completeness": "evaluate_completeness",  # 成功后去评估节点
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
    
    # evaluate_completeness → generate_code (需要更多分析) 或 generate_report (完成)
    workflow.add_conditional_edges(
        "evaluate_completeness",
        route_after_evaluation,
        {
            "generate_code": "generate_code",  # 循环回到代码生成
            "generate_report": "generate_report",  # 进入报告生成
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
        max_analysis_rounds: int = 3,  # 新增：最大分析轮数
    ) -> Generator[str, None, AnalysisResult]:
        """
        执行数据分析（流式输出）
        
        使用 LangGraph 的 stream 模式 + 线程队列实现真正的实时流式输出
        在节点执行过程中，LLM 的每个 token 都会实时传递
        
        每个请求使用独立的队列，确保多线程安全。
        
        支持多轮分析：系统会自动评估分析完整性，如果需要更多分析，
        会继续生成代码并执行，直到分析完成或达到最大轮数。
        
        Args:
            max_analysis_rounds: 最大分析轮数（默认3轮），防止无限循环
        
        Yields:
            str: 流式输出的字符串块
            
        Returns:
            AnalysisResult 分析结果
        """
        import uuid
        
        # 为每个请求生成唯一的 request_id（用于队列隔离）
        request_id = f"req-{uuid.uuid4().hex[:16]}"
        logger.info(f"🚀 开始分析请求: {request_id}，最大分析轮数: {max_analysis_rounds}")
        
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
                max_analysis_rounds=max_analysis_rounds,  # 传递最大分析轮数
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
                    # 检查是否是超时错误（ReadTimeout, ConnectTimeout等）
                    is_timeout_error = False
                    error_type_name = type(e).__name__
                    error_str = str(e).lower()
                    
                    # 检查错误类型名称
                    if 'timeout' in error_type_name.lower():
                        is_timeout_error = True
                    # 检查错误消息
                    elif 'timeout' in error_str or 'timed out' in error_str:
                        is_timeout_error = True
                    # 检查是否是 httpcore 或 httpx 的超时错误
                    elif 'httpcore' in str(type(e).__module__) or 'httpx' in str(type(e).__module__):
                        if 'timeout' in error_type_name.lower() or 'timeout' in error_str:
                            is_timeout_error = True
                    
                    if is_timeout_error:
                        # 超时错误：只记录简单信息，不输出完整堆栈
                        logger.warning(f"⏰ 工作流执行超时 (request_id={request_id}): {error_type_name} - {str(e)}")
                    else:
                        # 其他错误：正常记录完整堆栈
                        logger.error(f"❌ 工作流执行出错 (request_id={request_id}): {e}", exc_info=True)
                finally:
                    execution_done.set()
                    # 发送结束标记到该请求的队列
                    _push_to_request_queue(request_id, None)
            
            # 启动后台线程执行工作流
            graph_thread = threading.Thread(target=run_graph, daemon=True)
            graph_thread.start()
            
            # 实时从该请求的队列中读取并 yield token
            consumer_disconnected = False
            while True:
                try:
                    # 从该请求的队列中获取 token（超时0.1秒，避免阻塞太久）
                    chunk = request_queue.get(timeout=0.1)
                    
                    # None 表示结束
                    if chunk is None:
                        break
                    
                    # 实时 yield token（捕获连接断开异常）
                    try:
                        yield chunk
                    except Exception as e:
                        # 捕获 yield 异常（通常是连接断开）
                        logger.warning(f"⚠️ [DEBUG] yield 时连接断开 (request_id={request_id}): {e}")
                        consumer_disconnected = True
                        _mark_queue_disconnected(request_id)
                        break
                    
                except queue.Empty:
                    # 检查工作流是否已完成
                    if execution_done.is_set():
                        # 清空队列中剩余的内容
                        while True:
                            try:
                                chunk = request_queue.get_nowait()
                                if chunk is None:
                                    break
                                try:
                                    yield chunk
                                except Exception as e:
                                    # 捕获 yield 异常（通常是连接断开）
                                    logger.warning(f"⚠️ [DEBUG] yield 时连接断开 (request_id={request_id}): {e}")
                                    consumer_disconnected = True
                                    _mark_queue_disconnected(request_id)
                                    break
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

