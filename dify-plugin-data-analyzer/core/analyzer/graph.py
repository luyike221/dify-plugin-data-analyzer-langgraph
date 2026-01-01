"""
LangGraph Data Analysis Workflow

基于 LangGraph 1.0.0+ 实现的数据分析工作流图
支持：代码生成 → 执行 → 错误修复 → 报告生成
"""

import re
import logging
from typing import Dict, Any, List, Optional, Generator, Literal

from langgraph.graph import StateGraph, START, END

from .state import AnalysisState, AnalysisPhase, CodeExecution, create_initial_state, AnalysisResult
from .prompts import PromptTemplates

# 配置日志
logger = logging.getLogger(__name__)


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
) -> str:
    """调用 LLM 并返回响应内容"""
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
    )
    return response.choices[0].message.content


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

def generate_code_node(state: AnalysisState) -> Dict[str, Any]:
    """
    代码生成节点
    
    根据用户需求和数据信息，调用 LLM 生成 Python 分析代码
    """
    logger.info("📝 [Node] 代码生成节点开始执行")
    
    # 创建 LLM 客户端
    client = create_llm_client(state["api_url"], state.get("api_key"))
    
    # 构建 prompt
    messages = PromptTemplates.format_code_generation_prompt(
        csv_path=state["csv_path"],
        row_count=state["row_count"],
        column_names=state["column_names"],
        column_metadata=state["column_metadata"],
        data_preview=state["data_preview"],
        user_prompt=state["user_prompt"],
    )
    
    # 调用 LLM
    response = call_llm(
        client=client,
        messages=messages,
        model=state["model"],
        temperature=state["temperature"],
    )
    
    # 提取代码
    code = extract_python_code(response)
    
    if code:
        logger.info(f"✅ [Node] 成功生成代码，长度: {len(code)} 字符")
        return {
            "phase": AnalysisPhase.CODE_EXECUTION.value,
            "current_code": code,
            "code_history": [code],
            "messages": messages + [{"role": "assistant", "content": response}],
            "stream_output": [f"\n📝 **生成的分析代码：**\n\n```python\n{code}\n```\n\n"],
        }
    else:
        logger.warning("⚠️ [Node] 未能从 LLM 响应中提取代码")
        return {
            "phase": AnalysisPhase.REPORT_GENERATION.value,
            "current_output": response,
            "messages": messages + [{"role": "assistant", "content": response}],
            "stream_output": [f"\n⚠️ 未生成代码，LLM 直接返回：\n\n{response}\n\n"],
        }


def execute_code_node(state: AnalysisState) -> Dict[str, Any]:
    """
    代码执行节点
    
    在本地安全环境中执行生成的 Python 代码
    """
    logger.info("▶️ [Node] 代码执行节点开始执行")
    
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
        return {
            "phase": AnalysisPhase.REPORT_GENERATION.value,
            "current_output": output,
            "execution_success": True,
            "execution_history": [execution],
            "round_count": state.get("round_count", 0) + 1,
            "stream_output": [f"\n✅ **执行结果：**\n\n```\n{output}\n```\n\n"],
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
    
    # 调用 LLM 修复
    response = call_llm(
        client=client,
        messages=messages,
        model=state["model"],
        temperature=state["temperature"],
    )
    
    # 提取修复后的代码
    fixed_code = extract_python_code(response)
    
    if fixed_code:
        logger.info(f"✅ [Node] 成功获取修复代码，重试次数: {retry_count}")
        return {
            "phase": AnalysisPhase.CODE_EXECUTION.value,
            "current_code": fixed_code,
            "code_history": [fixed_code],
            "retry_count": retry_count,
            "stream_output": [f"\n🔧 **修复后的代码（尝试 {retry_count}/{max_retries}）：**\n\n```python\n{fixed_code}\n```\n\n"],
        }
    else:
        logger.warning("⚠️ [Node] 未能从修复响应中提取代码")
        return {
            "phase": AnalysisPhase.REPORT_GENERATION.value,
            "retry_count": retry_count,
            "stream_output": [f"\n⚠️ 无法修复代码，跳过执行，直接生成报告\n\n"],
        }


def generate_report_node(state: AnalysisState) -> Dict[str, Any]:
    """
    报告生成节点
    
    根据代码执行结果，调用 LLM 生成分析报告
    """
    logger.info("📄 [Node] 报告生成节点开始执行")
    
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
    
    # 构建报告 prompt
    messages = PromptTemplates.format_report_generation_prompt(
        user_prompt=state["user_prompt"],
        code=code,
        execution_output=output,
    )
    
    # 调用 LLM 生成报告
    report = call_llm(
        client=client,
        messages=messages,
        model=state["model"],
        temperature=state["temperature"],
    )
    
    logger.info(f"✅ [Node] 成功生成报告，长度: {len(report)} 字符")
    
    return {
        "phase": AnalysisPhase.COMPLETED.value,
        "report": report,
        "stream_output": [f"\n📊 **数据分析报告：**\n\n{report}\n"],
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
    
    START → generate_code → execute_code ─┬─(成功)─→ generate_report → END
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
    workflow.add_node("generate_code", generate_code_node)
    workflow.add_node("execute_code", execute_code_node)
    workflow.add_node("fix_code", fix_code_node)
    workflow.add_node("generate_report", generate_report_node)
    
    # 添加边
    # START → generate_code
    workflow.add_edge(START, "generate_code")
    
    # generate_code → execute_code (如果生成了代码)
    workflow.add_conditional_edges(
        "generate_code",
        lambda s: "execute_code" if s.get("phase") == AnalysisPhase.CODE_EXECUTION.value else "generate_report",
        {
            "execute_code": "execute_code",
            "generate_report": "generate_report",
        }
    )
    
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
    ) -> Generator[str, None, AnalysisResult]:
        """
        执行数据分析（流式输出）
        
        Yields:
            str: 流式输出的字符串块
            
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
        
        # 使用 stream 模式执行
        final_state = None
        for state in self._graph.stream(initial_state):
            # state 是 {node_name: node_output} 的字典
            for node_name, node_output in state.items():
                # 输出流式内容
                if "stream_output" in node_output:
                    for chunk in node_output["stream_output"]:
                        yield chunk
                
                # 更新最终状态
                final_state = node_output
        
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

