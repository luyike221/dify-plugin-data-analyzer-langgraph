"""
LangGraph State Definitions

定义数据分析工作流的状态类型，使用 TypedDict 和 Annotated 
实现 LangGraph 1.0.0+ 的状态管理
"""

from typing import TypedDict, Annotated, List, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum
import operator


class AnalysisPhase(str, Enum):
    """分析阶段枚举"""
    INIT = "init"
    INTENT_ANALYSIS = "intent_analysis"  # 意图识别和策略制定
    CODE_GENERATION = "code_generation"
    CODE_EXECUTION = "code_execution"
    ERROR_FIXING = "error_fixing"
    EVALUATE_COMPLETENESS = "evaluate_completeness"  # 评估分析完整性（新增）
    REPORT_GENERATION = "report_generation"
    COMPLETED = "completed"
    FAILED = "failed"
    USER_CLARIFICATION_NEEDED = "user_clarification_needed"  # 需要用户澄清


@dataclass
class CodeExecution:
    """代码执行记录"""
    code: str
    output: str
    success: bool
    error_message: Optional[str] = None
    attempt: int = 1


@dataclass
class AnalysisResult:
    """分析结果"""
    success: bool
    report: str = ""
    code_history: List[str] = field(default_factory=list)
    execution_outputs: List[str] = field(default_factory=list)
    generated_files: List[Dict[str, str]] = field(default_factory=list)
    error_message: Optional[str] = None
    total_rounds: int = 0


class AnalysisState(TypedDict, total=False):
    """
    LangGraph 分析工作流状态
    
    使用 TypedDict 定义状态结构，支持 LangGraph 的状态管理和持久化
    
    Attributes:
        # === 请求标识 ===
        request_id: 请求唯一标识（用于多线程隔离，每个请求有独立的队列）
        
        # === 输入数据 ===
        workspace_dir: 工作空间目录
        thread_id: 会话ID
        csv_path: CSV文件路径
        column_names: 列名列表
        column_metadata: 列元数据
        row_count: 数据行数
        data_preview: 数据预览字符串
        user_prompt: 用户分析需求
        
        # === LLM 配置 ===
        api_url: LLM API 地址
        api_key: LLM API 密钥
        model: 模型名称
        temperature: 生成温度
        debug_print_execution_output: 是否在流式输出中打印代码执行结果（用于调试）
        
        # === 工作流状态 ===
        phase: 当前阶段
        current_code: 当前生成的代码
        current_output: 当前执行输出
        execution_success: 执行是否成功
        error_message: 错误信息
        
        # === 历史记录（使用 Annotated 实现追加） ===
        code_history: 代码历史
        execution_history: 执行历史
        messages: LLM 对话消息历史
        
        # === 计数器 ===
        retry_count: 重试次数
        round_count: 分析轮次
        
        # === 多轮分析相关 ===
        max_analysis_rounds: 最大分析轮数（防止无限循环）
        completed_directions: 已完成的分析方向列表
        next_analysis_direction: 下一轮分析方向
        need_more_analysis: 是否需要更多分析
        all_execution_outputs: 所有轮次的执行结果
        
        # === 输出 ===
        report: 最终报告
        generated_files: 生成的文件列表
        stream_output: 流式输出内容
    """
    
    # === 请求标识（多线程隔离） ===
    request_id: str  # 请求唯一标识，用于获取该请求的独立队列
    
    # === 输入数据 ===
    workspace_dir: str
    thread_id: str
    csv_path: str
    column_names: List[str]
    column_metadata: Dict[str, Any]
    row_count: int
    data_preview: str
    user_prompt: str
    
    # === LLM 配置 ===
    api_url: str
    api_key: Optional[str]
    model: str
    temperature: float
    debug_print_execution_output: bool  # 是否在流式输出中打印代码执行结果（用于调试）
    
    # === 工作流状态 ===
    phase: str
    current_code: str
    current_output: str
    execution_success: bool
    error_message: Optional[str]
    
    # === 意图分析结果 ===
    refined_prompt: str  # 重写后的用户输入
    analysis_type: str  # 分析类型：simple/overview/specific
    analysis_tasks: List[str]  # 分析任务列表
    current_task: str  # 当前轮次要完成的任务
    completed_tasks: Annotated[List[str], operator.add]  # 已完成的任务列表
    intent_analysis_result: str  # 意图分析结果（JSON格式）
    needs_clarification: bool  # 是否需要用户澄清
    clarification_message: Optional[str]  # 澄清消息
    
    # 兼容旧字段（保留但不推荐使用）
    analysis_strategy: str  # 分析策略（旧）
    research_directions: List[str]  # 研究方向列表（旧）
    
    # === 历史记录 ===
    code_history: Annotated[List[str], operator.add]
    execution_history: Annotated[List[CodeExecution], operator.add]
    messages: Annotated[List[Dict[str, str]], operator.add]
    
    # === 计数器 ===
    retry_count: int
    round_count: int
    
    # === 多轮分析相关 ===
    max_analysis_rounds: int  # 最大分析轮数（防止无限循环），默认3
    completed_directions: Annotated[List[str], operator.add]  # 已完成的分析方向
    next_analysis_direction: str  # 下一轮分析方向
    need_more_analysis: bool  # 是否需要更多分析
    all_execution_outputs: Annotated[List[str], operator.add]  # 所有轮次的执行结果
    
    # === 输出 ===
    report: str
    generated_files: Annotated[List[Dict[str, str]], operator.add]
    stream_output: Annotated[List[str], operator.add]


def create_initial_state(
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
    request_id: Optional[str] = None,  # 新增：请求唯一标识
    debug_print_execution_output: bool = False,  # 是否在流式输出中打印代码执行结果
    max_analysis_rounds: int = 3,  # 最大分析轮数，防止无限循环
) -> AnalysisState:
    """
    创建初始分析状态
    
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
        request_id: 请求唯一标识（用于多线程隔离）
        debug_print_execution_output: 是否在流式输出中打印代码执行结果
        max_analysis_rounds: 最大分析轮数（默认3轮，防止无限循环）
        
    Returns:
        初始化的 AnalysisState
    """
    import uuid
    
    # 如果没有提供 request_id，生成一个唯一的
    if request_id is None:
        request_id = f"req-{uuid.uuid4().hex[:16]}"
    
    return AnalysisState(
        # 请求标识
        request_id=request_id,
        
        # 输入数据
        workspace_dir=workspace_dir,
        thread_id=thread_id,
        csv_path=csv_path,
        column_names=column_names,
        column_metadata=column_metadata,
        row_count=row_count,
        data_preview=data_preview,
        user_prompt=user_prompt,
        
        # LLM 配置
        api_url=api_url,
        api_key=api_key,
        model=model,
        temperature=temperature,
        debug_print_execution_output=debug_print_execution_output,
        
        # 工作流状态
        phase=AnalysisPhase.INIT.value,
        current_code="",
        current_output="",
        execution_success=False,
        error_message=None,
        
        # 意图分析结果（初始为空）
        refined_prompt="",
        analysis_type="",  # simple/overview/specific
        analysis_tasks=[],
        current_task="",
        completed_tasks=[],
        intent_analysis_result="",
        needs_clarification=False,
        clarification_message=None,
        
        # 兼容旧字段
        analysis_strategy="",
        research_directions=[],
        
        # 历史记录
        code_history=[],
        execution_history=[],
        messages=[],
        
        # 计数器
        retry_count=0,
        round_count=0,
        
        # 多轮分析相关
        max_analysis_rounds=max_analysis_rounds,
        completed_directions=[],
        next_analysis_direction="",
        need_more_analysis=False,
        all_execution_outputs=[],
        
        # 输出
        report="",
        generated_files=[],
        stream_output=[],
    )

