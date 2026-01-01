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
    CODE_GENERATION = "code_generation"
    CODE_EXECUTION = "code_execution"
    ERROR_FIXING = "error_fixing"
    REPORT_GENERATION = "report_generation"
    COMPLETED = "completed"
    FAILED = "failed"


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
        
        # === 输出 ===
        report: 最终报告
        generated_files: 生成的文件列表
        stream_output: 流式输出内容
    """
    
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
    
    # === 工作流状态 ===
    phase: str
    current_code: str
    current_output: str
    execution_success: bool
    error_message: Optional[str]
    
    # === 历史记录 ===
    code_history: Annotated[List[str], operator.add]
    execution_history: Annotated[List[CodeExecution], operator.add]
    messages: Annotated[List[Dict[str, str]], operator.add]
    
    # === 计数器 ===
    retry_count: int
    round_count: int
    
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
        
    Returns:
        初始化的 AnalysisState
    """
    return AnalysisState(
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
        
        # 工作流状态
        phase=AnalysisPhase.INIT.value,
        current_code="",
        current_output="",
        execution_success=False,
        error_message=None,
        
        # 历史记录
        code_history=[],
        execution_history=[],
        messages=[],
        
        # 计数器
        retry_count=0,
        round_count=0,
        
        # 输出
        report="",
        generated_files=[],
        stream_output=[],
    )

