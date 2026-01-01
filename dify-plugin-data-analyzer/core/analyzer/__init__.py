"""
LangGraph-based Data Analyzer Module

基于 LangGraph 1.0.0+ 的数据分析模块，支持：
- 代码生成 → 执行 → 报告生成的完整工作流
- 错误自动修复和重试
- 流式输出
- 状态持久化
"""

from .state import AnalysisState, AnalysisResult
from .graph import create_analysis_graph, DataAnalysisGraph
from .prompts import PromptTemplates
from .api import (
    run_langgraph_analysis,
    run_langgraph_analysis_stream,
    analyze_excel_with_langgraph,
)

__all__ = [
    # 状态和类型
    "AnalysisState",
    "AnalysisResult", 
    # 工作流图
    "create_analysis_graph",
    "DataAnalysisGraph",
    # Prompt 模板
    "PromptTemplates",
    # API 函数
    "run_langgraph_analysis",
    "run_langgraph_analysis_stream",
    "analyze_excel_with_langgraph",
]

