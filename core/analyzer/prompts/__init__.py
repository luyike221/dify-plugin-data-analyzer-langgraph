"""
Prompt Templates for Data Analysis

定义数据分析各阶段使用的 Prompt 模板

重构设计原则：
1. 任务驱动：每轮分析都有明确的"分析任务"
2. 上下文连贯：各节点共享分析上下文
3. 职责单一：每个节点只做一件事
4. 模块化：按类型/阶段拆分，方便修改提示词
"""

from typing import Dict, Any, List, Optional

# 导入各个模块的常量和函数
from .data_info import (
    DATA_INFO_TEMPLATE,
    format_data_info,
)
from .strategy_planning import (
    STRATEGY_PLANNING_SYSTEM,
    STRATEGY_PLANNING_USER,
    format_strategy_planning_prompt,
)
from .code_generation import (
    CODE_GENERATION_SYSTEM,
    CODE_GENERATION_USER_FIRST,
    CODE_GENERATION_USER_CONTINUE,
    format_code_generation_prompt,
)
from .code_fix import (
    CODE_FIX_SYSTEM,
    CODE_FIX_USER,
    format_code_fix_prompt,
)
from .evaluate_completeness import (
    EVALUATE_COMPLETENESS_SYSTEM,
    EVALUATE_COMPLETENESS_USER,
    format_evaluate_completeness_prompt,
)
from .report_generation import (
    REPORT_GENERATION_SYSTEM,
    REPORT_GENERATION_USER,
    format_report_generation_prompt,
)
from .continue_analysis import (
    CONTINUE_ANALYSIS_SYSTEM,
    CONTINUE_ANALYSIS_USER,
    format_continue_analysis_prompt,
)


class PromptTemplates:
    """Prompt 模板管理类（保持向后兼容）"""
    
    # ========================================
    # 共享的数据信息模板（供各节点复用）
    # ========================================
    # 注意：这里直接引用导入的常量，避免重复定义
    DATA_INFO_TEMPLATE = DATA_INFO_TEMPLATE  # type: ignore
    
    # ========================================
    # 1. 策略制定 Prompt
    # ========================================
    STRATEGY_PLANNING_SYSTEM = STRATEGY_PLANNING_SYSTEM
    STRATEGY_PLANNING_USER = STRATEGY_PLANNING_USER
    
    # ========================================
    # 2. 代码生成 Prompt
    # ========================================
    CODE_GENERATION_SYSTEM = CODE_GENERATION_SYSTEM
    CODE_GENERATION_USER_FIRST = CODE_GENERATION_USER_FIRST
    CODE_GENERATION_USER_CONTINUE = CODE_GENERATION_USER_CONTINUE
    
    # ========================================
    # 3. 代码修复 Prompt
    # ========================================
    CODE_FIX_SYSTEM = CODE_FIX_SYSTEM
    CODE_FIX_USER = CODE_FIX_USER
    
    # ========================================
    # 4. 分析评估 Prompt
    # ========================================
    EVALUATE_COMPLETENESS_SYSTEM = EVALUATE_COMPLETENESS_SYSTEM
    EVALUATE_COMPLETENESS_USER = EVALUATE_COMPLETENESS_USER
    
    # ========================================
    # 5. 报告生成 Prompt
    # ========================================
    REPORT_GENERATION_SYSTEM = REPORT_GENERATION_SYSTEM
    REPORT_GENERATION_USER = REPORT_GENERATION_USER
    
    # ========================================
    # 6. 继续分析 Prompt
    # ========================================
    CONTINUE_ANALYSIS_SYSTEM = CONTINUE_ANALYSIS_SYSTEM
    CONTINUE_ANALYSIS_USER = CONTINUE_ANALYSIS_USER
    
    # ========================================
    # 格式化函数（保持向后兼容）
    # ========================================
    
    @classmethod
    def _format_data_info(
        cls,
        csv_path: str,
        row_count: int,
        column_names: List[str],
        column_metadata: Dict[str, Any],
        data_preview: str,
    ) -> str:
        """格式化数据信息（供各节点复用）"""
        return format_data_info(
            csv_path, row_count, column_names, column_metadata, data_preview
        )
    
    @classmethod
    def format_strategy_planning_prompt(
        cls,
        csv_path: str,
        row_count: int,
        column_names: List[str],
        column_metadata: Dict[str, Any],
        data_preview: str,
        user_prompt: str,
    ) -> List[Dict[str, str]]:
        """格式化策略制定 Prompt"""
        return format_strategy_planning_prompt(
            csv_path, row_count, column_names, column_metadata, data_preview, user_prompt
        )
    
    @classmethod
    def format_code_generation_prompt(
        cls,
        csv_path: str,
        row_count: int,
        column_names: List[str],
        column_metadata: Dict[str, Any],
        data_preview: str,
        user_prompt: str,
        previous_results: Optional[str] = None,
        is_first_round: bool = True,
    ) -> List[Dict[str, str]]:
        """格式化代码生成 Prompt"""
        return format_code_generation_prompt(
            csv_path, row_count, column_names, column_metadata, data_preview,
            user_prompt, previous_results, is_first_round
        )
    
    @classmethod
    def format_code_fix_prompt(
        cls,
        original_code: str,
        error_message: str,
        csv_path: str,
        column_names: List[str],
    ) -> List[Dict[str, str]]:
        """格式化代码修复 Prompt"""
        return format_code_fix_prompt(
            original_code, error_message, csv_path, column_names
        )
    
    @classmethod
    def format_evaluate_completeness_prompt(
        cls,
        user_prompt: str,
        analysis_tasks: List[str],
        current_output: str,
        previous_outputs: List[str],
        completed_tasks: List[str],
        current_round: int,
        max_rounds: int,
    ) -> List[Dict[str, str]]:
        """格式化分析评估 Prompt"""
        return format_evaluate_completeness_prompt(
            user_prompt, analysis_tasks, current_output, previous_outputs,
            completed_tasks, current_round, max_rounds
        )
    
    @classmethod
    def format_report_generation_prompt(
        cls,
        user_prompt: str,
        analysis_type: str,
        total_rounds: int,
        all_results: str,
        column_names: List[str] = None,
        column_metadata: Dict[str, Any] = None,
    ) -> List[Dict[str, str]]:
        """格式化报告生成 Prompt"""
        return format_report_generation_prompt(
            user_prompt, analysis_type, total_rounds, all_results,
            column_names, column_metadata
        )
    
    @classmethod
    def format_continue_analysis_prompt(
        cls,
        execution_output: str,
        user_prompt: str,
    ) -> List[Dict[str, str]]:
        """格式化继续分析 Prompt"""
        return format_continue_analysis_prompt(
            execution_output, user_prompt
        )


# 导出所有内容，方便直接使用
__all__ = [
    "PromptTemplates",
    # 数据信息
    "DATA_INFO_TEMPLATE",
    "format_data_info",
    # 策略制定
    "STRATEGY_PLANNING_SYSTEM",
    "STRATEGY_PLANNING_USER",
    "format_strategy_planning_prompt",
    # 代码生成
    "CODE_GENERATION_SYSTEM",
    "CODE_GENERATION_USER_FIRST",
    "CODE_GENERATION_USER_CONTINUE",
    "format_code_generation_prompt",
    # 代码修复
    "CODE_FIX_SYSTEM",
    "CODE_FIX_USER",
    "format_code_fix_prompt",
    # 分析评估
    "EVALUATE_COMPLETENESS_SYSTEM",
    "EVALUATE_COMPLETENESS_USER",
    "format_evaluate_completeness_prompt",
    # 报告生成
    "REPORT_GENERATION_SYSTEM",
    "REPORT_GENERATION_USER",
    "format_report_generation_prompt",
    # 继续分析
    "CONTINUE_ANALYSIS_SYSTEM",
    "CONTINUE_ANALYSIS_USER",
    "format_continue_analysis_prompt",
]

