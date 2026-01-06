#!/bin/bash

# 尝试激活 conda 环境（如果 conda 可用）
if command -v conda &> /dev/null; then
    # 初始化 conda（如果需要）
    if [ -z "$CONDA_DEFAULT_ENV" ]; then
        # 尝试找到 conda 的初始化脚本
        if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
            source "$HOME/miniconda3/etc/profile.d/conda.sh"
        elif [ -f "$HOME/anaconda3/etc/profile.d/conda.sh" ]; then
            source "$HOME/anaconda3/etc/profile.d/conda.sh"
        elif [ -f "/opt/conda/etc/profile.d/conda.sh" ]; then
            source "/opt/conda/etc/profile.d/conda.sh"
        fi
    fi
    
    # 尝试激活环境
    if conda env list | grep -q "analyze_dify_langgrpah"; then
        conda activate analyze_dify_langgrpah 2>/dev/null || {
            echo "⚠️  无法激活 conda 环境，尝试直接运行..."
        }
    fi
fi

# 查找 python 可执行文件
if command -v python3 &> /dev/null; then
    PYTHON_CMD=python3
elif command -v python &> /dev/null; then
    PYTHON_CMD=python
else
    echo "❌ 错误: 找不到 python 命令"
    echo "请确保已安装 Python 或激活正确的 conda 环境"
    exit 1
fi

# 运行主程序
echo "🚀 使用 $PYTHON_CMD 运行插件..."
$PYTHON_CMD -m main