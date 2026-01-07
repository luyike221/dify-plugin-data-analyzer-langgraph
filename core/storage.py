"""
Storage layer for DeepAnalyze API Server
Handles in-memory storage for OpenAI objects
"""

import os
import time
import uuid
import shutil
import threading
from pathlib import Path
from typing import List, Optional, Dict, Any

from .models import (
    FileObject, ThreadObject, MessageObject
)
from .utils import get_thread_workspace, uniquify_path


class Storage:
    """Simple in-memory storage for OpenAI objects"""

    def __init__(self):
        self.files: Dict[str, Dict[str, Any]] = {}
        self.threads: Dict[str, Dict[str, Any]] = {}
        self.messages: Dict[str, List[Dict[str, Any]]] = {}  # thread_id -> messages
        self._lock = threading.Lock()

    def create_file(self, filename: str, filepath: str, purpose: str) -> FileObject:
        """Create a file record"""
        with self._lock:
            file_id = f"file-{uuid.uuid4().hex[:24]}"
            file_size = os.path.getsize(filepath)
            file_obj = {
                "id": file_id,
                "object": "file",
                "bytes": file_size,
                "created_at": int(time.time()),
                "filename": filename,
                "purpose": purpose,
                "filepath": filepath,
            }
            self.files[file_id] = file_obj
            return FileObject(**file_obj)

    def get_file(self, file_id: str) -> Optional[FileObject]:
        """Get a file record"""
        with self._lock:
            if file_id in self.files:
                return FileObject(**self.files[file_id])
            return None

    def delete_file(self, file_id: str) -> bool:
        """Delete a file record
        
        优化：将文件删除操作移出锁外，避免大文件删除时长时间持有锁
        """
        # 在锁内获取文件路径并删除记录
        filepath = None
        with self._lock:
            if file_id in self.files:
                filepath = self.files[file_id].get("filepath")
                del self.files[file_id]
            else:
                return False
        
        # 在锁外执行文件删除操作（可能很慢）
        if filepath and os.path.exists(filepath):
            try:
                os.remove(filepath)
            except Exception as e:
                # 文件删除失败不影响记录删除
                print(f"Warning: Failed to delete file {filepath}: {e}")
        
        return True

    def list_files(self, purpose: Optional[str] = None) -> List[FileObject]:
        """List files with optional purpose filter"""
        with self._lock:
            files = list(self.files.values())
            if purpose:
                files = [f for f in files if f.get("purpose") == purpose]
            return [FileObject(**f) for f in files]

  
    def create_thread(
        self,
        metadata: Optional[Dict] = None,
        file_ids: Optional[List[str]] = None,
        tool_resources: Optional[Dict] = None
    ) -> ThreadObject:
        """Create a thread record
        
        优化：将文件操作移出锁外，避免大文件复制时长时间持有全局锁
        这样可以允许其他请求并发处理，不会因为一个大文件而阻塞所有请求
        """
        # 先在锁内创建thread记录（快速操作）
        with self._lock:
            thread_id = f"thread-{uuid.uuid4().hex[:24]}"
            now = int(time.time())
            thread = {
                "id": thread_id,
                "object": "thread",
                "created_at": now,
                "last_accessed_at": now,
                "metadata": metadata or {},
                "file_ids": file_ids or [],
                "tool_resources": tool_resources,
            }
            self.threads[thread_id] = thread
            self.messages[thread_id] = []
            
            # 在锁内读取文件数据（快速操作）
            files_to_copy = []
            for fid in (file_ids or []):
                if fid in self.files:
                    file_data = self.files[fid].copy()  # 复制数据，避免在锁外访问
                    files_to_copy.append(file_data)
        
        # 在锁外执行文件操作（这些操作可能很慢，特别是大文件）
        workspace_dir = get_thread_workspace(thread_id)
        os.makedirs(workspace_dir, exist_ok=True)
        os.makedirs(os.path.join(workspace_dir, "generated"), exist_ok=True)

        # 文件复制操作移到锁外，避免长时间持有锁
        for file_data in files_to_copy:
            src_path = file_data.get("filepath")
            if src_path and os.path.exists(src_path):
                dst_path = uniquify_path(Path(workspace_dir) / file_data["filename"])
                shutil.copy2(src_path, dst_path)

        return ThreadObject(**thread)

    def get_thread(self, thread_id: str) -> Optional[ThreadObject]:
        """Get a thread record"""
        with self._lock:
            if thread_id in self.threads:
                # Update last accessed time
                self.threads[thread_id]["last_accessed_at"] = int(time.time())
                return ThreadObject(**self.threads[thread_id])
            return None

    def delete_thread(self, thread_id: str) -> bool:
        """Delete a thread record
        
        优化：将工作空间删除操作移出锁外，避免大目录删除时长时间持有锁
        """
        # 在锁内删除记录
        workspace_dir = None
        with self._lock:
            if thread_id in self.threads:
                workspace_dir = get_thread_workspace(thread_id)
                del self.threads[thread_id]
                if thread_id in self.messages:
                    del self.messages[thread_id]
            else:
                return False
        
        # 在锁外执行工作空间删除操作（可能很慢，特别是包含大文件时）
        if workspace_dir and os.path.exists(workspace_dir):
            try:
                shutil.rmtree(workspace_dir)
            except Exception as e:
                # 工作空间删除失败不影响记录删除
                print(f"Warning: Failed to delete workspace {workspace_dir}: {e}")
        
        return True

    def create_message(
        self,
        thread_id: str,
        role: str,
        content: str,
        file_ids: Optional[List[str]] = None,
        metadata: Optional[Dict] = None,
    ) -> MessageObject:
        """Create a message record"""
        with self._lock:
            if thread_id not in self.threads:
                raise ValueError(f"Thread {thread_id} not found")

            message_id = f"msg-{uuid.uuid4().hex[:24]}"
            message = {
                "id": message_id,
                "object": "thread.message",
                "created_at": int(time.time()),
                "thread_id": thread_id,
                "role": role,
                "content": [{"type": "text", "text": {"value": content}}],
                "file_ids": file_ids or [],
                "assistant_id": None,
                "run_id": None,
                "metadata": metadata or {},
            }
            self.messages[thread_id].append(message)
            return MessageObject(**message)

    def list_messages(self, thread_id: str) -> List[MessageObject]:
        """List messages in a thread"""
        with self._lock:
            if thread_id not in self.messages:
                return []
            return [MessageObject(**m) for m in self.messages[thread_id]]

    
    def cleanup_expired_threads(self, timeout_hours: float = 12) -> int:
        """Clean up threads that haven't been accessed for more than timeout_hours"""
        with self._lock:
            now = int(time.time())
            timeout_seconds = int(timeout_hours * 3600)
            expired_threads = []

            for thread_id, thread_data in self.threads.items():
                last_accessed = thread_data.get("last_accessed_at", thread_data.get("created_at", 0))
                if now - last_accessed > timeout_seconds:
                    expired_threads.append(thread_id)

        cleaned_count = 0
        for thread_id in expired_threads:
            try:
                # Delete thread and its workspace
                if self.delete_thread(thread_id):
                    cleaned_count += 1
                    print(f"Cleaned up expired thread: {thread_id}")
            except Exception as e:
                print(f"Error cleaning up thread {thread_id}: {e}")

        return cleaned_count


# Global storage instance
storage = Storage()