import os
import re
import logging
import json
from typing import Optional

try:
    import fcntl
except ImportError:
    # Fallback for non-Unix systems if needed, but user is on Mac
    fcntl = None

logger = logging.getLogger(__name__)

class StateStore:
    """Interface for a key-value state store."""
    def read(self, key: str) -> Optional[str]:
        raise NotImplementedError

    def update(self, key: str, data: str):
        raise NotImplementedError

    def clear(self, key: str):
        raise NotImplementedError

    def close(self):
        pass

class FileStateStore(StateStore):
    """Implementation of StateStore that uses the local filesystem.
    Each key is stored in a separate file.
    """
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)

    def _get_file_path(self, key: str) -> str:
        # Basic sanitization for key to be a filename
        sanitized_key = re.sub(r'[^a-zA-Z0-9.-]', '_', key)
        return os.path.join(self.base_dir, sanitized_key + ".json")

    def read(self, key: str) -> Optional[str]:
        path = self._get_file_path(key)
        if not os.path.exists(path):
            return None

        try:
            with open(path, 'r', encoding='utf-8') as f:
                if fcntl:
                    # Acquire shared lock
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    return f.read()
                finally:
                    if fcntl:
                        # Release lock
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception as e:
            logger.error(f"Failed to read state for key: {key}", exc_info=True)
            raise e

    def update(self, key: str, data: str):
        path = self._get_file_path(key)
        try:
            with open(path, 'w', encoding='utf-8') as f:
                if fcntl:
                    # Acquire exclusive lock
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    f.write(data)
                finally:
                    if fcntl:
                        # Release lock
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception as e:
            logger.error(f"Failed to update state for key: {key}", exc_info=True)
            raise e

    def clear(self, key: str):
        path = self._get_file_path(key)
        if not os.path.exists(path):
            return

        try:
            # To ensure safety, try to open and lock before deleting
            # Note: Files.delete in Java doesn't lock, but the Java code tried to acquire a lock before deleting.
            # In Python, we can do the same.
            if fcntl:
                with open(path, 'w') as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    # Just acquiring the lock is enough to know it's safe to delete.
            
            os.remove(path)
        except FileNotFoundError:
            pass # Already deleted
        except Exception as e:
            logger.error(f"Failed to clear state for key: {key}", exc_info=True)
            raise e

class StateStoreProvider:
    """Interface for a state store provider."""
    def get_state_store(self, base_dir: str) -> StateStore:
        raise NotImplementedError

class DefaultStateStoreProvider(StateStoreProvider):
    """Default implementation that provides a FileStateStore."""
    def get_state_store(self, base_dir: str) -> StateStore:
        return FileStateStore(base_dir)

