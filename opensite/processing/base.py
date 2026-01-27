import logging
from pathlib import Path
from opensite.logging.base import LoggingBase
class ProcessBase:
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        self.node = node
        self.log = LoggingBase("ProcessBase", log_level, shared_lock)
        self.base_path = ""
        self.shared_metadata = shared_metadata if shared_metadata is not None else {}

    def set_output_variable(self, value: str, global_urn: int = None):
        """
        Publishes a value to the shared metadata registry.
        Defaults to the current node's global_urn if none is provided.
        """
        target_urn = global_urn if global_urn is not None else self.node.global_urn
        var_key = f"VAR:global_output_{target_urn}"
        
        self.shared_metadata[var_key] = value

    def get_output_variable(self, var_name: str) -> str:
        """
        Retrieves a value from the shared metadata registry.
        var_name should be the full string: 'VAR:global_output_76'
        """
        return self.shared_metadata.get(var_name)

    def run(self):
        """Main entry point for the process."""
        raise NotImplementedError("Subclasses must implement run()")

    def ensure_output_dir(self, file_path):
        """Utility to make sure the destination exists."""
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    def get_full_path(self, path_str: str) -> Path:
        """Helper to resolve paths against the base_path."""
        path = Path(path_str)
        if not path.is_absolute() and self.base_path:
            return (Path(self.base_path) / path).resolve()
        return path.resolve()