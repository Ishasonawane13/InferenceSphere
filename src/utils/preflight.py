"""Hardware and storage checks for expensive Spark workloads."""

from __future__ import annotations

import ctypes
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from src.config import PreflightConfig


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SystemResources:
    memory_gb: float
    cpu_cores: int
    free_disk_gb: float


def _memory_gb() -> float:
    if os.name == "nt":
        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("length", ctypes.c_ulong),
                ("memory_load", ctypes.c_ulong),
                ("total", ctypes.c_ulonglong),
                ("available", ctypes.c_ulonglong),
                ("total_page_file", ctypes.c_ulonglong),
                ("available_page_file", ctypes.c_ulonglong),
                ("total_virtual", ctypes.c_ulonglong),
                ("available_virtual", ctypes.c_ulonglong),
                ("available_extended", ctypes.c_ulonglong),
            ]

        status = MemoryStatus()
        status.length = ctypes.sizeof(MemoryStatus)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            raise OSError("Unable to read system memory on Windows")
        return status.available / 1024**3

    if hasattr(os, "sysconf"):
        page_size = os.sysconf("SC_PAGE_SIZE")
        available_pages = os.sysconf("SC_AVPHYS_PAGES")
        return page_size * available_pages / 1024**3
    raise OSError("Unable to read available system memory on this platform")


def get_system_resources(path: Path) -> SystemResources:
    """Read available memory, CPU cores, and free space for a target path."""
    usage = shutil.disk_usage(path)
    return SystemResources(
        memory_gb=_memory_gb(),
        cpu_cores=os.cpu_count() or 1,
        free_disk_gb=usage.free / 1024**3,
    )


def check_preflight(
    config: PreflightConfig,
    path: Path,
) -> SystemResources:
    """Log warnings when available resources are below configured thresholds."""
    resources = get_system_resources(path)
    if resources.memory_gb < config.minimum_memory_gb:
        logger.warning(
            "Available memory %.2f GB is below the %.2f GB threshold.",
            resources.memory_gb,
            config.minimum_memory_gb,
        )
    if resources.cpu_cores < config.minimum_cpu_cores:
        logger.warning(
            "Available CPU cores %d are below the %d-core threshold.",
            resources.cpu_cores,
            config.minimum_cpu_cores,
        )
    if resources.free_disk_gb < config.minimum_free_disk_gb:
        logger.warning(
            "Free disk space %.2f GB is below the %.2f GB threshold.",
            resources.free_disk_gb,
            config.minimum_free_disk_gb,
        )
    return resources
