"""Base types and utilities for visualization."""
from typing import Protocol, List
from dataclasses import dataclass
from enum import Enum

class WorkerTier(Enum):
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    LARGE = "LARGE"

class ThreadSimulator(Protocol):
    thread_id: int
    processed_items: List[any]
    available_time: float

class Worker(Protocol):
    worker_id: int
    tier: WorkerTier
    start_time: float
    completion_time: float
    threads: List[ThreadSimulator]
    file: any  # FileMetadata 