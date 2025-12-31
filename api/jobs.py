"""In-memory tracking for background scrape jobs."""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List

from utils.tools import ScrapeProgress, start_background_scrape


@dataclass
class Job:
    id: str
    urls: List[str]
    progress: ScrapeProgress

    def snapshot(self) -> Dict:
        now = time.time()
        current_elapsed = 0
        if self.progress.current_url and self.progress.current_started_at:
            current_elapsed = int(max(0, now - self.progress.current_started_at))
        total_elapsed = 0
        if self.progress.started_at:
            total_elapsed = int(max(0, (self.progress.finished_at or now) - self.progress.started_at))

        return {
            "job_id": self.id,
            "done": self.progress.done,
            "progress_percent": self.progress.progress_percent,
            "results": self.progress.results,
            "errors": self.progress.errors,
            "current_url": self.progress.current_url,
            "current_elapsed_seconds": current_elapsed,
            "total_elapsed_seconds": total_elapsed,
            "started_at": self.progress.started_at,
            "finished_at": self.progress.finished_at,
            "url_timings": self.progress.url_timings,
            "total_urls": len(self.urls),
            "completed_urls": len(self.progress.results),
        }


class JobStore:
    def __init__(self) -> None:
        self._jobs: Dict[str, Job] = {}
        self._lock = threading.Lock()

    def create(self, urls: List[str]) -> Job:
        job_id = uuid.uuid4().hex
        progress = start_background_scrape(urls)
        job = Job(id=job_id, urls=urls, progress=progress)
        with self._lock:
            self._jobs[job_id] = job
        return job

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)


job_store = JobStore()
