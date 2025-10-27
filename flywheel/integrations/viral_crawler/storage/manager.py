
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Dict
from ..core.models import Video

class ContentManager:
    def __init__(self, out_dir: Path, logger):
        self.out_dir = Path(out_dir)
        self.logger = logger
        (self.out_dir / "logs").mkdir(parents=True, exist_ok=True)

    def report(self) -> Dict:
        report = {
            "generated_at": datetime.now().isoformat(),
            "total_videos": 0,
            "platforms": {},
            "licenses": {},
        }
        for platform in ["youtube", "reddit", "tiktok", "instagram"]:
            p = self.out_dir / platform
            count = len(list(p.glob("*.mp4"))) if p.exists() else 0
            report["platforms"][platform] = count
            report["total_videos"] += count

        attr = self.out_dir / "ATTRIBUTION.txt"
        if attr.exists():
            report["attribution_lines"] = [line.strip() for line in attr.read_text(encoding="utf-8").splitlines() if line.strip()]
        else:
            report["attribution_lines"] = []

        path = self.out_dir / "content_report.json"
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        self.logger.info("Report written: %s", path)
        return report
