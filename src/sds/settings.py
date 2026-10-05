"""Runtime settings, read from the environment.

Kept deliberately small: paths and the SEC User-Agent. Anything about *what* the
system studies (sectors, tiers) lives in config/sectors.yaml instead.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    user_agent: str

    @property
    def db_path(self) -> Path:
        return self.data_dir / "sds.duckdb"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @classmethod
    def from_env(cls) -> Settings:
        data_dir = Path(os.environ.get("SDS_DATA_DIR", "./data")).resolve()
        user_agent = os.environ.get("SDS_USER_AGENT", "").strip()
        return cls(data_dir=data_dir, user_agent=user_agent)

    def require_user_agent(self) -> str:
        """SEC blocks requests without a declared User-Agent; fail early and clearly."""
        if "@" not in self.user_agent:
            raise RuntimeError(
                "Set SDS_USER_AGENT to '<app name> <contact email>' "
                "(SEC fair-access policy). See .env.example."
            )
        return self.user_agent
