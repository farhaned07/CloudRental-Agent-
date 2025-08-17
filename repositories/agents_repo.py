import os
import time
from typing import Dict, Any, Optional

import gspread
from google.oauth2.service_account import Credentials


_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


class AgentsRepository:
    def __init__(self):
        self.sheet_id = os.getenv("GOOGLE_SHEETS_DOCUMENT_ID")
        if not self.sheet_id:
            raise RuntimeError("GOOGLE_SHEETS_DOCUMENT_ID is required")
        self._cache = None
        self._cache_ts = 0.0
        self._cache_ttl = 60.0

    def _client(self):
        creds = Credentials.from_service_account_file(
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=_SCOPES
        )
        return gspread.authorize(creds)

    def _read_all(self):
        now = time.time()
        if self._cache is not None and now - self._cache_ts < self._cache_ttl:
            return self._cache
        gc = self._client()
        sh = gc.open_by_key(self.sheet_id)
        ws = sh.worksheet("agents")
        rows = ws.get_all_records()
        self._cache = rows
        self._cache_ts = now
        return rows

    def get_by_id(self, agent_id: str) -> Optional[Dict[str, Any]]:
        for r in self._read_all():
            if str(r.get("agent_id")) == str(agent_id):
                return r
        return None

