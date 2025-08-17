import os
import time
from typing import List, Dict, Any, Optional

import gspread
from google.oauth2.service_account import Credentials


_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


class PropertiesRepository:
    def __init__(self):
        self.sheet_id = os.getenv("GOOGLE_SHEETS_DOCUMENT_ID")
        if not self.sheet_id:
            raise RuntimeError("GOOGLE_SHEETS_DOCUMENT_ID is required")
        self._cache_data = None
        self._cache_ts = 0.0
        self._cache_ttl = 30.0
        # optional: per property calendar mapping (property_id -> calendar_id)
        self._calendar_map = None

    def _client(self):
        creds = Credentials.from_service_account_file(
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=_SCOPES
        )
        return gspread.authorize(creds)

    def _read_all(self) -> List[Dict[str, Any]]:
        now = time.time()
        if self._cache_data is not None and now - self._cache_ts < self._cache_ttl:
            return self._cache_data
        gc = self._client()
        sh = gc.open_by_key(self.sheet_id)
        ws = sh.worksheet("properties")
        rows = ws.get_all_records()
        self._cache_data = rows
        self._cache_ts = now
        return rows

    def get_calendar_id(self, pid: str) -> Optional[str]:
        # Optional: add a 'calendar_id' column in properties sheet.
        prop = self.get_by_id(pid)
        if prop:
            cid = prop.get('calendar_id')
            if cid:
                return str(cid)
        return os.getenv('DEFAULT_GOOGLE_CALENDAR_ID')

    def get_by_id(self, pid: str) -> Optional[Dict[str, Any]]:
        for r in self._read_all():
            if str(r.get("id")) == str(pid):
                return r
        return None

    def search(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = [r for r in self._read_all() if str(r.get("status", "active")).lower() == "active"]
        price_min = filters.get("price_min")
        price_max = filters.get("price_max")
        bedrooms = filters.get("bedrooms")
        bathrooms = filters.get("bathrooms")
        nbh = (filters.get("neighborhood") or "").lower()
        ptype = (filters.get("property_type") or "").lower()

        def ok(r: Dict[str, Any]) -> bool:
            try:
                price = int(str(r.get("price", 0)).replace(",", ""))
            except Exception:
                price = 0
            if price_min is not None and price < int(price_min):
                return False
            if price_max is not None and price > int(price_max):
                return False
            if bedrooms is not None and str(r.get("bedrooms")) != str(bedrooms):
                return False
            if bathrooms is not None and str(r.get("bathrooms")) != str(bathrooms):
                return False
            if nbh and nbh not in str(r.get("neighborhood", "")).lower():
                return False
            if ptype and ptype not in str(r.get("type", "")).lower():
                return False
            return True

        return [r for r in rows if ok(r)]

