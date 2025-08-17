import os
import time
import uuid
from typing import List, Dict, Any

import gspread
from google.oauth2.service_account import Credentials


_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


class BookingsRepository:
    def __init__(self):
        self.sheet_id = os.getenv("GOOGLE_SHEETS_DOCUMENT_ID")
        if not self.sheet_id:
            raise RuntimeError("GOOGLE_SHEETS_DOCUMENT_ID is required")
        self._cache = None
        self._cache_ts = 0.0
        self._cache_ttl = 15.0

    def _client(self):
        creds = Credentials.from_service_account_file(
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=_SCOPES
        )
        return gspread.authorize(creds)

    def _worksheet(self):
        gc = self._client()
        sh = gc.open_by_key(self.sheet_id)
        return sh.worksheet("bookings")

    def _read_all(self) -> List[Dict[str, Any]]:
        now = time.time()
        if self._cache is not None and now - self._cache_ts < self._cache_ttl:
            return self._cache
        ws = self._worksheet()
        rows = ws.get_all_records()
        self._cache = rows
        self._cache_ts = now
        return rows

    def exists(self, property_id: str, dt_iso: str) -> bool:
        for r in self._read_all():
            if str(r.get("property_id")) == str(property_id) and str(r.get("datetime")) == str(dt_iso):
                if str(r.get("status", "requested")).lower() in ("requested", "confirmed"):
                    return True
        return False

    def create(self, user_id: str, user_display_name: str, property_id: str, dt_iso: str, notes: str) -> Dict[str, Any]:
        booking_id = str(uuid.uuid4())[:8]
        row = {
            "booking_id": booking_id,
            "user_id": user_id,
            "user_display_name": user_display_name or "",
            "property_id": property_id,
            "datetime": dt_iso,
            "status": "requested",
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "notes": notes or "",
        }
        ws = self._worksheet()
        headers = ws.row_values(1)
        ws.append_row([row.get(h, "") for h in headers])
        # bust cache
        self._cache = None
        return row

    def list_for_user(self, user_id: str) -> List[Dict[str, Any]]:
        return [r for r in self._read_all() if str(r.get("user_id")) == str(user_id)]

    def cancel(self, booking_id: str) -> bool:
        ws = self._worksheet()
        data = ws.get_all_records()
        headers = ws.row_values(1)
        id_idx = headers.index("booking_id") if "booking_id" in headers else None
        status_idx = headers.index("status") if "status" in headers else None
        # get additional fields for calendar sync
        pid_idx = headers.index("property_id") if "property_id" in headers else None
        dt_idx = headers.index("datetime") if "datetime" in headers else None
        if id_idx is None or status_idx is None:
            return False
        # find row number (offset by header row)
        row_num = 2
        for r in data:
            if str(r.get("booking_id")) == str(booking_id):
                # write status to 'cancelled'
                ws.update_cell(row_num, status_idx + 1, "cancelled")
                self._cache = None
                return True
            row_num += 1
        return False

    def find_by_id(self, booking_id: str) -> Dict[str, Any]:
        for r in self._read_all():
            if str(r.get("booking_id")) == str(booking_id):
                return r
        return {}

