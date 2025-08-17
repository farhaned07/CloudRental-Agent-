import os
import time
import json
from typing import Dict, Any

import gspread
from google.oauth2.service_account import Credentials


_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


class SessionsRepository:
    def __init__(self):
        self.sheet_id = os.getenv("GOOGLE_SHEETS_DOCUMENT_ID")
        if not self.sheet_id:
            raise RuntimeError("GOOGLE_SHEETS_DOCUMENT_ID is required")

    def _client(self):
        creds = Credentials.from_service_account_file(
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=_SCOPES
        )
        return gspread.authorize(creds)

    def _worksheet(self):
        gc = self._client()
        sh = gc.open_by_key(self.sheet_id)
        try:
            ws = sh.worksheet("sessions")
        except Exception:
            ws = sh.add_worksheet(title="sessions", rows=1000, cols=5)
            ws.update("A1", [["user_id", "context_json", "updated_at"]])
        return ws

    def get_context(self, user_id: str) -> Dict[str, Any]:
        try:
            if not user_id:
                return {}
            ws = self._worksheet()
            records = ws.get_all_records()
            for r in records:
                if str(r.get("user_id")) == str(user_id):
                    try:
                        return json.loads(r.get("context_json") or "{}")
                    except Exception:
                        return {}
            return {}
        except Exception:
            return {}

    def set_context(self, user_id: str, context: Dict[str, Any]):
        try:
            if not user_id:
                return
            ws = self._worksheet()
            records = ws.get_all_records()
            headers = ws.row_values(1)
            uid_idx = headers.index("user_id") + 1
            ctx_idx = headers.index("context_json") + 1
            ts_idx = headers.index("updated_at") + 1
            row_num = 2
            for r in records:
                if str(r.get("user_id")) == str(user_id):
                    ws.update_cell(row_num, ctx_idx, json.dumps(context))
                    ws.update_cell(row_num, ts_idx, time.strftime("%Y-%m-%d %H:%M:%S"))
                    return
                row_num += 1
            ws.append_row([user_id, json.dumps(context), time.strftime("%Y-%m-%d %H:%M:%S")])
        except Exception:
            return

