import os
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dateutil import parser as date_parser
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


_SCOPES = [
    'https://www.googleapis.com/auth/calendar',
]


class CalendarRepository:
    def __init__(self):
        self._creds = None

    def _client(self):
        if not self._creds:
            self._creds = Credentials.from_service_account_file(
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'], scopes=_SCOPES
            )
        return build('calendar', 'v3', credentials=self._creds, cache_discovery=False)

    def find_event(self, calendar_id: str, property_id: str, dt_iso: str) -> Optional[str]:
        svc = self._client()
        q = f"pid:{property_id} dt:{dt_iso}"
        events = svc.events().list(calendarId=calendar_id, q=q).execute()
        for e in events.get('items', []):
            return e.get('id')
        return None

    def create_booking_event(self, calendar_id: str, property_id: str, title: str, dt_iso: str, user_display_name: Optional[str], duration_minutes: int = 30, timezone: str = 'Asia/Bangkok') -> Optional[str]:
        svc = self._client()
        summary = f"Viewing: {title}"
        description = f"pid:{property_id} dt:{dt_iso}\nBooked by: {user_display_name or ''}"
        # Parse start time and compute end time
        try:
            tz = ZoneInfo(timezone)
        except Exception:
            tz = ZoneInfo('UTC')
        start_dt = date_parser.parse(dt_iso)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=tz)
        end_dt = start_dt + timedelta(minutes=duration_minutes)
        event = {
            'summary': summary,
            'description': description,
            'start': {'dateTime': start_dt.isoformat(), 'timeZone': timezone},
            'end': {'dateTime': end_dt.isoformat(), 'timeZone': timezone},
        }
        created = svc.events().insert(calendarId=calendar_id, body=event).execute()
        return created.get('id')

    def delete_event(self, calendar_id: str, event_id: str) -> bool:
        svc = self._client()
        svc.events().delete(calendarId=calendar_id, eventId=event_id).execute()
        return True

