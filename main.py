import os
import sys
import json
import logging
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, Request, HTTPException
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timezone
import tempfile

from linebot.v3.webhook import WebhookParser
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    AsyncApiClient,
    AsyncMessagingApi,
    Configuration,
    ReplyMessageRequest,
    PushMessageRequest,
    TextMessage,
    TemplateMessage,
    ConfirmTemplate,
    DatetimePickerAction,
    QuickReply,
    QuickReplyItem,
    MessageAction,
    FlexMessage,
)
from linebot.v3.webhooks import (
    MessageEvent,
    PostbackEvent,
    TextMessageContent,
)

from repositories.properties_repo import PropertiesRepository
from repositories.bookings_repo import BookingsRepository
from repositories.agents_repo import AgentsRepository
from repositories.calendar_repo import CalendarRepository
from flex_templates import (
    build_property_card,
    build_property_carousel,
    build_booking_confirmation_bubble,
    build_pagination_bubble,
)
from gemini_client import GeminiNLU


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("realestate-bot")


# Allow service account JSON injection for serverless
sa_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
if sa_json and not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
    tmp.write(sa_json.encode()); tmp.flush(); tmp.close()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = tmp.name

channel_secret = os.getenv("LINE_CHANNEL_SECRET")
channel_access_token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")

# Lazy-initialize LINE clients only when creds exist
line_bot_api = None
parser = None
if channel_access_token and channel_secret:
    configuration = Configuration(access_token=channel_access_token)
    async_api_client = AsyncApiClient(configuration)
    line_bot_api = AsyncMessagingApi(async_api_client)
    parser = WebhookParser(channel_secret)


# Data layer
properties_repo = PropertiesRepository()
bookings_repo = BookingsRepository()
agents_repo = AgentsRepository()
calendar_repo = CalendarRepository()

# NLU
gemini = GeminiNLU()


app = FastAPI()
scheduler = None
try:
    # Some serverless platforms don’t allow background schedulers
    scheduler = AsyncIOScheduler()
    scheduler.start()
except Exception as e:
    logger.warning("Scheduler disabled: %s", e)


def _safe_text(text: str) -> TextMessage:
    return TextMessage(text=text[:4900])




@app.get("/callback")
async def callback_get():
    return "OK"

@app.get("/callback")
async def callback_get():
    return "OK"


@app.post("/callback")
async def callback(request: Request):
    if not parser or not line_bot_api:
        raise HTTPException(status_code=500, detail="LINE credentials not configured")
    signature = request.headers.get("X-Line-Signature")
    if not signature:
        raise HTTPException(status_code=400, detail="Missing signature")

    body_bytes = await request.body()
    body = body_bytes.decode()

    try:
        events = parser.parse(body, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    for event in events:
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessageContent):
            await _handle_text(event)
        elif isinstance(event, PostbackEvent):
            await _handle_postback(event)
        else:
            # Ignore other events for now
            continue

    return "OK"


async def _handle_text(event: MessageEvent):
    user_text = event.message.text.strip()
    user_id = getattr(getattr(event, "source", None), "user_id", None)

    try:
        intent = await gemini.parse_intent(user_text)
    except Exception as e:
        logger.exception("Gemini intent parsing failed: %s", e)
        intent = {"name": "fallback", "filters": {}}

    name = intent.get("name") or "fallback"
    filters = intent.get("filters") or {}

    if name in ("browse", "search"):
        # pagination cursor (0-based index)
        cursor = 0
        try:
            if "cursor" in filters:
                cursor = max(0, int(filters["cursor"]))
        except Exception:
            cursor = 0
        props = properties_repo.search(filters)
        if not props:
            await line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[_safe_text("No properties found. Try broadening your search.")]
                )
            )
            return
        page = props[cursor:cursor + 9]
        bubbles = [build_property_card(p) for p in page]
        # add next pager if there are more
        if cursor + 9 < len(props):
            next_cursor = cursor + 9
            bubbles.append(build_pagination_bubble("More results", f"action=browse&cursor={next_cursor}"))
        car = build_property_carousel(bubbles)
        await line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[FlexMessage(alt_text="Property results", contents=car)]
            )
        )
        return

    if name == "detail":
        pid = filters.get("property_id")
        prop = properties_repo.get_by_id(pid) if pid else None
        if not prop:
            await line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[_safe_text("Property not found.")]
                )
            )
            return
        bubble = build_property_card(prop, include_actions=True)
        await line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[FlexMessage(alt_text="Property detail", contents=bubble)]
            )
        )
        return

    if name == "book":
        pid = filters.get("property_id")
        prop = properties_repo.get_by_id(pid) if pid else None
        if not prop:
            await line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[_safe_text("Please specify a valid property to book.")]
                )
            )
            return

        await line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[
                    TextMessage(
                        text=f"Pick a date/time for {prop.get('title', 'the property')}",
                        quick_reply=QuickReply(items=[
                            QuickReplyItem(
                                action=DatetimePickerAction(
                                    label="Pick date",
                                    data=f"action=book_pick&pid={prop['id']}",
                                    mode="datetime",
                                )
                            )
                        ])
                    )
                ]
            )
        )
        return

    if name == "my_bookings":
        bookings = bookings_repo.list_for_user(user_id)
        if not bookings:
            await line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[_safe_text("You have no bookings.")]
                )
            )
            return
        lines = []
        for b in bookings[:10]:
            prop = properties_repo.get_by_id(b.get("property_id"))
            title = prop.get("title") if prop else b.get("property_id")
            lines.append(f"#{b.get('booking_id')} - {title} at {b.get('datetime')} [{b.get('status')}]")
        await line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[_safe_text("\n".join(lines))]
            )
        )
        return

    if name == "cancel":
        bid = filters.get("booking_id")
        if not bid:
            await line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[_safe_text("Please specify a booking id to cancel.")]
                )
            )
            return
        ok = bookings_repo.cancel(bid)
        msg = "Cancelled." if ok else "Booking not found or already cancelled."
        await line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[_safe_text(msg)]
            )
        )
        return

    # fallback: show help
    await line_bot_api.reply_message(
        ReplyMessageRequest(
            reply_token=event.reply_token,
            messages=[_safe_text(
                "Try: 'browse', 'search 2br in Thonglor under 30k', 'detail <id>', 'book <id>', 'my bookings', 'cancel <booking_id>'"
            )]
        )
    )


async def _handle_postback(event: PostbackEvent):
    data = event.postback.data or ""
    params = getattr(event.postback, "params", None)

    # booking datetime chosen
    if data.startswith("action=book_pick") and isinstance(params, dict) and params.get("datetime"):
        pid = _extract_query_param(data, "pid")
        dt = params.get("datetime")
        user_id = getattr(getattr(event, "source", None), "user_id", None)
        display_name = None
        try:
            display_name = None  # optional: call profile API if needed
        except Exception:
            display_name = None

        prop = properties_repo.get_by_id(pid) if pid else None
        if not prop:
            await line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[_safe_text("Property not found.")]
                )
            )
            return

        # check availability: both Sheets and Calendar
        if bookings_repo.exists(pid, dt):
            await line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[_safe_text("Time slot already taken. Please choose another time.")]
                )
            )
            return
        calendar_id = properties_repo.get_calendar_id(pid)
        if calendar_id and calendar_repo.find_event(calendar_id, pid, dt):
            await line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[_safe_text("Time slot already taken (Calendar). Please choose another time.")]
                )
            )
            return

        # try to enrich with user display name
        try:
            if user_id:
                profile = await line_bot_api.get_profile(user_id=user_id)
                display_name = getattr(profile, "display_name", None) or display_name
        except Exception:
            pass

        booking = bookings_repo.create(
            user_id=user_id,
            user_display_name=display_name,
            property_id=pid,
            dt_iso=dt,
            notes=None,
        )

        bubble = build_booking_confirmation_bubble(prop, booking)
        await line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[FlexMessage(alt_text="Booking confirmed", contents=bubble)]
            )
        )
        # create Google Calendar event
        try:
            if calendar_id:
                calendar_repo.create_booking_event(
                    calendar_id=calendar_id,
                    property_id=pid,
                    title=prop.get('title', 'Viewing'),
                    dt_iso=dt,
                    user_display_name=display_name,
                )
        except Exception as e:
            logger.warning("Failed to create calendar event: %s", e)
        return

    # detail from flex button
    if data.startswith("action=detail"):
        pid = _extract_query_param(data, "pid")
        prop = properties_repo.get_by_id(pid) if pid else None
        if not prop:
            await line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[_safe_text("Property not found.")]
                )
            )
            return
        bubble = build_property_card(prop, include_actions=True)
        await line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[FlexMessage(alt_text="Property detail", contents=bubble)]
            )
        )
        return

    # initiate booking from flex button
    if data.startswith("action=book"):
        pid = _extract_query_param(data, "pid")
        prop = properties_repo.get_by_id(pid) if pid else None
        if not prop:
            await line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[_safe_text("Property not found.")]
                )
            )
            return
        await line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[
                    TextMessage(
                        text=f"Pick a date/time for {prop.get('title', 'the property')}",
                        quick_reply=QuickReply(items=[
                            QuickReplyItem(
                                action=DatetimePickerAction(
                                    label="Pick date",
                                    data=f"action=book_pick&pid={prop['id']}",
                                    mode="datetime",
                                )
                            )
                        ])
                    )
                ]
            )
        )
        return

    # cancel from flex button
    if data.startswith("action=cancel"):
        bid = _extract_query_param(data, "bid")
        # attempt to cancel Sheets and Calendar
        ok = bookings_repo.cancel(bid) if bid else False
        if bid:
            b = bookings_repo.find_by_id(bid)
            pid = b.get('property_id')
            dt = b.get('datetime')
            cid = properties_repo.get_calendar_id(pid) if pid else None
            try:
                if cid and dt:
                    ev_id = calendar_repo.find_event(cid, pid, dt)
                    if ev_id:
                        calendar_repo.delete_event(cid, ev_id)
            except Exception as e:
                logger.warning("Failed to delete calendar event: %s", e)
        msg = "Cancelled." if ok else "Booking not found or already cancelled."
        await line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[_safe_text(msg)]
            )
        )
        return

    # browse pagination from flex button
    if data.startswith("action=browse"):
        cursor_str = _extract_query_param(data, "cursor") or "0"
        try:
            cursor = max(0, int(cursor_str))
        except Exception:
            cursor = 0
        props = properties_repo.search({})
        page = props[cursor:cursor + 9]
        if not page:
            await line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[_safe_text("No more results.")]
                )
            )
            return
        bubbles = [build_property_card(p) for p in page]
        if cursor + 9 < len(props):
            next_cursor = cursor + 9
            bubbles.append(build_pagination_bubble("More results", f"action=browse&cursor={next_cursor}"))
        car = build_property_carousel(bubbles)
        await line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[FlexMessage(alt_text="More properties", contents=car)]
            )
        )
        return


def _extract_query_param(data: str, key: str) -> Optional[str]:
    try:
        parts = data.split("&")
        for p in parts:
            if p.startswith(f"{key}="):
                return p.split("=", 1)[1]
    except Exception:
        return None
    return None


# simple reminder job (T-24h and T-2h) running every 10 minutes
async def send_reminders():
    if not line_bot_api:
        logger.warning('Reminders skipped: LINE credentials not configured')
        return
    try:
        now = datetime.now(timezone.utc)
        upcoming = bookings_repo._read_all()
        for b in upcoming:
            status = str(b.get('status', 'requested')).lower()
            if status not in ('requested', 'confirmed'):
                continue
            dt_iso = b.get('datetime')
            try:
                dt = datetime.fromisoformat((dt_iso or '').replace('Z', '+00:00'))
            except Exception:
                continue
            delta = (dt - now).total_seconds()
            # within ~2h or ~24h windows (±5min)
            if (2*3600 - 5*60) < delta < (2*3600 + 5*60) and int(os.getenv('ENABLE_REMINDERS', '1')) == 1:
                await _push_reminder(b, '2h')
            if (24*3600 - 5*60) < delta < (24*3600 + 5*60) and int(os.getenv('ENABLE_REMINDERS', '1')) == 1:
                await _push_reminder(b, '24h')
    except Exception as e:
        logger.warning('Reminder job failed: %s', e)


# If scheduler exists, register the job
if scheduler is not None:
    try:
        scheduler.add_job(send_reminders, 'interval', minutes=10)
    except Exception as e:
        logger.warning("Failed to add reminder job: %s", e)


# Optional HTTP trigger for reminders (for platforms like Vercel/cron)
@app.get('/cron/reminders')
async def cron_reminders():
    await send_reminders()
    return {"ok": True}


async def _push_reminder(booking: Dict[str, Any], window: str):
    try:
        user_id = booking.get('user_id')
        pid = booking.get('property_id')
        prop = properties_repo.get_by_id(pid)
        title = prop.get('title') if prop else pid
        dt_str = booking.get('datetime')
        msg = f"Reminder: Viewing for {title} at {dt_str} (T-{window})."
        if user_id:
            await line_bot_api.push_message(
                PushMessageRequest(
                    to=user_id,
                    messages=[_safe_text(msg)]
                )
            )
    except Exception as e:
        logger.warning('Push reminder failed: %s', e)

