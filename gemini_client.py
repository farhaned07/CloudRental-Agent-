import os
import json
import re
import aiohttp


class GeminiNLU:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is required")
        self.endpoint = (
            "https://generativelanguage.googleapis.com/v1beta/models/" \
            "gemini-pro:generateContent?key=" + self.api_key
        )

    async def parse_intent(self, user_text: str) -> dict:
        # Try LLM first
        intent = await self._parse_with_gemini(user_text)
        if intent:
            return intent
        # Fallback to regex
        return self._regex_intent(user_text)

    async def _parse_with_gemini(self, user_text: str) -> dict:
        system_prompt = (
            "You are an intent parser for a real estate chatbot. "
            "Extract a single intent and normalized filters as JSON. "
            "Supported intents: browse, search, detail, book, my_bookings, cancel, fallback. "
            "Filters can include: price_max, price_min, bedrooms, bathrooms, neighborhood, property_type, property_id. "
            "For cancel intent include booking_id if present. "
            "Return ONLY a minified JSON object like {\"name\":\"search\",\"filters\":{...}} with no extra text."
        )
        payload = {
            "contents": [
                {"role": "user", "parts": [{"text": system_prompt + "\nUser: " + user_text}]}
            ]
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.endpoint, json=payload, timeout=20) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    try:
                        text = data["candidates"][0]["content"]["parts"][0]["text"]
                        return json.loads(text)
                    except Exception:
                        return None
        except Exception:
            return None

    def _regex_intent(self, text: str) -> dict:
        q = text.lower().strip()
        # normalize spaces
        q = ' '.join(q.split())
        if q.startswith("browse"):
            return {"name": "browse", "filters": {}}
        if q.startswith("my bookings"):
            return {"name": "my_bookings", "filters": {}}
        m = re.match(r"detail\s+(\S+)", q)
        if m:
            return {"name": "detail", "filters": {"property_id": m.group(1)}}
        m = re.match(r"book\s+(\S+)", q)
        if m:
            return {"name": "book", "filters": {"property_id": m.group(1)}}
        m = re.match(r"cancel\s+(\S+)", q)
        if m:
            return {"name": "cancel", "filters": {"booking_id": m.group(1)}}
        # naive parse for search
        filters = {}
        b = re.search(r"(\d+)\s*(br|bed|beds|bd|bedroom|bedrooms)\b", q)
        if b:
            filters["bedrooms"] = int(b.group(1))
        m = re.search(r"under\s+(\d+[\d,\s]*)", q)
        if m:
            filters["price_max"] = int(m.group(1).replace(",", "").replace(" ", ""))
        m = re.search(r"under\s+(\d+)\s*k\b", q)
        if m:
            filters["price_max"] = int(m.group(1)) * 1000
        m = re.search(r"over\s+(\d+[\d,]*)", q)
        if m:
            filters["price_min"] = int(m.group(1).replace(",", ""))
        m = re.search(r"in\s+([a-z\-\s]+)$", q)
        if m:
            filters["neighborhood"] = m.group(1).strip()
        if "condo" in q:
            filters["property_type"] = "condo"
        if "retail" in q:
            filters["property_type"] = "retail"
        if "land" in q:
            filters["property_type"] = "land"
        if filters:
            return {"name": "search", "filters": filters}
        return {"name": "fallback", "filters": {}}

