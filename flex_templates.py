from typing import Dict, Any, List

from linebot.v3.messaging import (
    FlexBox,
    FlexText,
    FlexImage,
    FlexButton,
    FlexSeparator,
    FlexBubble,
    FlexContainer,
    PostbackAction,
)


def build_property_card(p: Dict[str, Any], include_actions: bool = False) -> FlexBubble:
    title = p.get("title", "Property")
    price = p.get("price", "")
    image = p.get("thumbnail_url") or (p.get("image_urls", ",").split(",")[0] if p.get("image_urls") else None)
    bedrooms = p.get("bedrooms", "?")
    bathrooms = p.get("bathrooms", "?")
    address = p.get("address", "")
    area = p.get("neighborhood", "")

    body_contents: List[Any] = []
    if image:
        body_contents.append(
            FlexImage(url=image, size="full", aspect_mode="cover", aspect_ratio="20:13")
        )
    body_contents.append(
        FlexBox(
            layout="vertical",
            spacing="sm",
            contents=[
                FlexText(text=title, weight="bold", size="lg", wrap=True),
                FlexText(text=f"฿{price} • {bedrooms}BR/{bathrooms}BA", size="sm", color="#666666", wrap=True),
                FlexText(text=area or address, size="sm", color="#888888", wrap=True),
            ],
        )
    )

    footer_contents: List[Any] = []
    footer_contents.append(
        FlexButton(
            style="link",
            height="sm",
            action=PostbackAction(label="Details", data=f"action=detail&pid={p.get('id')}")
        )
    )
    if include_actions:
        footer_contents.append(FlexSeparator())
        footer_contents.append(
            FlexButton(
                style="link",
                height="sm",
                action=PostbackAction(label="Book viewing", data=f"action=book&pid={p.get('id')}")
            )
        )

    return FlexBubble(
        hero=None,
        body=FlexBox(layout="vertical", contents=body_contents, spacing="md"),
        footer=FlexBox(layout="vertical", contents=footer_contents, spacing="sm"),
    )


def build_property_carousel(bubbles: List[FlexBubble]) -> FlexContainer:
    payload = {
        "type": "carousel",
        "contents": [b.to_dict() for b in bubbles]
    }
    return FlexContainer.from_dict(payload)


def build_booking_confirmation_bubble(p: Dict[str, Any], b: Dict[str, Any]) -> FlexBubble:
    title = p.get("title", "Property")
    dt = b.get("datetime")
    booking_id = b.get("booking_id")
    body = FlexBox(
        layout="vertical",
        contents=[
            FlexText(text="Booking Confirmed", weight="bold", size="lg"),
            FlexText(text=title, size="md", wrap=True),
            FlexText(text=f"When: {dt}", size="sm", color="#666666"),
            FlexText(text=f"Booking #: {booking_id}", size="sm", color="#666666"),
        ],
        spacing="sm"
    )
    footer = FlexBox(
        layout="vertical",
        contents=[
            FlexButton(style="link", height="sm", action=PostbackAction(label="Details", data=f"action=detail&pid={p.get('id')}")),
            FlexSeparator(),
            FlexButton(style="link", height="sm", action=PostbackAction(label="Cancel booking", data=f"action=cancel&bid={booking_id}")),
        ],
        spacing="sm"
    )
    return FlexBubble(body=body, footer=footer)


def build_pagination_bubble(label: str, data: str) -> FlexBubble:
    body = FlexBox(
        layout="vertical",
        contents=[FlexText(text=label, weight="bold", size="md")]
    )
    footer = FlexBox(
        layout="vertical",
        contents=[FlexButton(style="link", height="sm", action=PostbackAction(label="Next", data=data))]
    )
    return FlexBubble(body=body, footer=footer)

