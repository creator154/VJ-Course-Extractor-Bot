# helpers.py

import logging
import re
from base64 import b64decode
from typing import Dict, Optional

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

from pyrogram import Client, filters
from pyrogram.types import Message
from pyromod.exceptions import ListenerTimeout

from config import auth_users


def is_authorized(user_id: int) -> bool:
    return bool(auth_users and user_id in auth_users)


async def ask_user(
    bot: Client,
    m: Message,
    editable: Message,
    text: str,
    user_id: int,
    timeout: int = 120
) -> Optional[str]:

    await editable.edit(text)

    try:
        msg = await bot.listen(
            chat_id=m.chat.id,
            filters=filters.user(user_id),
            timeout=timeout
        )

        val = (msg.text or "").strip()

        try:
            await msg.delete(True)
        except Exception:
            pass

        return val

    except ListenerTimeout:

        await editable.edit(
            "**Timeout! You took too long to respond.**"
        )

        return None

    except Exception as e:

        logging.exception(
            "Error during input listener:"
        )

        await editable.edit(
            f"**Error:** `{e}`"
        )

        return None


def _extract_url_from_html(value: str) -> str:
    """
    Extract a normal URL from iframe/embed HTML.
    """

    value = value.strip()

    if not value:
        return ""

    # iframe src
    match = re.search(
        r'''src\s*=\s*["'](https?://[^"']+)["']''',
        value,
        re.IGNORECASE
    )

    if match:
        return match.group(1).strip()

    # Any URL inside HTML
    match = re.search(
        r'https?://[^"\'\s<>]+',
        value
    )

    if match:
        return match.group(0).strip()

    return value


def extract_url_from_video_details(
    item: Dict
) -> str:
    """
    Extract the officially supplied media/stream URL
    from PW video details.

    Priority:
        videoUrl
        mediaUrl
        streamUrl
        hlsUrl
        mpdUrl
        downloadUrl
        url
        fileUrl

    Then checks the same fields at item level.
    """

    if not isinstance(item, dict):
        return ""

    v_details = item.get("videoDetails") or {}

    if not isinstance(v_details, dict):
        v_details = {}

    candidates = [
        v_details.get("videoUrl"),
        v_details.get("mediaUrl"),
        v_details.get("streamUrl"),
        v_details.get("hlsUrl"),
        v_details.get("mpdUrl"),
        v_details.get("downloadUrl"),
        v_details.get("url"),
        v_details.get("fileUrl"),

        item.get("videoUrl"),
        item.get("mediaUrl"),
        item.get("streamUrl"),
        item.get("hlsUrl"),
        item.get("mpdUrl"),
        item.get("downloadUrl"),
        item.get("url"),
        item.get("fileUrl"),
    ]

    for candidate in candidates:

        if candidate is None:
            continue

        if not isinstance(candidate, str):
            continue

        url = candidate.strip()

        if not url:
            continue

        # Remove accidental surrounding quotes
        url = url.strip("\"'")

        # HTML / iframe response
        if (
            "<" in url
            or "iframe" in url.lower()
            or "src=" in url.lower()
        ):

            extracted = _extract_url_from_html(url)

            if extracted:
                url = extracted

        # Only accept actual HTTP(S) URLs here.
        if re.match(
            r"^https?://",
            url,
            re.IGNORECASE
        ):

            logging.info(
                "VIDEO URL EXTRACTED | type=%s | url=%s",
                (
                    "MPD"
                    if ".mpd" in url.lower()
                    else
                    "HLS"
                    if ".m3u8" in url.lower()
                    else
                    "MEDIA"
                ),
                url
            )

            return url

    logging.warning(
        "No usable video URL found in video details."
    )

    return ""


def appx_decrypt(enc: str) -> str:

    if not enc:
        return ""

    try:

        enc_bytes = b64decode(
            enc.split(":")[0]
        )

        if not enc_bytes:
            return ""

        key = b"638udh3829162018"
        iv = b"fedcba9876543210"

        cipher = AES.new(
            key,
            AES.MODE_CBC,
            iv
        )

        return unpad(
            cipher.decrypt(enc_bytes),
            AES.block_size
        ).decode("utf-8")

    except Exception:

        logging.exception(
            "APPX decrypt failed"
        )

        return ""
