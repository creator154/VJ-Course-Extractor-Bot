import asyncio
import json
import logging
import os
import random
import time
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

# Logging setup
logging.basicConfig(level=logging.INFO)

# ---------------- RATE LIMIT SETTINGS (tune here) ----------------
MAX_CONCURRENCY = 4          # parallel requests at a time
MIN_REQUEST_INTERVAL = 0.25  # min seconds between any two requests (~4 req/s)
MAX_RETRIES = 6              # retries per request
BASE_BACKOFF = 2.0           # exponential backoff base (seconds)
MAX_BACKOFF = 60.0           # cap for a single wait
# -----------------------------------------------------------------

SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENCY)


class RateLimiter:
    """Global limiter: spaces out requests and supports a shared cooldown
    when the server answers 429, so ALL tasks slow down together."""

    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._lock = asyncio.Lock()
        self._next_slot = 0.0
        self._pause_until = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            start = max(now, self._next_slot, self._pause_until)
            self._next_slot = start + self.min_interval
        delay = start - now
        if delay > 0:
            await asyncio.sleep(delay)

    def pause(self, seconds: float):
        self._pause_until = max(self._pause_until, time.monotonic() + seconds)


RATE_LIMITER = RateLimiter(MIN_REQUEST_INTERVAL)

# User response tracking dictionary for built-in ask_user
USER_RESPONSES: Dict[int, str] = {}
USER_EVENTS: Dict[int, asyncio.Event] = {}


class ProcessCancelledException(Exception):
    """Custom exception raised when a process is cancelled by the user."""
    pass


def format_time(seconds: float) -> str:
    seconds = int(seconds)
    mins, secs = divmod(seconds, 60)
    hrs, mins = divmod(mins, 60)
    if hrs > 0:
        return f"{hrs:02d}h {mins:02d}m {secs:02d}s"
    return f"{mins:02d}m {secs:02d}s"


def extract_url_from_video_details(item: Dict) -> str:
    """Built-in video URL extractor for PW PenPencil API responses."""
    if not item or not isinstance(item, dict):
        return ""

    vd = item.get("videoDetails", {}) or {}
    if vd:
        url = (
            vd.get("videoUrl")
            or vd.get("hlsUrl")
            or vd.get("mpdUrl")
            or vd.get("url")
            or ""
        )
        if url:
            return url

    direct_url = item.get("url") or item.get("videoUrl") or ""
    return direct_url


async def ask_user(
    bot: Client,
    message: Message,
    editable: Message,
    text: str,
    user_id: int,
    timeout: int = 300
) -> Optional[str]:
    """Built-in helper to wait for user text input via Telegram."""
    event = asyncio.Event()
    USER_EVENTS[user_id] = event
    USER_RESPONSES[user_id] = None

    await editable.edit(text)

    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        response = USER_RESPONSES.get(user_id)
        return response
    except asyncio.TimeoutError:
        return None
    finally:
        USER_EVENTS.pop(user_id, None)
        USER_RESPONSES.pop(user_id, None)


async def prompt_user(
    bot: Client,
    message: Message,
    editable: Message,
    text: str,
    user_id: int
) -> str:
    cancel_notice = (
        "\n\n"
        "<blockquote>❌ **Send `/cancel` at any time to abort this process.**</blockquote>"
    )

    response = await ask_user(bot, message, editable, text + cancel_notice, user_id)

    if response is None or response.strip().lower() == "/cancel":
        await editable.edit("**Process Cancelled by User ❌**")
        raise ProcessCancelledException("User requested cancellation.")

    return response.strip()


async def update_status_card(
    editable: Message,
    task_name: str,
    current: int,
    total: int,
    start_time: float,
    activity: str
):
    percentage = (current / total * 100) if total > 0 else 0
    elapsed = time.time() - start_time

    if current > 0 and total > 0:
        avg_time_per_unit = elapsed / current
        remaining_units = total - current
        eta = avg_time_per_unit * remaining_units
        eta_str = format_time(eta)
    else:
        eta_str = "Calculating..."

    filled_blocks = int(percentage // 10)
    progress_bar = "▓" * filled_blocks + "░" * (10 - filled_blocks)

    status_text = (
        f"<blockquote>⚙️ **Processing Task:** `{task_name}`</blockquote>\n\n"
        f"📊 **Progress:** [{progress_bar}] `{percentage:.1f}%` ({current}/{total})\n"
        f"📌 **Current Activity:** {activity}\n"
        f"⏱️ **Time Elapsed:** `{format_time(elapsed)}`\n"
        f"⏳ **Time Left (ETA):** `{eta_str}`\n\n"
        f"<blockquote>❌ **Send `/cancel` to abort.**</blockquote>"
    )

    try:
        await editable.edit(status_text)
    except Exception:
        pass


def _backoff_delay(attempt: int, retry_after: Optional[float] = None) -> float:
    """Server's Retry-After wins; otherwise exponential backoff + jitter."""
    if retry_after is not None and retry_after > 0:
        delay = retry_after
    else:
        delay = BASE_BACKOFF * (2 ** attempt)
    delay = min(delay, MAX_BACKOFF)
    return delay + random.uniform(0, 1)


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


async def fetch_pwwp_data(
    session: aiohttp.ClientSession,
    url: str,
    headers: Dict = None,
    params: Dict = None,
    data: Dict = None,
    method: str = "GET"
) -> Any:
    """Request with global throttling, 429 handling and backoff.
    The semaphore is held only during the request itself, never while sleeping."""

    for attempt in range(MAX_RETRIES):
        wait_for = 0.0  # >0 means: retry after sleeping this long

        try:
            async with SEMAPHORE:
                await RATE_LIMITER.wait()

                async with session.request(
                    method, url, headers=headers, params=params, json=data
                ) as response:
                    status = response.status
                    response_body = await response.text()
                    retry_after = _parse_retry_after(response.headers.get("Retry-After"))

            logging.info(
                "PWWP API | method=%s | status=%s | endpoint=%s",
                method, status, url
            )

            if status == 401:
                logging.error("PWWP AUTH FAILED | endpoint=%s", url)
                return {
                    "_auth_error": True,
                    "_status": 401,
                    "_response": response_body
                }

            if status in (429, 503):
                wait_for = _backoff_delay(attempt, retry_after)
                RATE_LIMITER.pause(wait_for)  # slow down ALL tasks
                logging.warning(
                    "PWWP RATE LIMITED | status=%s | attempt=%s | waiting %.1fs | endpoint=%s",
                    status, attempt + 1, wait_for, url
                )

            elif status >= 500:
                wait_for = _backoff_delay(attempt)
                logging.error(
                    "PWWP SERVER ERROR | attempt=%s | status=%s | endpoint=%s",
                    attempt + 1, status, url
                )

            elif status >= 400:
                # 403/404 etc. will not fix themselves, don't hammer the API
                logging.error("PWWP CLIENT ERROR | status=%s | endpoint=%s", status, url)
                return None

            else:
                try:
                    return json.loads(response_body)
                except json.JSONDecodeError:
                    return None

        except asyncio.TimeoutError:
            logging.error("PWWP TIMEOUT | attempt=%s | endpoint=%s", attempt + 1, url)
            wait_for = _backoff_delay(attempt)
        except aiohttp.ClientError as e:
            logging.error("PWWP NETWORK ERROR | attempt=%s | endpoint=%s | error=%s", attempt + 1, url, str(e))
            wait_for = _backoff_delay(attempt)
        except Exception:
            logging.exception("PWWP UNEXPECTED ERROR | endpoint=%s", url)
            wait_for = _backoff_delay(attempt)

        if attempt < MAX_RETRIES - 1 and wait_for > 0:
            await asyncio.sleep(wait_for)

    logging.error("PWWP GAVE UP after %s attempts | endpoint=%s", MAX_RETRIES, url)
    return None


# Cache so the same schedule-details is never fetched twice
# (same item can show up in several chapters / content types).
_DETAILS_CACHE: Dict[str, "asyncio.Future"] = {}


async def get_schedule_details(session, url: str, headers: Dict):
    fut = _DETAILS_CACHE.get(url)
    if fut is None:
        fut = asyncio.ensure_future(fetch_pwwp_data(session, url, headers=headers))
        _DETAILS_CACHE[url] = fut
    return await asyncio.shield(fut)


async def process_pwwp_chapter_content(
    session: aiohttp.ClientSession,
    selected_batch_id: str,
    subject_id: str,
    schedule_id: str,
    content_type: str,
    headers: Dict
) -> Dict[str, List[str]]:

    url = (
        f"https://api.penpencil.co/v1/batches/"
        f"{selected_batch_id}/subject/{subject_id}/schedule/"
        f"{schedule_id}/schedule-details"
    )

    data = await get_schedule_details(session, url, headers)
    content = []

    if data and not data.get("_auth_error") and data.get("success") and data.get("data"):
        item = data["data"]
        topic = item.get("topic", "")

        # 1. Video extraction
        if content_type in ("videos", "dppVideos", "DppVideos"):
            video_url = extract_url_from_video_details(item)
            if video_url:
                content.append(f"{topic}:{video_url}")

        # 2. Notes / PDFs extraction
        if content_type in ("notes", "dppNotes", "DppNotes", "videos", "dppVideos"):
            for att in item.get("attachments", []) or []:
                u = att.get("url") or att.get("fileUrl") or (
                    (att.get("baseUrl") or "") + (att.get("key") or "")
                )
                if u:
                    content.append(f"{topic}:{u}")

            for hw in item.get("homeworkIds", []) or []:
                hw_topic = hw.get("topic", topic)
                for att in (hw.get("attachmentIds", []) or hw.get("attachments", []) or []):
                    u = att.get("url") or att.get("fileUrl") or (
                        (att.get("baseUrl") or "") + (att.get("key") or "")
                    )
                    if u:
                        content.append(f"{hw_topic}:{u}")

    return {content_type: content} if content else {}


async def fetch_pwwp_all_schedule(
    session: aiohttp.ClientSession,
    chapter_id: str,
    selected_batch_id: str,
    subject_id: str,
    content_type: str,
    headers: Dict
) -> List[Dict]:

    all_schedules = []
    page = 1

    while True:
        url = (
            f"https://api.penpencil.co/v2/batches/"
            f"{selected_batch_id}/subject/{subject_id}/contents"
        )

        params = {
            "topicId": chapter_id,
            "contentType": content_type,
            "page": page
        }

        data = await fetch_pwwp_data(session, url, headers=headers, params=params)

        if data and not data.get("_auth_error") and data.get("success") and data.get("data"):
            items = data["data"]
            if not items:
                break

            for item in items:
                item["content_type"] = content_type

                if content_type in ("videos", "dppVideos"):
                    direct_url = extract_url_from_video_details(item)
                    if direct_url:
                        item["_pre_extracted_url"] = direct_url

                all_schedules.append(item)

            page += 1
        else:
            break

    return all_schedules


async def process_pwwp_chapters(
    session: aiohttp.ClientSession,
    chapter_id: str,
    selected_batch_id: str,
    subject_id: str,
    headers: Dict,
    allowed_content_types: List[str]
) -> Dict[str, List[str]]:

    all_schedules = await asyncio.gather(
        *[
            fetch_pwwp_all_schedule(
                session, chapter_id, selected_batch_id, subject_id, ct, headers
            )
            for ct in allowed_content_types
        ]
    )

    flat_schedule = []
    seen = set()
    for sublist in all_schedules:
        for s in sublist:
            key = (s.get("_id"), s.get("content_type"))
            if key in seen:
                continue
            seen.add(key)
            flat_schedule.append(s)
    tasks = []

    for item in flat_schedule:
        sid = item["_id"]
        ct = item["content_type"]

        if ct in ("videos", "dppVideos") and item.get("_pre_extracted_url"):
            async def _direct(c_type=ct, name=item.get("topic", sid), url=item["_pre_extracted_url"]):
                return {c_type: [f"{name}:{url}"]}
            tasks.append(_direct())
        else:
            tasks.append(
                process_pwwp_chapter_content(
                    session, selected_batch_id, subject_id, sid, ct, headers
                )
            )

    if not tasks:
        return {}

    results = await asyncio.gather(*tasks, return_exceptions=True)
    combined = {}

    for res in results:
        if isinstance(res, Exception):
            logging.error("Chapter content error: %s", res)
            continue

        for c_type, c_list in res.items():
            combined.setdefault(c_type, []).extend(c_list)

    return combined


async def get_pwwp_all_chapters(
    session: aiohttp.ClientSession,
    selected_batch_id: str,
    subject_id: str,
    headers: Dict
) -> List[Dict]:

    chapters = []
    page = 1

    while True:
        url = f"https://api.penpencil.co/v2/batches/{selected_batch_id}/subject/{subject_id}/topics"
        params = {"page": page}

        data = await fetch_pwwp_data(session, url, headers=headers, params=params)

        if data and not data.get("_auth_error") and data.get("data"):
            items = data["data"]
            if not items:
                break
            chapters.extend(items)
            page += 1
        else:
            break

    return chapters


async def process_pwwp_subject(
    session: aiohttp.ClientSession,
    subject: Dict,
    selected_batch_id: str,
    selected_batch_name: str,
    zipf: zipfile.ZipFile,
    json_data: Dict,
    all_subject_urls: Dict[str, List[str]],
    headers: Dict,
    allowed_content_types: List[str]
):

    subject_name = subject.get("subject", "Unknown Subject").replace("/", "-")
    subject_id = subject.get("_id")

    json_data[selected_batch_name][subject_name] = {}
    zipf.writestr(f"{subject_name}/", "")

    chapters = await get_pwwp_all_chapters(session, selected_batch_id, subject_id, headers)
    if not chapters:
        return

    # Chapters are processed in small groups instead of all at once,
    # so we don't queue thousands of requests in one burst.
    CHAPTER_BATCH = 3
    results: List[Any] = []

    for i in range(0, len(chapters), CHAPTER_BATCH):
        group = chapters[i:i + CHAPTER_BATCH]
        group_results = await asyncio.gather(
            *[
                process_pwwp_chapters(
                    session, ch["_id"], selected_batch_id, subject_id, headers, allowed_content_types
                )
                for ch in group
            ],
            return_exceptions=True
        )
        results.extend(group_results)

    all_urls = []

    for ch, content_map in zip(chapters, results):
        if isinstance(content_map, Exception):
            logging.error("Chapter failed: %s", content_map)
            continue

        ch_name = ch.get("name", "Unknown Chapter").replace("/", "-")
        json_data[selected_batch_name][subject_name][ch_name] = {}

        for c_type in allowed_content_types:
            if content_map.get(c_type):
                c_list = content_map[c_type]
                c_list.reverse()

                zipf.writestr(
                    f"{subject_name}/{ch_name}/{c_type}.txt",
                    "\n".join(c_list).encode("utf-8")
                )

                json_data[selected_batch_name][subject_name][ch_name][c_type] = c_list
                all_urls.extend(c_list)

    all_subject_urls[subject_name] = all_urls


async def process_pwwp(bot: Client, m: Message, user_id: int):
    api_headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "origin": "https://www.pw.live",
        "referer": "https://www.pw.live/",
        "user-agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "Chrome/148.0.0.0 Safari/537.36"
        ),
        "client-id": "5eb393ee95fab7468a79d189",
        "client-type": "WEB",
        "content-type": "application/json",
        "randomid": str(uuid.uuid4()),
        "x-sdk-version": "0.0.25",
    }

    _DETAILS_CACHE.clear()
    base_payload = {"organizationId": "5eb393ee95fab7468a79d189"}
    editable = await m.reply_text("**Wait initializing process... ⏳**")
    clean_name = None

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60)
        ) as session:

            raw_input = await prompt_user(
                bot, m, editable,
                "**Enter Your Account Access Token or Phone Number:**",
                user_id
            )

            access_token = None

            # Phone Login
            if raw_input.isdigit() and len(raw_input) == 10:
                otp_payload = {**base_payload, "username": raw_input, "countryCode": "+91"}
                await editable.edit("**Sending OTP to registered phone... ⏳**")

                async with session.post(
                    "https://api.penpencil.co/v1/users/get-otp-secure?smsType=0",
                    headers={**api_headers, "randomid": str(uuid.uuid4())},
                    json=otp_payload
                ) as resp:
                    if resp.status == 429:
                        await editable.edit("**Too many OTP requests ⏳ Please wait a few minutes and try again.**")
                        return
                    if resp.status >= 400:
                        await editable.edit("**OTP Request Failed ❌**")
                        return

                otp = await prompt_user(bot, m, editable, "**Enter OTP received on phone:**", user_id)
                if not otp.isdigit():
                    await editable.edit("**Invalid OTP format! ❌**")
                    return

                token_payload = {
                    **base_payload,
                    "client_id": "system-admin",
                    "grant_type": "password",
                    "latitude": 0,
                    "longitude": 0,
                    "username": raw_input,
                    "otp": str(otp)
                }

                await editable.edit("**Verifying OTP... ⏳**")

                async with session.post(
                    "https://api.penpencil.co/v3/oauth/token",
                    headers={**api_headers, "randomid": str(uuid.uuid4())},
                    json=token_payload
                ) as resp:
                    if resp.status == 429:
                        await editable.edit("**Too many attempts ⏳ Please wait a few minutes and try again.**")
                        return
                    if resp.status >= 400:
                        await editable.edit("**Login Failed ❌**")
                        return
                    res_data = await resp.json() if resp.status == 200 else {}
                    access_token = res_data.get("data", {}).get("access_token")

                if not access_token:
                    await editable.edit("**Login Failed ❌ Invalid response.**")
                    return

                await editable.edit("**PW Login Successful ✅ Token generated.**")

            else:
                access_token = raw_input.strip()
                if access_token.lower().startswith("bearer "):
                    access_token = access_token[7:].strip()

            if not access_token:
                await editable.edit("**Invalid Access Token ❌**")
                return

            auth_headers = {**api_headers, "authorization": f"Bearer {access_token}"}

            # Batch Search
            batch_search = await prompt_user(bot, m, editable, "**Enter Batch Name to Search:**", user_id)
            await editable.edit("**Searching courses online... 🔍**")

            courses_res = await fetch_pwwp_data(
                session,
                "https://api.penpencil.co/v3/batches/search",
                headers=auth_headers,
                params={"name": batch_search}
            )

            if courses_res and courses_res.get("_auth_error"):
                await editable.edit("🔐 **Authorization Failed ❌ HTTP 401**")
                return

            courses = courses_res.get("data", []) if courses_res else []
            if not courses:
                await editable.edit("❌ **No Batches Found!**")
                return

            text_list = "\n".join(
                [f"<blockquote>**{i + 1}.** `{c.get('name', 'Batch')}`</blockquote>" for i, c in enumerate(courses)]
            )

            idx_str = await prompt_user(
                bot, m, editable,
                f"**Select Course Index:**\n\n{text_list}",
                user_id
            )

            if not idx_str.isdigit() or not (1 <= int(idx_str) <= len(courses)):
                await editable.edit("**Invalid Selection ❌**")
                return

            selected_course = courses[int(idx_str) - 1]
            batch_id = selected_course["_id"]
            batch_name = selected_course.get("name", "Batch")
            clean_name = batch_name.replace("/", "-").replace("|", "-")

            # FETCH BATCH DETAILS & CHECK PAID VS RANDOM TOKEN
            await editable.edit("🔍 **Checking Token Permissions & Batch Subscription...**")
            b_details = await fetch_pwwp_data(
                session,
                f"https://api.penpencil.co/v3/batches/{batch_id}/details",
                headers=auth_headers
            )

            batch_data = b_details.get("data", {}) if b_details else {}
            is_purchased = batch_data.get("isPurchased", False) or batch_data.get("isEnrolled", False)

            if is_purchased:
                token_mode_str = "💎 **PAID TOKEN DETECTED**\nExtracting: **Videos + Notes + DPPs**"
                allowed_content_types = ["videos", "notes", "dppNotes", "dppVideos"]
            else:
                token_mode_str = "🔓 **RANDOM / FREE TOKEN DETECTED**\nExtracting: **PDFs Only (Notes & DPP Notes)**"
                allowed_content_types = ["notes", "dppNotes"]

            await editable.edit(f"✅ **Batch Selected:** `{batch_name}`\n\n{token_mode_str}")
            await asyncio.sleep(2)

            start_time = time.time()
            subjects = batch_data.get("subjects", [])
            total_subjects = len(subjects)

            json_data = {batch_name: {}}
            all_urls = {}
            zip_path = f"{clean_name}.zip"

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for idx, sub in enumerate(subjects):
                    sub_name = sub.get("subject", "Unknown")
                    await update_status_card(
                        editable=editable,
                        task_name=f"Extracting: {batch_name}",
                        current=idx,
                        total=total_subjects,
                        start_time=start_time,
                        activity=f"Extracting subject: `{sub_name}`"
                    )

                    await process_pwwp_subject(
                        session,
                        sub,
                        batch_id,
                        batch_name,
                        zipf,
                        json_data,
                        all_urls,
                        auth_headers,
                        allowed_content_types
                    )

            await update_status_card(
                editable=editable,
                task_name=f"Extracting: {batch_name}",
                current=total_subjects,
                total=total_subjects,
                start_time=start_time,
                activity="Compiling final files..."
            )

            with open(f"{clean_name}.json", "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=4, ensure_ascii=False)

            with open(f"{clean_name}.txt", "w", encoding="utf-8") as f:
                for sub_urls in all_urls.values():
                    if sub_urls:
                        f.write("\n".join(sub_urls) + "\n")

            # UPLOAD TO TELEGRAM
            time_taken = format_time(time.time() - start_time)
            caption = (
                f"**Batch Name:** `{batch_name}`\n"
                f"**Mode:** `{ 'Paid (Video+PDF)' if is_purchased else 'Free (PDF Only)' }`\n"
                f"**Time Taken:** `{time_taken}`"
            )

            await editable.edit("📤 **Uploading generated documents to Telegram...**")
            uploaded_count = 0

            for ext in ["txt", "zip", "json"]:
                f_path = f"{clean_name}.{ext}"
                if os.path.exists(f_path) and os.path.getsize(f_path) > 0:
                    try:
                        with open(f_path, "rb") as doc:
                            await m.reply_document(doc, caption=caption, file_name=f"{clean_name}.{ext}")
                        uploaded_count += 1
                    except Exception as upload_err:
                        logging.error("Failed to upload %s: %s", f_path, upload_err)
                    finally:
                        if os.path.exists(f_path):
                            os.remove(f_path)

            if uploaded_count == 0:
                await editable.edit("**Extraction completed, but no content or links were found. ❌**")
            else:
                await editable.delete()

    except ProcessCancelledException:
        if clean_name:
            for ext in ["txt", "zip", "json"]:
                f_path = f"{clean_name}.{ext}"
                if os.path.exists(f_path):
                    try:
                        os.remove(f_path)
                    except Exception:
                        pass

    except Exception as e:
        logging.exception("Error in process_pwwp:")
        try:
            await editable.edit(f"**Error : {e}**")
        except Exception:
            pass


# ======================= TODAY'S CLASS FEATURE =======================

PW_ORG_ID = "5eb393ee95fab7468a79d189"
IST = timezone(timedelta(hours=5, minutes=30))


def _base_api_headers() -> Dict:
    return {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "origin": "https://www.pw.live",
        "referer": "https://www.pw.live/",
        "user-agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "Chrome/148.0.0.0 Safari/537.36"
        ),
        "client-id": PW_ORG_ID,
        "client-type": "WEB",
        "content-type": "application/json",
        "randomid": str(uuid.uuid4()),
        "x-sdk-version": "0.0.25",
    }


async def pw_login_flow(bot, m, editable, session, user_id, api_headers) -> Optional[Dict]:
    """Token or phone+OTP login. Returns auth headers or None."""
    base_payload = {"organizationId": PW_ORG_ID}

    raw_input = await prompt_user(
        bot, m, editable,
        "**Enter Your Account Access Token or Phone Number:**",
        user_id
    )

    if raw_input.isdigit() and len(raw_input) == 10:
        await editable.edit("**Sending OTP to registered phone... ⏳**")
        async with session.post(
            "https://api.penpencil.co/v1/users/get-otp-secure?smsType=0",
            headers={**api_headers, "randomid": str(uuid.uuid4())},
            json={**base_payload, "username": raw_input, "countryCode": "+91"}
        ) as resp:
            if resp.status == 429:
                await editable.edit("**Too many OTP requests ⏳ Wait a few minutes and try again.**")
                return None
            if resp.status >= 400:
                await editable.edit("**OTP Request Failed ❌**")
                return None

        otp = await prompt_user(bot, m, editable, "**Enter OTP received on phone:**", user_id)
        if not otp.isdigit():
            await editable.edit("**Invalid OTP format! ❌**")
            return None

        await editable.edit("**Verifying OTP... ⏳**")
        async with session.post(
            "https://api.penpencil.co/v3/oauth/token",
            headers={**api_headers, "randomid": str(uuid.uuid4())},
            json={
                **base_payload,
                "client_id": "system-admin",
                "grant_type": "password",
                "latitude": 0,
                "longitude": 0,
                "username": raw_input,
                "otp": str(otp)
            }
        ) as resp:
            if resp.status >= 400:
                await editable.edit("**Login Failed ❌**")
                return None
            res_data = await resp.json()
            access_token = (res_data.get("data") or {}).get("access_token")

        if not access_token:
            await editable.edit("**Login Failed ❌ Invalid response.**")
            return None
    else:
        access_token = raw_input.strip()
        if access_token.lower().startswith("bearer "):
            access_token = access_token[7:].strip()

    if not access_token:
        await editable.edit("**Invalid Access Token ❌**")
        return None

    return {**api_headers, "authorization": f"Bearer {access_token}"}


async def pw_select_batch_flow(bot, m, editable, session, user_id, auth_headers) -> Optional[Dict]:
    """Search batch by name and let user pick. Returns the selected course dict."""
    batch_search = await prompt_user(bot, m, editable, "**Enter Batch Name to Search:**", user_id)
    await editable.edit("**Searching courses online... 🔍**")

    courses_res = await fetch_pwwp_data(
        session,
        "https://api.penpencil.co/v3/batches/search",
        headers=auth_headers,
        params={"name": batch_search}
    )

    if courses_res and courses_res.get("_auth_error"):
        await editable.edit("🔐 **Authorization Failed ❌ HTTP 401**")
        return None

    courses = courses_res.get("data", []) if courses_res else []
    if not courses:
        await editable.edit("❌ **No Batches Found!**")
        return None

    text_list = "\n".join(
        [f"<blockquote>**{i + 1}.** `{c.get('name', 'Batch')}`</blockquote>" for i, c in enumerate(courses)]
    )
    idx_str = await prompt_user(
        bot, m, editable,
        f"**Select Course Index:**\n\n{text_list}",
        user_id
    )

    if not idx_str.isdigit() or not (1 <= int(idx_str) <= len(courses)):
        await editable.edit("**Invalid Selection ❌**")
        return None

    return courses[int(idx_str) - 1]


def _fmt_ist(iso_str: Optional[str]) -> str:
    if not iso_str:
        return "--:--"
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST).strftime("%I:%M %p")
    except Exception:
        return "--:--"


def _name_of(x: Any) -> str:
    if isinstance(x, dict):
        return x.get("name") or x.get("subject") or ""
    return str(x) if x else ""


def format_today_classes(items: List[Dict], batch_name: str) -> List[str]:
    """Returns message chunks (<4000 chars each) for Telegram."""
    today = datetime.now(IST).strftime("%d %b %Y")
    header = f"📅 **Today's Classes — {today}**\n📚 `{batch_name}`\n\n"

    def sort_key(it):
        return (it.get("startTime") or "")

    blocks = []
    for n, it in enumerate(sorted(items, key=sort_key), 1):
        topic = it.get("topic") or "Untitled"
        subject = _name_of(it.get("subjectId")) or _name_of(it.get("subject"))
        teachers = it.get("teacherIds") or []
        teacher = ", ".join(filter(None, (_name_of(t) if isinstance(t, dict) else "" for t in teachers)))
        if not teacher:
            teacher = ""
        if not teacher and teachers and isinstance(teachers[0], dict):
            t0 = teachers[0]
            teacher = ((t0.get("firstName") or "") + " " + (t0.get("lastName") or "")).strip()

        status = "🔴 LIVE" if it.get("isLive") else ""
        url = extract_url_from_video_details(it)

        lines = [
            f"**{n}. {topic}** {status}".strip(),
            f"⏰ {_fmt_ist(it.get('startTime'))} - {_fmt_ist(it.get('endTime'))}",
        ]
        if subject:
            lines.append(f"📖 {subject}")
        if teacher:
            lines.append(f"👨‍🏫 {teacher}")
        if url:
            lines.append(f"🔗 `{url}`")
        blocks.append("<blockquote>" + "\n".join(lines) + "</blockquote>")

    chunks, cur = [], header
    for b in blocks:
        if len(cur) + len(b) + 2 > 3800:
            chunks.append(cur)
            cur = ""
        cur += b + "\n"
    if cur.strip():
        chunks.append(cur)
    return chunks


async def process_pwwp_today(bot: Client, m: Message, user_id: int):
    api_headers = _base_api_headers()
    editable = await m.reply_text("**Wait initializing process... ⏳**")

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60)
        ) as session:

            auth_headers = await pw_login_flow(bot, m, editable, session, user_id, api_headers)
            if not auth_headers:
                return

            course = await pw_select_batch_flow(bot, m, editable, session, user_id, auth_headers)
            if not course:
                return

            batch_id = course["_id"]
            batch_name = course.get("name", "Batch")

            await editable.edit(f"📅 **Fetching today's classes for** `{batch_name}` ... ⏳")

            res = await fetch_pwwp_data(
                session,
                f"https://api.penpencil.co/v1/batches/{batch_id}/todays-schedule",
                headers=auth_headers
            )

            if res and res.get("_auth_error"):
                await editable.edit("🔐 **Authorization Failed ❌ HTTP 401**")
                return

            if not res:
                await editable.edit("**Could not fetch today's schedule ❌ (API error, check logs).**")
                return

            data = res.get("data")
            if isinstance(data, dict):  # some responses wrap the list
                data = data.get("schedules") or data.get("data") or []
            items = data or []

            if not items:
                await editable.edit(f"📭 **No classes scheduled today for** `{batch_name}`")
                return

            chunks = format_today_classes(items, batch_name)
            await editable.edit(chunks[0])
            for extra in chunks[1:]:
                await m.reply_text(extra)

    except ProcessCancelledException:
        pass
    except Exception as e:
        logging.exception("Error in process_pwwp_today:")
        try:
            await editable.edit(f"**Error : {e}**")
        except Exception:
            pass


def pwwp_menu_buttons() -> InlineKeyboardMarkup:
    """Use this wherever your PW menu is shown."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Extract Batch", callback_data="pwwp")],
        [InlineKeyboardButton("📅 Today's Class", callback_data="pwwp_today")],
    ])


def register_pwwp_handlers(bot: Client):

    # Captures user text input for prompt_user/ask_user
    @bot.on_message(filters.text & ~filters.command(["start", "help"]))
    async def capture_user_input(client: Client, message: Message):
        user_id = message.from_user.id
        if user_id in USER_EVENTS:
            USER_RESPONSES[user_id] = message.text
            USER_EVENTS[user_id].set()

    @bot.on_callback_query(filters.regex("^pwwp$"))
    async def pwwp_callback(client: Client, callback_query):
        user_id = callback_query.from_user.id
        await callback_query.answer()

        asyncio.create_task(
            process_pwwp(client, callback_query.message, user_id)
        )

    @bot.on_callback_query(filters.regex("^pwwp_today$"))
    async def pwwp_today_callback(client: Client, callback_query):
        user_id = callback_query.from_user.id
        await callback_query.answer()

        asyncio.create_task(
            process_pwwp_today(client, callback_query.message, user_id)
        )
