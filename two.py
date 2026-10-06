import asyncio
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
import aiohttp
from pyrogram import Client, filters
from pyrogram.types import Message

from helpers import ask_user, is_authorized


class ProcessCancelledException(Exception):
    """Custom exception raised when a process is cancelled by the user."""
    pass


def format_time(seconds: float) -> str:
    """Format seconds into a human-readable HH:MM:SS or MM:SS string."""
    seconds = int(seconds)
    mins, secs = divmod(seconds, 60)
    hrs, mins = divmod(mins, 60)
    if hrs > 0:
        return f"{hrs:02d}h {mins:02d}m {secs:02d}s"
    return f"{mins:02d}m {secs:02d}s"


async def prompt_user(bot: Client, message: Message, editable: Message, text: str, user_id: int) -> str:
    """Helper wrapper to ask user input with built-in /cancel check."""
    cancel_notice = "\n\n<blockquote>❌ **Send `/cancel` at any time to abort this process.**</blockquote>"
    full_text = text + cancel_notice
    
    response = await ask_user(bot, message, editable, full_text, user_id)
    
    if response is None or response.strip().lower() == "/cancel":
        try:
            await editable.edit("**Process Cancelled by User ❌**")
        except Exception:
            pass
        raise ProcessCancelledException("User requested cancellation.")
    
    return response.strip()


async def update_status_card(editable: Message, task_name: str, current: int, total: int, start_time: float, activity: str):
    """Formats and updates a progress tracking message card."""
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


SEMAPHORE = asyncio.Semaphore(10)


def transform_classplus_url(url_val: str) -> Tuple[str, bool]:
    """Converts Classplus preview image/thumbnail CDN URLs into valid m3u8 stream links."""
    is_video = False

    if any(ext in url_val.lower() for ext in (".m3u8", ".mpd", "mp4", "video")):
        return url_val, True

    # 1. Tencent CDN Master M3U8
    if "media-cdn.classplusapp.com/tencent/" in url_val:
        url_val = url_val.rsplit('/', 1)[0] + "/master.m3u8"
        return url_val, True

    # 2. Match CC, LC, UC, DY CDN folder paths with thumbnails
    if re.search(r'/(cc|lc|uc|dy)/', url_val):
        url_val = re.sub(r'thumbnail\.(png|jpg|jpeg)$', 'master.m3u8', url_val)
        return url_val, True

    # 3. Akamai / Azure Classplus Media Thumbnails
    if ("classplusapp.com" in url_val or "classplus.co" in url_val) and url_val.endswith(('.png', '.jpg', '.jpeg')):
        if "/media/" in url_val:
            base_path = url_val.rsplit('/', 1)[0]
            url_val = f"{base_path}/master.m3u8"
            return url_val, True

    # 4. Testbook VOD streams
    if "cpvideocdn.testbook.com" in url_val or "cpvod.testbook.com" in url_val:
        match = re.search(r'/streams/([a-f0-9]{24})/', url_val)
        if match:
            url_val = f'https://cpvod.testbook.com/{match.group(1)}/playlist.m3u8'
            return url_val, True

    # 5. Classplus DRM streams
    if "media-cdn.classplusapp.com/drm/" in url_val:
        parts = url_val.split('/')
        if len(parts) >= 5:
            video_id = parts[-3] if url_val.endswith(('.png', '.jpg', '.jpeg')) else parts[-2]
            url_val = f'https://media-cdn.classplusapp.com/drm/{video_id}/playlist.m3u8'
            return url_val, True

    return url_val, is_video


async def fetch_cpwp_signed_url(url_val: str, name: str, session: aiohttp.ClientSession, headers: Dict[str, str]) -> Optional[str]:
    async with SEMAPHORE:
        try:
            async with session.get("https://api.classplusapp.com/cams/uploader/video/jw-signed-url", params={"url": url_val}, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("url") or data.get("drmUrls", {}).get("manifestUrl") or data.get("signedUrl")
        except Exception as e:
            logging.error(f"Error fetching signed URL for {name}: {e}")
        return None


async def process_cpwp_url(url_val: str, name: str, session: aiohttp.ClientSession, headers: Dict[str, str]) -> Tuple[str, str]:
    """Returns a tuple of (formatted_string, content_type_category)."""
    transformed_url, is_video = transform_classplus_url(url_val)

    lower_url = transformed_url.lower()
    if ".pdf" in lower_url:
        return f"{name}:{transformed_url}\n", "pdf"
    if any(ext in lower_url for ext in (".jpg", ".jpeg", ".png", ".webp")):
        return f"{name}:{transformed_url}\n", "image"

    if not is_video:
        return f"{name}:{transformed_url}\n", "other"

    if "testbook.com" in transformed_url or "drm" in transformed_url:
        return f"{name}:{transformed_url}\n", "video"

    signed_url = await fetch_cpwp_signed_url(transformed_url, name, session, headers)
    final_url = signed_url if signed_url else transformed_url

    return f"{name}:{final_url}\n", "video"


async def get_cpwp_course_content(
    session: aiohttp.ClientSession,
    headers: Dict[str, str],
    Batch_Token: str,
    editable: Message,
    start_time: float,
    folder_id: int = 0,
    limit: int = 9999999999,
    progress_stats: Optional[Dict[str, int]] = None
) -> Tuple[List[str], int, int, int]:
    if progress_stats is None:
        progress_stats = {"processed": 0, "total": 1}

    MAX_RETRIES = 3
    fetched_urls: set = set()
    results: List[str] = []
    video_count = 0
    pdf_count = 0
    image_count = 0
    content_tasks: List[asyncio.Task] = []
    folder_tasks: List[asyncio.Task] = []

    contents: List[Dict[str, Any]] = []

    for retry in range(MAX_RETRIES):
        try:
            content_api = f'https://api.classplusapp.com/v2/course/preview/content/list/{Batch_Token}'
            params = {'folderId': folder_id, 'limit': limit}

            async with SEMAPHORE:
                async with session.get(content_api, params=params, headers=headers) as res:
                    res.raise_for_status()
                    res_json = await res.json()
                    contents = res_json.get('data', [])
                    break
        except Exception as e:
            logging.warning(f"Attempt {retry + 1} failed for folder {folder_id}: {e}")
            if retry == MAX_RETRIES - 1:
                return [], 0, 0, 0
            await asyncio.sleep(2 ** retry)

    progress_stats["total"] += len(contents)

    for content in contents:
        progress_stats["processed"] += 1
        current_item = content.get('name', 'Item')
        
        if progress_stats["processed"] % 5 == 0 or progress_stats["processed"] == progress_stats["total"]:
            await update_status_card(
                editable=editable,
                task_name="Scanning Course Contents",
                current=progress_stats["processed"],
                total=progress_stats["total"],
                start_time=start_time,
                activity=f"Scanning: `{current_item[:30]}`"
            )

        content_type = content.get('contentType')

        # Folder processing
        if content_type == 1:
            folder_task = asyncio.create_task(
                get_cpwp_course_content(
                    session, headers, Batch_Token, editable, start_time,
                    folder_id=content['id'], progress_stats=progress_stats
                )
            )
            folder_tasks.append(folder_task)
        else:
            name: str = content.get('name', '').strip()
            raw_url: Optional[str] = (
                content.get('url') 
                or content.get('videoUrl') 
                or content.get('streamUrl') 
                or content.get('thumbnailUrl')
            )

            if not raw_url:
                continue

            transformed_url, is_video = transform_classplus_url(raw_url)

            if transformed_url in fetched_urls:
                continue
            fetched_urls.add(transformed_url)

            lower_url = transformed_url.lower()

            # Categorize PDFs first
            if ".pdf" in lower_url or content_type == 4:
                pdf_count += 1
                results.append(f"{name}:{transformed_url}\n")
            # Categorize Images next
            elif any(ext in lower_url for ext in (".jpg", ".jpeg", ".png", ".webp")) or content_type == 5:
                image_count += 1
                results.append(f"{name}:{transformed_url}\n")
            # Categorize Videos
            elif is_video or content_type in (2, 3):
                task = asyncio.create_task(process_cpwp_url(raw_url, name, session, headers))
                content_tasks.append(task)
            # General fallback
            else:
                if ".pdf" in raw_url.lower():
                    pdf_count += 1
                elif any(ext in raw_url.lower() for ext in (".jpg", ".jpeg", ".png", ".webp")):
                    image_count += 1
                results.append(f"{name}:{transformed_url}\n")

    # Resolve async tasks and collect true counts
    if content_tasks:
        resolved_items = await asyncio.gather(*content_tasks, return_exceptions=True)
        for item in resolved_items:
            if isinstance(item, tuple) and len(item) == 2:
                line_str, category = item
                results.append(line_str)
                if category == "video":
                    video_count += 1
                elif category == "pdf":
                    pdf_count += 1
                elif category == "image":
                    image_count += 1

    # Resolve sub-folder async tasks
    if folder_tasks:
        resolved_folder_items = await asyncio.gather(*folder_tasks, return_exceptions=True)
        for res in resolved_folder_items:
            if isinstance(res, tuple) and len(res) == 4:
                nested_results, nested_video_count, nested_pdf_count, nested_image_count = res
                results.extend(nested_results)
                video_count += nested_video_count
                pdf_count += nested_pdf_count
                image_count += nested_image_count

    return results, video_count, pdf_count, image_count


async def search_courses_api(session: aiohttp.ClientSession, headers: dict, org_code: str, query: str) -> List[dict]:
    search_url = f"https://api.classplusapp.com/v2/course/preview/search?searchQuery={query}&limit=30"
    search_headers = {
        **headers,
        'tutorWebsiteDomain': f'https://{org_code}.courses.store'
    }
    async with session.get(search_url, headers=search_headers) as resp:
        if resp.status == 200:
            res_json = await resp.json()
            return res_json.get('data', {}).get('coursesData', []) or res_json.get('data', [])
    return []


async def get_enrolled_courses(session: aiohttp.ClientSession, headers: dict):
    purchased_url = "https://api.classplusapp.com/v2/course/purchased"
    async with session.get(purchased_url, headers=headers) as resp:
        if resp.status == 200:
            res_json = await resp.json()
            courses = res_json.get('data', {}).get('courses', []) or res_json.get('data', [])
            if isinstance(courses, list) and courses:
                return courses

    my_courses_url = "https://api.classplusapp.com/v2/course/list"
    async with session.get(my_courses_url, headers=headers) as resp:
        if resp.status == 200:
            res_json = await resp.json()
            courses = res_json.get('data', {}).get('courses', [])
            if isinstance(courses, list) and courses:
                return courses

    return []


async def process_cpwp(bot: Client, m: Message, user_id: int):
    headers = {
        'accept-encoding': 'gzip',
        'accept-language': 'EN',
        'api-version': '35',
        'app-version': '1.4.71.1',
        'build-number': '35',
        'connection': 'Keep-Alive',
        'content-type': 'application/json',
        'device-details': 'Xiaomi_Redmi 7_SDK-32',
        'device-id': 'cc4473819ba3ee7f51f560f801574304',
        'host': 'api.classplusapp.com',
        'region': 'IN',
        'user-agent': 'Mobile-Android',
        'webengage-luid': '00000187-6fe4-5d41-a530-26186858be4c'
    }

    connector = aiohttp.TCPConnector(limit=1000)
    async with aiohttp.ClientSession(connector=connector) as session:
        editable = None
        file_path = None
        try:
            editable = await m.reply_text("**Processing Classplus request... ⏳**")

            org_code = await prompt_user(bot, m, editable, "**Enter ORG Code Of Your Classplus App:**", user_id)
            org_code = org_code.lower().strip()

            raw_input = await prompt_user(bot, m, editable, "**Enter Access Token OR 10-digit Mobile Number:**", user_id)
            raw_input = raw_input.strip()

            access_token = None
            if raw_input.isdigit() and len(raw_input) == 10:
                await editable.edit("**Fetching Organization Info... ⏳**")

                org_info_url = f"https://api.classplusapp.com/v2/orgs/{org_code}"
                async with session.get(org_info_url, headers=headers) as org_resp:
                    if org_resp.status != 200:
                        await editable.edit(f"**Invalid Org Code ❌**\n`{await org_resp.text()}`")
                        return

                    org_data = await org_resp.json()
                    org_id = org_data.get("data", {}).get("orgId") or org_data.get("data", {}).get("id")

                if not org_id:
                    await editable.edit("**Could not resolve Organization ID ❌**")
                    return

                await editable.edit("**Sending OTP to Mobile Number... ⏳**")

                otp_headers = {**headers, "api-version": "52"}

                otp_req_payload = {
                    "countryExt": "91",
                    "mobile": raw_input,
                    "orgId": int(org_id),
                    "orgCode": org_code
                }
                async with session.post("https://api.classplusapp.com/v2/otp/generate", json=otp_req_payload, headers=otp_headers) as otp_resp:
                    if otp_resp.status != 200:
                        await editable.edit(f"**Failed to Send OTP ❌**\n`{await otp_resp.text()}`")
                        return

                    otp_data = await otp_resp.json()
                    session_id = otp_data.get("data", {}).get("sessionId")

                otp = await prompt_user(bot, m, editable, "**Enter OTP received on phone:**", user_id)
                if not otp.isdigit():
                    await editable.edit("**Invalid OTP ❌**")
                    return

                await editable.edit("**Verifying OTP... ⏳**")
                verify_payload = {
                    "otp": str(otp.strip()),
                    "countryExt": "91",
                    "sessionId": str(session_id),
                    "orgId": int(org_id),
                    "fingerprintId": headers.get("device-id", "cc4473819ba3ee7f51f560f801574304"),
                    "mobile": raw_input
                }
                async with session.post("https://api.classplusapp.com/v2/users/verify", json=verify_payload, headers=otp_headers) as verify_resp:
                    ver_json = await verify_resp.json()
                    if verify_resp.status != 200 or ver_json.get("status") == "failure":
                        err_msg = ver_json.get("message") or await verify_resp.text()
                        await editable.edit(f"**OTP Verification Failed ❌**\n`{err_msg}`")
                        return

                    access_token = (
                        ver_json.get("data", {}).get("token")
                        or ver_json.get("data", {}).get("user", {}).get("token")
                        or ver_json.get("token")
                    )

                if not access_token:
                    await editable.edit("**Failed to retrieve Access Token from Classplus ❌**")
                    return

                await editable.edit(f"**Classplus Login Successful ✅**\n\n**Token:** `{access_token}`")
                editable = await m.reply_text("**Wait Processing Your Request....**")
            else:
                access_token = raw_input

            headers['x-access-token'] = access_token

            await editable.edit("**Fetching Enrolled Courses... ⏳**")
            courses = await get_enrolled_courses(session, headers)

            if not courses:
                hash_headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': f'https://{org_code}.courses.store/?mainCategory=0',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
                }

                async with session.get(f"https://{org_code}.courses.store", headers=hash_headers) as response:
                    html_text = await response.text()
                    hash_match = re.search(r'["\']hash["\']\s*:\s*["\']([^"\']+)["\']', html_text)

                    if hash_match:
                        token = hash_match.group(1)
                        async with session.get(f"https://api.classplusapp.com/v2/course/preview/similar/{token}?limit=30", headers=headers) as resp:
                            if resp.status == 200:
                                res_json = await resp.json()
                                courses = res_json.get('data', {}).get('coursesData', [])

            if not courses:
                await editable.edit("**Didn't Find Any Course ❌**")
                return

            text = ''.join([
                f"<blockquote>**{cnt + 1}.** `\n{c.get('name')} 💵₹{c.get('finalPrice', c.get('price', 'N/A'))}`</blockquote>\n"
                for cnt, c in enumerate(courses)
            ])
            raw_text2 = await prompt_user(bot, m, editable, f"**Send index number of the Course**\n\n{text}\n**If Your Batch Not Listed Then Enter Your Batch Name**", user_id)

            course = None
            raw_text2 = raw_text2.strip()
            if raw_text2.isdigit() and 1 <= int(raw_text2) <= len(courses):
                course = courses[int(raw_text2) - 1]
            else:
                search_term = raw_text2.lower()
                matched_courses = [c for c in courses if search_term in c.get('name', '').lower()]

                if not matched_courses:
                    await editable.edit(f"**Searching for `{raw_text2}` across all app batches... ⏳**")
                    matched_courses = await search_courses_api(session, headers, org_code, raw_text2)

                if matched_courses:
                    text_search = ''.join([
                        f"<blockquote>**{cnt + 1}.** `\n{c.get('name')} 💵₹{c.get('finalPrice', c.get('price', 'N/A'))}`</blockquote>\n"
                        for cnt, c in enumerate(matched_courses)
                    ])
                    raw_text3 = await prompt_user(bot, m, editable, f"**Send index number of the Batch to download.**\n\n{text_search}", user_id)

                    raw_text3 = raw_text3.strip()
                    if raw_text3.isdigit() and 1 <= int(raw_text3) <= len(matched_courses):
                        course = matched_courses[int(raw_text3) - 1]
                    else:
                        await editable.edit("**Wrong Index Number ❌**")
                        return
                else:
                    await editable.edit("**Didn't Find Any Course Matching The Search Term ❌**")
                    return

            if not course:
                await editable.edit("**Invalid course selection ❌**")
                return

            selected_batch_id = course.get('id') or course.get('courseId')
            selected_batch_name = course.get('name', 'Course')
            clean_batch_name = re.sub(r'[\\/*?:"<>|]', "-", selected_batch_name)
            file_path = f"{clean_batch_name}.txt"

            batch_headers = {
                'Accept': 'application/json, text/plain, */*',
                'region': 'IN',
                'accept-language': 'EN',
                'Api-Version': '22',
                'x-access-token': access_token,
                'tutorWebsiteDomain': f'https://{org_code}.courses.store'
            }

            params = {'courseId': f'{selected_batch_id}'}

            async with session.get("https://api.classplusapp.com/v2/course/preview/org/info", params=params, headers=batch_headers) as info_resp:
                if info_resp.status == 200:
                    res_info = await info_resp.json()
                    Batch_Token = res_info['data']['hash']
                    App_Name = res_info['data']['name']

                    start_time = time.time()
                    await update_status_card(
                        editable=editable,
                        task_name=f"Extracting: {selected_batch_name}",
                        current=0,
                        total=100,
                        start_time=start_time,
                        activity="Initializing content crawler..."
                    )

                    course_content, video_count, pdf_count, image_count = await get_cpwp_course_content(
                        session, headers, Batch_Token, editable, start_time
                    )

                    if course_content:
                        await update_status_card(
                            editable=editable,
                            task_name=f"Extracting: {selected_batch_name}",
                            current=100,
                            total=100,
                            start_time=start_time,
                            activity="Saving content links to file..."
                        )

                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(''.join(course_content))

                        formatted_time = format_time(time.time() - start_time)

                        await editable.delete()

                        caption = f"**App Name : ```\n{App_Name}({org_code})```\nBatch Name : ```\n{selected_batch_name}``````\n🎬 : {video_count} | 📁 : {pdf_count} | 🖼  : {image_count}``````\nTime Taken : {formatted_time}```**"

                        with open(file_path, 'rb') as f:
                            await m.reply_document(document=f, caption=caption, file_name=f"{clean_batch_name}.txt")

                    else:
                        await editable.edit("**Didn't Find Any Content In The Course ❌**")
                else:
                    await editable.edit(f"**Error:** `{await info_resp.text()}`")

        except ProcessCancelledException:
            pass
        except Exception as e:
            logging.exception("Error in process_cpwp:")
            if editable:
                try:
                    await editable.edit(f"**Error : {e}**")
                except Exception:
                    pass
        finally:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception:
                    pass


def register_cpwp_handlers(bot: Client):
    @bot.on_callback_query(filters.regex("^cpwp$"))
    async def cpwp_callback(client: Client, callback_query):
        user_id = callback_query.from_user.id
        await callback_query.answer()
        asyncio.create_task(process_cpwp(client, callback_query.message, user_id))
