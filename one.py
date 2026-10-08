import os
import random
import json
import logging
import time
import uuid
import asyncio
from typing import List, Dict, Any
import aiohttp
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from helpers import ask_user, is_authorized
# Aapke pehle se bane helper functions (fetch_pwwp_data, extract_attachment_url, format_time, update_status_card, prompt_user)

# ==========================================
# 1. RANDOM TOKEN POOL
# ==========================================
# Yahan apne sabhi PW working access tokens daal dein
PW_TOKENS = [
    "YOUR_PW_ACCESS_TOKEN_1",
    "YOUR_PW_ACCESS_TOKEN_2",
    "YOUR_PW_ACCESS_TOKEN_3",
]

def get_random_pw_token() -> str:
    """Returns a random access token from the pool."""
    if not PW_TOKENS or PW_TOKENS[0].startswith("YOUR_PW"):
        logging.warning("No tokens configured in PW_TOKENS pool!")
    return random.choice(PW_TOKENS)


# ==========================================
# 2. ONLY PDF EXTRACTION FOR TODAY'S SCHEDULE
# ==========================================
async def get_pwwp_todays_pdf_details(
    session: aiohttp.ClientSession,
    selected_batch_id: str,
    subject_id: str,
    schedule_id: str,
    headers: Dict
) -> List[str]:

    url = (
        f"https://api.penpencil.co/v1/batches/"
        f"{selected_batch_id}/subject/{subject_id}/schedule/"
        f"{schedule_id}/schedule-details"
    )

    data = await fetch_pwwp_data(session, url, headers=headers)
    pdf_content = []

    if (
        data
        and not data.get("_auth_error")
        and not data.get("_404")
        and data.get("success")
        and data.get("data")
    ):
        item = data["data"]
        topic = item.get("topic", "Class Material")

        # Extract Homework / Class Notes PDFs
        homeworks = item.get("homeworkIds", []) or []
        
        # Extract DPP PDFs
        if item.get("dpp"):
            dpp_hws = item.get("dpp", {}).get("homeworkIds", []) or []
            homeworks.extend(dpp_hws)

        for hw in homeworks:
            hw_topic = hw.get("topic", topic)
            for att in hw.get("attachmentIds", []) or []:
                u = extract_attachment_url(att)
                
                # Strict PDF Check
                if u and (".pdf" in u.lower() or u.endswith(".pdf")):
                    pdf_content.append(f"📄 {hw_topic}\n🔗 {u}\n\n")

    return pdf_content


async def get_todays_all_pdfs(
    session: aiohttp.ClientSession,
    selected_batch_id: str,
    headers: Dict,
    editable: Message,
    start_time: float
) -> List[str]:

    # Primary Endpoint
    url = f"https://api.penpencil.co/v1/batches/{selected_batch_id}/todays-schedule"
    data = await fetch_pwwp_data(session, url, headers=headers)

    # Fallback to Microservice Endpoint if 404
    if data and data.get("_404"):
        url_fallback = f"https://api.penpencil.co/batch-service/v1/batches/{selected_batch_id}/todays-schedule"
        data = await fetch_pwwp_data(session, url_fallback, headers=headers)

    all_pdfs = []

    if data and data.get("_404"):
        logging.info("No schedule found for today (404) for batch %s", selected_batch_id)
        return all_pdfs

    if (
        data
        and not data.get("_auth_error")
        and data.get("success")
        and data.get("data")
    ):
        schedules = data["data"]
        total_items = len(schedules)

        for idx, item in enumerate(schedules):
            await update_status_card(
                editable=editable,
                task_name="Extracting Today's PDFs (Notes/DPP)",
                current=idx + 1,
                total=total_items,
                start_time=start_time,
                activity=f"Searching PDFs in Subject Class ({idx + 1}/{total_items})"
            )

            res = await get_pwwp_todays_pdf_details(
                session,
                selected_batch_id,
                item.get("batchSubjectId"),
                item.get("_id"),
                headers
            )
            all_pdfs.extend(res)

    return all_pdfs


# ==========================================
# 3. PROCESSOR FUNCTION
# ==========================================
async def process_pwwp_today_pdfs(bot: Client, m: Message, user_id: int):
    # Select Random Token
    token = get_random_pw_token()
    
    if not token or token.startswith("YOUR_PW"):
        await m.reply_text("❌ **Error:** Token pool me koi valid token setup nahi hai.")
        return

    auth_headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "origin": "https://www.pw.live",
        "referer": "https://www.pw.live/",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/148.0.0.0 Safari/537.36",
        "client-id": "5eb393ee95fab7468a79d189",
        "client-type": "WEB",
        "content-type": "application/json",
        "randomid": str(uuid.uuid4()),
        "x-sdk-version": "0.0.25",
        "authorization": f"Bearer {token}"
    }

    editable = await m.reply_text("🎲 **Random PW Token Picked!** Searching today's PDFs... ⏳")
    clean_name = None

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            # 1. Ask Batch Name
            batch_search = await prompt_user(
                bot, m, editable, "**Enter Batch Name to search Today's PDFs:**", user_id
            )

            await editable.edit("🔍 **Searching Batch...**")

            courses_res = await fetch_pwwp_data(
                session,
                "https://api.penpencil.co/v3/batches/search",
                headers=auth_headers,
                params={"name": batch_search}
            )

            if not courses_res or not courses_res.get("data"):
                await editable.edit("❌ **No Batches Found or Token Expired!**")
                return

            courses = courses_res.get("data", [])
            text_list = "\n".join([f"**{i+1}.** `{c.get('name')}`" for i, c in enumerate(courses)])

            idx_str = await prompt_user(
                bot, m, editable, f"**Select Course Number:**\n\n{text_list}", user_id
            )

            if not idx_str.isdigit() or not (1 <= int(idx_str) <= len(courses)):
                await editable.edit("❌ **Invalid Selection!**")
                return

            selected_course = courses[int(idx_str) - 1]
            batch_id = selected_course["_id"]
            batch_name = selected_course.get("name", "Batch")
            clean_name = batch_name.replace("/", "-").replace("|", "-")

            start_time = time.time()

            # 2. Extract Today's PDFs
            pdf_data = await get_todays_all_pdfs(
                session, batch_id, auth_headers, editable, start_time
            )

            if not pdf_data:
                await editable.edit("ℹ️ **Aaj is batch me koi PDF / Notes / DPP PDF available nahi hai.**")
                return

            # 3. Save to File
            file_path = f"{clean_name}_Today_PDFs.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(pdf_data)

            time_taken = format_time(time.time() - start_time)
            caption = (
                f"📚 **Batch:** `{batch_name}`\n"
                f"📄 **Total PDFs Found:** `{len(pdf_data)}`\n"
                f"⏱️ **Time Taken:** `{time_taken}`"
            )

            # 4. Upload File
            await editable.edit("📤 **Uploading PDF Links Document...**")
            
            with open(file_path, "rb") as doc:
                await m.reply_document(doc, caption=caption, file_name=file_path)

            await editable.delete()

    except Exception as e:
        logging.exception("Error in process_pwwp_today_pdfs:")
        await editable.edit(f"❌ **Error:** `{e}`")
    finally:
        if clean_name:
            f_path = f"{clean_name}_Today_PDFs.txt"
            if os.path.exists(f_path):
                os.remove(f_path)


# ==========================================
# 4. HANDLER REGISTRATION & BUTTON INLINE MENU
# ==========================================
def register_pwwp_pdf_handlers(bot: Client):

    # Button Handler for Today's PDFs
    @bot.on_callback_query(filters.regex("^pwwp_today_pdfs$"))
    async def pwwp_today_pdfs_callback(client: Client, callback_query):
        user_id = callback_query.from_user.id
        await callback_query.answer()
        asyncio.create_task(process_pwwp_today_pdfs(client, callback_query.message, user_id))


# Telegram Command Menu Example Button markup
def get_pwwp_inline_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📥 Full PW Downloader", callback_data="pwwp"),
            InlineKeyboardButton("📄 Today's PDFs Only (Random Token)", callback_data="pwwp_today_pdfs")
        ]
    ])
