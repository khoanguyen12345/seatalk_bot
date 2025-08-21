from flask import Flask, request, Response
import json
import hashlib
import requests
import json
from googleapiclient.discovery import build
from google.oauth2 import service_account
from typing import Dict, Any
from dotenv import load_dotenv
import os
import threading
import re
import google.generativeai as genai

load_dotenv()

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel("gemini-2.0-flash")

app = Flask(__name__)

SIGNING_SECRET = os.getenv("SIGNING_SECRET").encode("utf-8")
SEATALK_MESSAGE_URL = os.getenv("SEATALK_WEBHOOK")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
BOT_NAME = "KOL Information"

EVENT_VERIFICATION = "event_verification"
NEW_BOT_SUBSCRIBER = "new_bot_subscriber"
MESSAGE_FROM_BOT_SUBSCRIBER = "message_from_bot_subscriber"
INTERACTIVE_MESSAGE_CLICK = "interactive_message_click"
BOT_ADDED_TO_GROUP_CHAT = "bot_added_to_group_chat"
BOT_REMOVED_FROM_GROUP_CHAT = "bot_removed_from_group_chat"
NEW_MENTIONED_MESSAGE_RECEIVED_FROM_GROUP_CHAT = "new_mentioned_message_received_from_group_chat"
GOOGLE_CREDENTIALS_PATH='credentials.json'

EVENT_VERIFICATION = "event_verification"

def is_valid_signature(signing_secret: bytes, body: bytes, signature: str) -> bool:
    return hashlib.sha256(body + signing_secret).hexdigest() == signature


def sendMessage(message):
    messageContent = {
        "tag": "text",
        "text": {
            "format": 1,
            "content": message
        }
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(SEATALK_MESSAGE_URL, headers=headers, data=json.dumps(messageContent),timeout = 3.05)
    return response

def find_row_and_fetch(spreadsheet_id: str, sheet_name: str, identifier: str, service):
    values_api = service.spreadsheets().values()

    if identifier.isdigit():
        identifier = "https://www.tiktok.com/@"+identifier
        lookup_range = f"'{sheet_name}'!A:A"
    elif "tiktok.com" in identifier:
        lookup_range = f"'{sheet_name}'!A:A"
    else:
        lookup_range = f"'{sheet_name}'!C:C"

    col_resp = values_api.get(
        spreadsheetId=spreadsheet_id,
        range=lookup_range,
        valueRenderOption="UNFORMATTED_VALUE"
    ).execute()
    col_vals = col_resp.get("values", [])

    # normalize once for the input
    id_key = normalize_key(identifier)

    # 2) find first match using normalized comparison
    row_idx_0 = next(
        (i for i, row in enumerate(col_vals)
         if row and normalize_key(row[0]) == id_key),
        None
    )
    if row_idx_0 is None:
        return None

    # 3) fetch only that single row
    row_num = row_idx_0 + 1  # A1 is 1-based
    row_range = f"'{sheet_name}'!A{row_num}:ZZ{row_num}"
    row_resp = values_api.get(
        spreadsheetId=spreadsheet_id,
        range=row_range,
        valueRenderOption="UNFORMATTED_VALUE"
    ).execute()
    return (row_num, row_resp.get("values", [[]])[0])

def getDataAndSendMessage(identifier, inputMessage):
    service = authenticate_google_sheets()

    ID_RANGE_DICTIONARY = {
        "1YhJ7Fim_C9nKV-u8uh0xB6OZhxG5TvMljkPVeHI1U2k": ["[Mar25] List Result from BI","[Feb25] List Result from BI","[Jan25] List Result"],
        "1pJfWQweGxEr1V7ANy3H9iW0caN78g53h2ckGA8UasQU": ["June_Data"]
    }

    result_rows = {}
    for sid, tabs in ID_RANGE_DICTIONARY.items():
        for tab in tabs:
            hit = find_row_and_fetch(sid, tab, identifier, service)
            if hit:
                _, row = hit
                result_rows[tab] = row
                # if you only need the first match overall, you can break here

    if not result_rows:
        sendMessage(f"**Error:** **{identifier}** not found.")
        return

    prompt = generate_AI_prompt(inputMessage, result_rows)
    AI_resp = model.generate_content(prompt)
    sendMessage(gemini_text(AI_resp))

    return

#################################### HELPER FUNCTIONS ##########################################

def gemini_text(resp):
    try:
        t = (resp.text or "").strip()
        if t:
            return t
    except Exception:
        pass
    # Fallback to dict view if available
    try:
        return json.dumps(resp.to_dict(), ensure_ascii=False)
    except Exception:
        return "[No text content returned by the model.]"

def normalize_key(s: str) -> str:
    """Lowercase and strip all whitespace so 'hUng everything' == 'hungeverything'."""
    if s is None:
        return ""
    return re.sub(r"\s+", "", str(s).lower())

def generate_AI_prompt(message, dataFromSheet):
    if isinstance(dataFromSheet, str):
        data_block = dataFromSheet
    else:
        data_block = str(dataFromSheet)

    header = header = """Role: Careful data analyst. Use ONLY the pasted data.

JOB
- Answer exactly what’s asked.
- Single-month question → single-month answer only.
- Compute MoM % only if Δ cues appear (vs|delta|change|MoM|m/m|month on month).
- Do not mix months; prefer same-row signals, but you MAY use other rows from the same month’s sheet.

DATA FORMAT
- Input is EITHER:
  (a) a dict keyed by sheet names (e.g., "[Mar25] List Result from BI", "June_Data"), each value = ONE heterogeneous row (list), OR
  (b) a plain list-of-lists where each inner list is a row.
- A row contains strings, numbers, and stringified lists of dicts.
  • Channel shares = list of dicts with keys ending in *_gmv (video_gmv/live_gmv/showcase_gmv), values in 0..1.
  • Industry/Category shares = list of dicts like {"key": "...","name":"...","value": <0..1>} where keys do NOT end in *_gmv.
  • Ignore price ranges like "169.1K₫ - 9.7M₫" when searching for totals.

MONTH
- Month comes from the sheet key (e.g., “[Mar25]” → 2025-03; “June_Data” → June of inferred year).
- Year inference: if ANY month has a year, apply that year to month-only names (e.g., “June” → 2025-06). Do not mark ambiguous.
- Normalize months internally to YYYY-MM.

METRICS
- GMV = absolute currency.
- “contribution/share/%/mix” = percentage (not amount).
- Channels = *_gmv lists; Industries/Categories = non-*_gmv share lists.

TOTAL GMV (robust, deterministic)
- Resolve within the SAME month’s sheet (prefer same row). Stop at first success:
  1) A field labeled like “total gmv” (case/space-insensitive) → use that number.
  2) If a *_gmv share array exists, choose the LARGEST plain number within a ±10 numeric-token window AROUND that array.
  3) Otherwise, choose the single largest plain number in the month’s sheet that plausibly represents a total (≥4 digits or ≥1,000).
- Ignore percents, dates, NaN/Infinity, and price ranges/suffix numbers (₫/K₫/M₫) when selecting totals.

RESOLVING REQUESTED METRICS
- For live/video/showcase GMV (per-channel absolute):
  1) If an explicit per-channel value exists, use it.
  2) OTHERWISE, if total GMV + that channel’s share exist, compute (channel = total × share) or (channel = total × contribttuon %)
  3) Only if both fail → “insufficient data for <Month YYYY>”.
- For industry/category contribution:
  • Return shares for each industry/category (1 decimal), sorted descending.
  • If total GMV is resolved, ALSO provide computed amounts = share × total.
  • If the industry list is absent → “insufficient data for <Month YYYY>”.

MOM RESOLUTION
- When the question requests MoM:
  1) Identify the months (in the question’s order, or chronological if unspecified).
  2) For EACH month, resolve the requested metric using the rules above (explicit → share×total → insufficient).
  3) Compute deltas ONLY when BOTH months have absolute values. If one side is missing, state which month is missing and skip that pair.

NUMBERS & UNITS
- Clean numbers: 759,662→759662; “95%”→0.95; NaN/Infinity = missing.
- Currency output is USD. If input values carry ₫/K₫/M₫, convert to USD at 1 USD = 26,000 VND for output only.
- Currency format: compact ($818.9K, $2.39M, $3.61M). K=1 dp; M/B=2 dp.
- Percentages: 1 decimal.

OUTPUT (bullets ONLY — no preamble, no prose, no code, no JSON)
- The very first character must be “-”.
- One bullet per line; always label values as: <Metric> <Month YYYY>: <value>
- For MoM questions, list the month bullets first, then a final summary bullet:
  - <Metric> <From Mon YYYY> → <To Mon YYYY>: <+/-p%> (<+/-$Δ>)
- At the beginning of the OUTPUT, give a short introduction of the information you are going to give.
- In the body of ther OUTPUT, give information in human readable format, parsing any dictionary or lists.
- At the end of the OUTPUT, give suggestions on other information you can provide, based on the pasted data and headers of the KOL. If a suggestion cannot be answered with the data on hand (insufficient information), do not include it in the list. If there is no suggestion to be made, do not include this section in the OUTPUT.

DATA:
"""

    prompt = header + str(data_block) + "\n\nQUESTION:\n" + str(message)
    return prompt


def quote_sheet(name: str) -> str:
    # Wrap in single quotes and escape any single quotes inside the name
    return "'" + name.replace("'", "''") + "'"

def safe_range(sheet_name: str, a1: str = None) -> str:
    base = quote_sheet(sheet_name)  # -> "'[Feb25] List Result from BI'"
    return base if a1 is None else f"{base}!{a1}"

def xlookup(values, lookup_value, lookup_col_index):
    for row in values:
        if len(row) > lookup_col_index:
            if row[lookup_col_index] == lookup_value:
                return row
    return None


def authenticate_google_sheets():
    SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_CREDENTIALS_PATH")
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=credentials)
    return service

@app.route("/bot-callback", methods=["POST"])
def bot_callback_handler():
    body: bytes = request.get_data()
    signature: str = request.headers.get("signature")
    if not is_valid_signature(SIGNING_SECRET, body, signature):
        return ""
    data: Dict[str, Any] = json.loads(body)
    event_type: str = data.get("event_type", "")
    if event_type == EVENT_VERIFICATION:
        return data.get("event")
    elif event_type == NEW_BOT_SUBSCRIBER:
        pass
    elif event_type == MESSAGE_FROM_BOT_SUBSCRIBER:
        pass
    elif event_type == INTERACTIVE_MESSAGE_CLICK:
        pass
    elif event_type == BOT_ADDED_TO_GROUP_CHAT:
        pass
    elif event_type == NEW_MENTIONED_MESSAGE_RECEIVED_FROM_GROUP_CHAT:

        event = data.get("event", {})
        message_obj = event.get("message", {})
        sender = message_obj.get("sender", {})
        sender_employee_code = sender.get("employee_code", "")
        if sender_employee_code == os.getenv("SENDER_EMPLOYEE_CODE"):
            return Response("", status=200)
        plain_text = message_obj.get("text", {}).get("plain_text", "")

        user_message = plain_text
        mention_tag = "@" + BOT_NAME + " "
        user_message = user_message[len(mention_tag):].lstrip()

        inputString = user_message.split(" ",1)
        try:
            informationFields = inputString[1]
            fields = informationFields.split(" ")
        except:
            sendMessage("**Error:** No information requested.")
            return Response("", status=200)
        
        threading.Thread(target=getDataAndSendMessage, args=(normalize_key(inputString[0]), user_message)).start()
        return Response("", status=200)
    else:
        return Response("", status=204)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8888))
    app.run(host="0.0.0.0", port=port)