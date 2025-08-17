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

def getDataAndSendMessage(identifier,inputMessage):
    service = authenticate_google_sheets()

    sheet = service.spreadsheets()

    ID_RANGE_DICTIONARY = {
        "1YhJ7Fim_C9nKV-u8uh0xB6OZhxG5TvMljkPVeHI1U2k": ["[Mar25] List Result from BI","[Feb25] List Result from BI","[Jan25] List Result"],
        "1pJfWQweGxEr1V7ANy3H9iW0caN78g53h2ckGA8UasQU": ["June_Data"]
    }

    all_values = {}
    result_rows = {}
    
    for spreadsheet_id, sheet_names in ID_RANGE_DICTIONARY.items():
        for sheet_name in sheet_names:
            rng = safe_range(sheet_name, "A1:ZZ999999")
            result = sheet.values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
            values = result.get("values", [])
            all_values.setdefault(spreadsheet_id, {})[sheet_name] = values

    lookup_col = 2
    
    lookup_col = 2  # zero-based
    identifier_key = str(identifier).strip()

    # first match per sheet/range
    per_sheet_hits = {}           # {spreadsheet_id: {range_name: {"row_index": i, "row": [...]}}}

    for spreadsheet_id, sheets in all_values.items():
        for sheet_name, rows in sheets.items():  # was: rng
            if not rows:
                continue
            found = None
            for r_idx, row in enumerate(rows):
                if len(row) > lookup_col and str(row[lookup_col]).strip() == identifier_key:
                    found = {"row_index": r_idx, "row": row}
                    break
            if found:
                per_sheet_hits.setdefault(spreadsheet_id, {})[sheet_name] = found
                result_rows[sheet_name] = found["row"]  # <-- key-value pair
    
    if result_rows == {}:
        sendMessage(f"**Error:** **{identifier}** not found.") 
        return

    prompt = generate_AI_prompt(inputMessage,result_rows)

    AI_resp = model.generate_content(
    prompt)

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
        import json
        return json.dumps(resp.to_dict(), ensure_ascii=False)
    except Exception:
        return "[No text content returned by the model.]"

def generate_AI_prompt(message, dataFromSheet):
    if isinstance(dataFromSheet, str):
        data_block = dataFromSheet
    else:
        data_block = str(dataFromSheet)

    header = header = """Role: Careful data analyst. Use ONLY pasted data.

JOB
- Answer exactly what’s asked.
- Single-month question → single-month answer only.
- Compute MoM percentage change if delta cues appear (vs|delta|change|MoM|m/m|month on month). Be smart. Identify possible synonyms for these cues.

METRICS
- GMV = absolute currency.
- video/live/showcase GMV = per-channel absolute; if total GMV + share exist ⇒ channel = total×share.
- “contribution/share/%/mix” = percentage (not amount).
- Accept shares from any list of dicts with *_gmv keys.

PARSE
- Month keys: bracketed “[Mar25] …” and plain “June_Data/Feb Summary”.
- Year inference: if any month has a year, apply that year to month-only names (e.g., “June” → 2025-06). Do not mark ambiguous.
- Normalize months to YYYY-M
- GMV total: use the clearest total; ignore “K₫/M₫” price ranges; if multiple numeric candidates, pick the largest plain number; treat 759,662→759662; “NaN/Infinity” = missing; “95%”→0.95 when needed.
- If only a share exists but an absolute was requested → say absolute unavailable; report share (and compute absolute only if total exists).

OUTPUT
- 1–2 sentences; no preamble or narration; no JSON.
- Currency with commas; shares as percentages (2 decimals).

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
        
        threading.Thread(target=getDataAndSendMessage, args=(inputString[0], user_message)).start()
        return Response("", status=200)
    else:
        return Response("", status=204)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8888)