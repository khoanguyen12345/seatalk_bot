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

load_dotenv()

app = Flask(__name__)

SIGNING_SECRET = os.getenv("SIGNING_SECRET").encode()
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

def getDataAndSendMessage(identifier,informationList):
    RANGE = '[Mar25] List Result from BI!A1:BS'
    service = authenticate_google_sheets()

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE).execute()
    values = result.get('values', [])

    lookup_col = -1

    if identifier.isdigit():
        lookup_col = 1
    elif "tiktok.com" in identifier:
        lookup_col = 0
    else:
        lookup_col = 2

    result_cols = []
    valid_info_list = []
    result = []
    error_list = []

    for information in informationList:    
        if information == "overview":
            result_cols.extend([0,1,2,43,52,16])
            valid_info_list.extend(["TikTok Link","User ID","Channel Name","Total GMV","Total Commissions","GPM"])
            break
        elif information == "tier":
            result_cols.extend([61,62])
            valid_info_list.extend(["Live Tier", "Video Tier"])
            break
        elif information == "gmv":
            result_cols.append(43)
            valid_info_list.append("GMV")
        elif ("comm" in information) or ("earning" in information):
            result_cols.append(52)
            valid_info_list.append("Total Commissions")
        elif information == "gpm":
            result_cols.append(16)
            valid_info_list.append("GPM")
        elif information == "uid" or information == "user id":
            result_cols.append(1)
            valid_info_list.append("User ID")
        elif "link" in information and "tiktok" in information:
            result_cols.append(0)
            valid_info_list.append("TikTok Link")
        elif "name" in information:
            result_cols.append(2)
            valid_info_list.append("Channel Name")
        else: 
            error_list.append(information)
    
    result_row = xlookup(values,identifier,lookup_col)
    print(result_row)
    
    if result_row == None:
        sendMessage("KOL not found") 
        return

    for cols in result_cols:
        result.append(result_row[cols])

    resultString = ""

    for i in range(len(result)):
        raw_value = result[i]
        if valid_info_list[i] == "User ID":
            resultString += valid_info_list[i] + ": " + raw_value + "\n"
            continue

        try:
            num = float(raw_value)
            formatted_value = f"{num:,}"
            print(formatted_value)
        except ValueError:
            formatted_value = raw_value
            print(formatted_value)

        resultString += valid_info_list[i] + ": " + formatted_value + "\n"
    
    if len(error_list) > 0:
        error_string = ', '.join(error_list)
        resultString += "I was not able to find the following fields: " + error_string
    
    sendMessage(resultString)
    return

#################################### HELPER FUNCTIONS ##########################################
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
    # 1. validate the signature
    if not is_valid_signature(SIGNING_SECRET, body, signature):
        return ""
    # 2. handle events
    data: Dict[str, Any] = json.loads(body)
    event_type: str = data.get("event_type", "")
    if event_type == EVENT_VERIFICATION:
        return data.get("event")
    elif event_type == NEW_BOT_SUBSCRIBER:
    # fill with your own code
        pass
    elif event_type == MESSAGE_FROM_BOT_SUBSCRIBER:
    # fill with your own code
        pass
    elif event_type == INTERACTIVE_MESSAGE_CLICK:
    # fill with your own code
        pass
    elif event_type == BOT_ADDED_TO_GROUP_CHAT:
    # fill with your own code
        pass
    elif event_type == NEW_MENTIONED_MESSAGE_RECEIVED_FROM_GROUP_CHAT:

        event = data.get("event", {})
        message_obj = event.get("message", {})
        sender = message_obj.get("sender", {})
        sender_employee_code = sender.get("employee_code", "")
        if sender_employee_code == "9235642586":
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
            sendMessage("Please enter the information you want to see")
            return Response("", status=200)
        
        threading.Thread(target=getDataAndSendMessage, args=(inputString[0], fields)).start()
        return Response("", status=200)
    else:
        return Response("", status=204)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8888)