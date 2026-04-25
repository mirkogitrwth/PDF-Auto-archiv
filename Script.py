import os
import json
import time
import io
import pypdf
from google import genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# --- KONFIGURATION ---
SCAN_FOLDER_ID = '1h3f-WZhYQFTKO4lAWYRiFZ6OCwXF7xkr' 
ARCHIVE_BASE_ID = '1XKGNecC9kyW9jtGZKJ_lSenxpYOBNMWG'

# --- SETUP ---
creds_info = json.loads(os.environ['GOOGLE_CREDENTIALS'])
creds = service_account.Credentials.from_service_account_info(
    creds_info, scopes=['https://www.googleapis.com/auth/drive'])
drive_service = build('drive', 'v3', credentials=creds)

# API Client
client = genai.Client(api_key=os.environ['GEMINI_API_KEY'])

def get_or_create_folder(name, parent_id):
    query = f"name = '{name}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    response = drive_service.files().list(q=query).execute()
    files = response.get('files', [])
    if files:
        return files[0]['id']
    file_metadata = {'name': name, 'parents': [parent_id], 'mimeType': 'application/vnd.google-apps.folder'}
    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
    return folder.get('id')

def process_files():
    query = f"'{SCAN_FOLDER_ID}' in parents and mimeType = 'application/pdf' and trashed = false"
    results = drive_service.files().list(q=query).execute()
    files = results.get('files', [])

    if not files:
        print("☕️ Keine neuen PDFs gefunden.")
        return

    for file_info in files:
        file_id = file_info['id']
        filename = file_info['name']
        print(f"\n📂 Verarbeite: {filename}...")

        request = drive_service.files().get_media(fileId=file_id)
        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        
        file_stream.seek(0)
        with open("temp.pdf", "wb") as f:
            f.write(file_stream.getbuffer())
        
        # --- KORREKTUR HIER: 'file' statt 'path' ---
        sample_file = client.files.upload(file="temp.pdf")
        
        while sample_file.state.name == "PROCESSING":
            time.sleep(2)
            sample_file = client.files.get(name=sample_file.name)

        prompt = """
        Du bist ein professioneller Archivar. Sieh dir diese gescannten Seiten an.
        Es können mehrere verschiedene Dokumente in dieser einen PDF sein.

        1. Identifiziere jedes einzelne Dokument.
        2. Bestimme den Zeitraum (YYYY-MM), den Typ und den Absender.
        3. Gib mir die Seitenzahlen an (die erste Seite ist 0).

        Antworte NUR mit einem JSON-Array:
        [
          {"filename": "2024-03_Rechnung_Telekom", "folder": "Rechnungen", "pages": [0]},
          {"filename": "2024-01_Versicherungsschein_HUK", "folder": "Versicherung", "pages": [1, 2]}
        ]

        Kategorien: Gehalt, Versicherung, Steuern, Wohnung, Gesundheit, Sonstiges.
        """
        
        # --- KORREKTUR HIER: Modellname ohne 'models/' Präfix ---
        response = client.models.generate_content(
            model='gemini-2.0-flash', 
            contents=[sample_file, prompt]
        )

        try:
            clean_json = response.text.replace('```json', '').replace('```', '').strip()
            instructions = json.loads(clean_json)
        except Exception as e:
            print(f"❌ Fehler beim Lesen der KI-Antwort: {e}")
            continue

        reader = pypdf.PdfReader("temp.pdf")
        for doc in instructions:
            writer = pypdf.PdfWriter()
            for p_num in doc['pages']:
                if p_num < len(reader.pages):
                    writer.add_page(reader.pages[p_num])
            
            output_stream = io.BytesIO()
            writer.write(output_stream)
            output_stream.seek(0)

            target_folder_id = get_or_create_folder(doc['folder'], ARCHIVE_BASE_ID)
            
            split_filename = f"split_temp.pdf"
            with open(split_filename, "wb") as f:
                f.write(output_stream.getbuffer())
            
            file_metadata = {'name': f"{doc['filename']}.pdf", 'parents': [target_folder_id]}
            media = MediaFileUpload(split_filename, mimetype='application/pdf')
            drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f"   ✅ Archiviert: {doc['folder']}/{doc['filename']}.pdf")
            os.remove(split_filename)

        drive_service.files().delete(fileId=file_id).execute()
        print(f"🏁 {filename} erledigt.")
        os.remove("temp.pdf")
        time.sleep(15)

if __name__ == "__main__":
    process_files()
