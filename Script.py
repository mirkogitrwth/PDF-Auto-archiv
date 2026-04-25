import os
import json
import time
import io
import pypdf
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# --- KONFIGURATION ---
# Die IDs findest du in der Browser-URL deines Drive-Ordners
SCAN_FOLDER_ID = '1h3f-WZhYQFTKO4lAWYRiFZ6OCwXF7xkr' 
ARCHIVE_BASE_ID = '1XKGNecC9kyW9jtGZKJ_lSenxpYOBNMWG'

# --- SETUP ---
# Authentifizierung über GitHub Secrets
creds_info = json.loads(os.environ['GOOGLE_CREDENTIALS'])
creds = service_account.Credentials.from_service_account_info(
    creds_info, scopes=['https://www.googleapis.com/auth/drive'])
drive_service = build('drive', 'v3', credentials=creds)

genai.configure(api_key=os.environ['GEMINI_API_KEY'])
model = genai.GenerativeModel('gemini-flash-latest')

def get_or_create_folder(name, parent_id):
    """Findet einen Ordner im Archiv oder erstellt ihn."""
    query = f"name = '{name}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    response = drive_service.files().list(q=query).execute()
    files = response.get('files', [])
    if files:
        return files[0]['id']
    else:
        file_metadata = {'name': name, 'parents': [parent_id], 'mimeType': 'application/vnd.google-apps.folder'}
        folder = drive_service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')

def process_files():
    # 1. PDFs im Scan-Ordner suchen
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

        # 2. Datei in den Speicher laden
        request = drive_service.files().get_media(fileId=file_id)
        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        
        file_stream.seek(0)
        
        # 3. KI-Analyse (Vision)
        # Wir müssen die Datei temporär speichern, um sie hochzuladen
        with open("temp.pdf", "wb") as f:
            f.write(file_stream.getbuffer())
        
        sample_file = genai.upload_file(path="temp.pdf", display_name=filename)
        while sample_file.state.name == "PROCESSING":
            time.sleep(2)
            sample_file = genai.get_file(sample_file.name)

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
        
        response = model.generate_content([sample_file, prompt])
        genai.delete_file(sample_file.name) # KI-Cache aufräumen

        try:
            clean_json = response.text.replace('```json', '').replace('```', '').strip()
            instructions = json.loads(clean_json)
        except:
            print(f"❌ Fehler beim Lesen der KI-Antwort für {filename}")
            continue

        # 4. Splitting & Upload
        reader = pypdf.PdfReader("temp.pdf")
        for doc in instructions:
            writer = pypdf.PdfWriter()
            for p_num in doc['pages']:
                if p_num < len(reader.pages):
                    writer.add_page(reader.pages[p_num])
            
            output_stream = io.BytesIO()
            writer.write(output_stream)
            output_stream.seek(0)

            # Zielordner finden/erstellen
            target_folder_id = get_or_create_folder(doc['folder'], ARCHIVE_BASE_ID)
            
            # Hochladen
            file_metadata = {'name': f"{doc['filename']}.pdf", 'parents': [target_folder_id]}
            media = MediaFileUpload("temp.pdf", mimetype='application/pdf') # Platzhalter, wir nutzen den Stream:
            
            # Da MediaFileUpload eine Datei braucht, speichern wir den Split kurz
            split_filename = f"split_temp.pdf"
            with open(split_filename, "wb") as f:
                f.write(output_stream.getbuffer())
            
            media = MediaFileUpload(split_filename, mimetype='application/pdf')
            drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f"   ✅ Archiviert: {doc['folder']}/{doc['filename']}.pdf")
            os.remove(split_filename)

        # 5. Original löschen (oder in Unterordner verschieben)
        drive_service.files().delete(fileId=file_id).execute()
        print(f"🏁 {filename} erledigt und gelöscht.")
        os.remove("temp.pdf")
        
        print("Warte 20 Sek für Quota...")
        time.sleep(20)

if __name__ == "__main__":
    process_files()
