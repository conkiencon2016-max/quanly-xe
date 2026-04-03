from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

# ===== CONFIG =====
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'credentials.json'

FOLDER_ID = "1Jb1APNiFf5A_h8J8S-27K4fcEl4SRLUH?usp=drive_link"

# ===== AUTH =====
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build('drive', 'v3', credentials=creds)

# ===== UPLOAD FILE =====
def upload_file(file_path):

    file_name = os.path.basename(file_path)

    file_metadata = {
        'name': file_name,
        'parents': [FOLDER_ID]
    }

    media = MediaFileUpload(file_path, resumable=True)

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    print("Uploaded:", file_name)


# ===== MAIN =====
if __name__ == "__main__":

    BACKUP_DIR = "backups"

    for f in os.listdir(BACKUP_DIR):

        path = os.path.join(BACKUP_DIR, f)

        if os.path.isfile(path):
            upload_file(path)
