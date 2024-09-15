import os.path
import io

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]


class GoogleDrive:
    def __init__(self) -> None:
        creds = None
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", SCOPES
                )
                creds = flow.run_local_server(port=8080)

            with open("token.json", "w") as token:
                token.write(creds.to_json())
        service = build("drive", "v3", credentials=creds)
        self._service = service

    """
    List all files from a shared folder, this is limited to 1000 files per folder.
    """

    def listFilesFromSharedFolder(self, folderId: str) -> list[any]:
        q = f"'{folderId}' in parents"
        try:
            results = (
                self._service.files()
                .list(
                    # This needs to be rewritten to also be recursive.
                    pageSize=1000,
                    fields="nextPageToken, files(*)",
                    q=q,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                )
                .execute()
            )
        except HttpError as error:
            print(f"An error occurred: {error}")
            exit(1)
        return results.get("files", [])

    """
    Downloads file from drive using its ID as reference.
    """

    def download(self, fileID: str):
        try:
            request = self._service.files().get_media(fileId=fileID)
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                _, done = downloader.next_chunk()

        except HttpError as error:
            print(f"An error occurred: {error}")
            exit(1)

        return file.getvalue()
