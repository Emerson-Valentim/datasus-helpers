from typing import TypedDict

import ports.reader.google_drive_api as gd
import ports.writer.pandas_parser as pp

ROOT_FOLDER = "1-6DTwFGkk8x-Z_bI5Qp_cTopCax-Fb2d"

"""
File structure that contains a reference to file in its source.
"""


class FileInfo(TypedDict):
    id: str
    source: str


def main():
    drive = gd.GoogleDrive()
    print("Authenticated! Job started.")
    results: list[FileInfo] = []
    files = recursiveSearch(
        drive=drive,
        targetID=ROOT_FOLDER,
        sourceName="root",
        targetMimeType="application/octet-stream",
        results=results,
    )

    folders = []
    filesToProcess = len(files)
    parser = pp.PandasParser()
    print("Downloading and converting files...")
    for index, file in enumerate(files):
        print(f"Processing {index+1}/{filesToProcess}.", end="\r", flush=True)
        destination = str.replace(f'{file["source"]}', "root/", "./csv/")
        destination = str.replace(destination, ".parquet/", "/")
        destination = str.replace(destination, ".parquet", ".csv")

        rawBytes = drive.download(fileID=file["id"])
        folder = parser.parquetBytesToCSV(rawBytes=rawBytes, dst=destination)
        if folder not in folders and not isSkipped(folder):
            folders.append(folder)

    print("")
    print("Merging files...")
    for folder in folders:
        print(f"Merging {folder}")
        parser.mergeCSVFiles(folder)


"""
Reads all files from a folder recursively and saves reference for .parquet files.
"""


def recursiveSearch(
    drive: gd.GoogleDrive,
    targetID: str,
    sourceName: str,
    targetMimeType: str,
    results: list[FileInfo],
):
    files = drive.listFilesFromSharedFolder(folderId=targetID)

    if len(files) == 0:
        return results

    for file in files:
        newSource = f"{sourceName}/{file['name']}"  # Corrected quote style for 'name'

        if file["mimeType"] == targetMimeType and file["name"].endswith(".parquet"):
            results.append({"id": file["id"], "source": newSource})
            continue

        if file["mimeType"] == "application/vnd.google-apps.folder":
            print(f"Scanning folder: {newSource}")
            recursiveSearch(
                drive=drive,
                targetID=file["id"],
                sourceName=newSource,
                targetMimeType=targetMimeType,
                results=results,
            )

    return results


"""
Verifies if a directory should be skipped.
"""


def isSkipped(dir: str) -> bool:
    for toSkip in ["tabelas_auxiliares", "ps_tratados"]:
        if toSkip in str.lower(dir):
            return True
    return False


main()
