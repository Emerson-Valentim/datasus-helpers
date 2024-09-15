import io
import glob
import os
from pathlib import Path

import pandas as pd
from pyarrow import dataset as ds


class PandasParser:
    def __init__(self) -> None:
        return

    """
    Converts parquet bytes to csv file. It also create the directory if necessary.
    """

    def parquetBytesToCSV(self, rawBytes: bytes, dst: str):
        folder = self._assertDestination(dst)

        file = io.BytesIO(rawBytes)

        parser = pd.read_parquet(file)
        parser = parser.astype(str)

        parser.to_csv(dst, encoding="utf-8", index=False)

        return folder

    """
    Read all CSV files from a directory and merge into a single one. Then it deletes all files that were merged.
    """

    def mergeCSVFiles(self, dir: str):
        mergedFileName = "merged.csv"
        # Read all CSV files into a single pandas DataFrame
        all_files = glob.glob(os.path.join(dir, "*.csv"))
        try:
          all_files.remove(mergedFileName)
        except:
            pass
        dfs = [pd.read_csv(file, dtype=str) for file in all_files]

        merged_df = pd.concat(dfs, ignore_index=True)

        merged_file = os.path.join(dir, mergedFileName)
        merged_df.to_csv(merged_file, encoding="utf-8", index=False)

        for file in all_files:
            if file != merged_file:
                os.remove(file)

    """
    Asserts directory existence.
    """

    def _assertDestination(self, destination: str) -> str:
        pathParts = str.split(destination, "/")
        # File name portion.
        pathParts.pop()

        folder = Path(*pathParts)
        folder.mkdir(parents=True, exist_ok=True)

        return f"./{folder.as_posix()}"
