#!/usr/bin/env python3
import json
from pathlib import Path
from typing import Any

import pandas as pd

output_file = Path(__file__).parent / "chirp.csv"

raw_data_dir = Path(__file__).parent / "raw_data"

individual_chirp_file_dir = Path(__file__).parent / "chirp_files"
individual_chirp_file_dir.mkdir(exist_ok=True)

all_csv_files = raw_data_dir.glob("*.csv")
all_csv_files = sorted(all_csv_files)
# all_csv_files = [raw_data_dir / "airports.csv"]
metadata_file = raw_data_dir / "metadata.jsonc"


def read_metadata_file(metadata_file_path: Path) -> dict:
    metadata_file_content = metadata_file_path.read_text()
    if "//" in metadata_file_content:
        lines = metadata_file_content.split("\n")
        metadata_file_content = "\n".join([line.split("//")[0] for line in lines])
    return json.loads(metadata_file_content)


metadata = read_metadata_file(metadata_file)

dataframes: dict[str, pd.DataFrame] = {}


def apply_metadata(df: pd.DataFrame, metadata: dict) -> pd.DataFrame:
    if "comment" in metadata:
        df["Comment"] = metadata["comment"]
    if "name_format" in metadata:
        df["Name"] = df.apply(lambda row: metadata["name_format"].format(**row), axis=1)

    if "radio_params" in metadata:
        radio_params: dict[str, Any] = metadata[filename]["radio_params"]
        for key, value in radio_params.items():
            df[key] = value
    return df


def write_chirp_csv(df: pd.DataFrame, output_file: Path):
    df.index = df.index + 1
    df.index.name = "Location"
    df[
        [
            "Name",
            "Frequency",
            "Duplex",
            "Offset",
            "Tone",
            "rToneFreq",
            "cToneFreq",
            "DtcsCode",
            "DtcsPolarity",
            "RxDtcsCode",
            "CrossMode",
            "Mode",
            "TStep",
            "Skip",
            "Power",
            "Comment",
            "URCALL",
            "RPT1CALL",
            "RPT2CALL",
            "DVCODE",
        ]
    ].to_csv(output_file)


for csv_file in all_csv_files:
    filename = csv_file.stem
    print(f"Processing {filename}")
    df = pd.read_csv(csv_file)
    df["Index"] = df.index + 1
    df["Comment"] = filename
    if "Description" in df.columns:
        df["Name"] = df["Description"]

    df["Duplex"] = ""
    df["Offset"] = 0.0
    df["Tone"] = ""
    df["rToneFreq"] = 88.5
    df["cToneFreq"] = 88.5
    df["DtcsCode"] = "023"
    df["DtcsPolarity"] = "NN"
    df["RxDtcsCode"] = "023"
    df["CrossMode"] = "Tone->Tone"
    df["Mode"] = "NFM"
    df["TStep"] = 12.5
    df["Skip"] = ""
    df["Power"] = "4.0W"
    df["URCALL"] = ""
    df["RPT1CALL"] = ""
    df["RPT2CALL"] = ""
    df["DVCODE"] = ""

    if filename in metadata:
        df = apply_metadata(df, metadata[filename])

    write_chirp_csv(df, individual_chirp_file_dir / f"{filename}.csv")

    dataframes[filename] = df


result = pd.concat(dataframes.values(), ignore_index=True)

# result.sort_values(by=["Comment"], inplace=True)
# result.reset_index(drop=True, inplace=True)
result.index = result.index + 1
result.index.name = "Location"

write_chirp_csv(result, output_file)

print(f"Output written to {output_file}")
