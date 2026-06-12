"""Prepare the eCommerce dataset: sample CSV → JSON Lines."""
import json
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

_COLUMNS = [
    "event_time", "event_type", "product_id", "category_id",
    "category_code", "brand", "price", "user_id", "user_session",
]
_DTYPES = {
    "category_id": str,
    "user_session": str,
    "event_type":  str,
    "brand":       str,
    "category_code": str,
}


def prepare_dataset(
    input_path: str,
    output_path: str,
    sample_size: int = 1_000_000,
    chunk_size: int = 100_000,
) -> None:
    """Extract a stratified sample from a CSV and write it as JSON Lines.

    Args:
        input_path: Path to the source CSV file (2019-Oct.csv or 2019-Nov.csv).
        output_path: Path for the output JSONL file.
        sample_size: Maximum number of rows to extract.
        chunk_size: Number of rows to read per CSV chunk.

    Raises:
        FileNotFoundError: If input_path does not exist.
    """
    input_path  = Path(input_path)
    output_path = Path(output_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_written = 0
    logger.info("Sampling %d rows from %s (chunk=%d)", sample_size, input_path, chunk_size)

    with open(output_path, "w") as out_f:
        reader = pd.read_csv(
            input_path,
            chunksize=chunk_size,
            dtype=_DTYPES,
            usecols=_COLUMNS,
        )
        for chunk in reader:
            if total_written >= sample_size:
                break
            chunk = chunk.dropna(subset=["event_time", "event_type", "product_id", "user_id"])
            chunk = chunk.head(sample_size - total_written)
            for _, row in chunk.iterrows():
                record = {
                    "event_time":    str(row["event_time"]),
                    "event_type":    str(row["event_type"]),
                    "product_id":    int(row["product_id"]) if pd.notna(row["product_id"]) else 0,
                    "category_id":   str(row["category_id"]) if pd.notna(row["category_id"]) else "0",
                    "category_code": str(row["category_code"]) if pd.notna(row["category_code"]) else None,
                    "brand":         str(row["brand"]) if pd.notna(row["brand"]) else None,
                    "price":         float(row["price"]) if pd.notna(row["price"]) else 0.0,
                    "user_id":       int(row["user_id"]) if pd.notna(row["user_id"]) else 0,
                    "user_session":  str(row["user_session"]) if pd.notna(row["user_session"]) else "",
                }
                out_f.write(json.dumps(record) + "\n")
                total_written += 1

    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info("Written %d records → %s (%.1f MB)", total_written, output_path, size_mb)
    if size_mb > 500:
        logger.warning("Output file exceeds 500 MB — consider reducing sample_size")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Prepare eCommerce dataset for the ETL pipeline")
    parser.add_argument("--input",       required=True,  help="Path to source CSV (2019-Oct.csv)")
    parser.add_argument("--output",      default=os.getenv("DATA_PATH", "data/sample/sample_data.jsonl"))
    parser.add_argument("--sample-size", type=int, default=1_000_000)
    parser.add_argument("--chunk-size",  type=int, default=100_000)
    args = parser.parse_args()

    prepare_dataset(args.input, args.output, args.sample_size, args.chunk_size)
