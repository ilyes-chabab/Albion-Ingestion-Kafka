
import os
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
from huggingface_hub import hf_hub_download
import pyarrow.parquet as pq


# Télécharger le fichier Parquet depuis Hugging Face
def get_hf_file():
    project_root = Path(__file__).resolve().parent.parent

    load_dotenv(project_root / ".env")

    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise ValueError("Le token HF_TOKEN est absent du fichier .env")

    input_dir = project_root / "data" / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    file_path = hf_hub_download(
        repo_id="Sayli84/albion-online-market-prices",
        filename="data/market_prices.parquet",
        repo_type="dataset",
        local_dir=str(input_dir),
        token=hf_token
    )

    print(f"Fichier récupéré : {file_path}")
    return file_path


# Extraire les données Parquet et les renvoyer sous forme structurée
def extract_data():
    file_path = get_hf_file()
    parquet_file = pq.ParquetFile(file_path)

    extraction_date = datetime.now(timezone.utc).isoformat()

    for batch in parquet_file.iter_batches(batch_size=1000):
        for row in batch.to_pylist():
            yield {
                "source": "HF_albion_online_market_prices",
                "extract_date": extraction_date,
                "data": row
            }


if __name__ == "__main__":
    extracted_data = extract_data()

    # Afficher un message pour vérifier l'extraction
    print(next(extracted_data))
