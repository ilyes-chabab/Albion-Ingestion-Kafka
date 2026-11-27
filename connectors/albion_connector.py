
import json
import os
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
from huggingface_hub import HfApi, hf_hub_download
import pyarrow.parquet as pq


REPO_ID = "Sayli84/albion-online-market-prices"
FILENAME = "data/market_prices.parquet"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = PROJECT_ROOT / "data" / "state" / "last_ingested.json"


def get_hf_token():
    load_dotenv(PROJECT_ROOT / ".env")

    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise ValueError("Le token HF_TOKEN est absent du fichier .env")
    return hf_token


# Récupérer le sha (identifiant de version) du dataset sur Hugging Face.
# Les métadonnées sont publiques : pas besoin de token ici.
def get_latest_sha():
    load_dotenv(PROJECT_ROOT / ".env")
    info = HfApi().dataset_info(REPO_ID, token=os.getenv("HF_TOKEN"))
    return info.sha


# Lire le sha de la dernière version ingérée avec succès (None si jamais ingéré)
def read_last_sha():
    if not STATE_FILE.exists():
        return None
    return json.loads(STATE_FILE.read_text(encoding="utf-8")).get("sha")


# Enregistrer le sha une fois la version entièrement publiée dans Kafka
def save_last_sha(sha, run_id=None, rows=None):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "sha": sha,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "rows": rows,
    }
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)  # écriture atomique : pas de fichier d'état à moitié écrit


# Vérifier si une nouvelle version du dataset est disponible
def check_new_version():
    latest = get_latest_sha()
    last = read_last_sha()
    return latest, latest != last


# Télécharger le fichier Parquet depuis Hugging Face (à une version précise si revision est donnée)
def get_hf_file(revision=None):
    hf_token = get_hf_token()

    input_dir = PROJECT_ROOT / "data" / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    file_path = hf_hub_download(
        repo_id=REPO_ID,
        filename=FILENAME,
        repo_type="dataset",
        revision=revision,
        local_dir=str(input_dir),
        token=hf_token
    )

    print(f"Fichier récupéré : {file_path}")
    return file_path


# Extraire les données Parquet et les renvoyer sous forme structurée
def extract_data(revision=None):
    file_path = get_hf_file(revision)
    parquet_file = pq.ParquetFile(file_path)

    extraction_date = datetime.now(timezone.utc).isoformat()

    for batch in parquet_file.iter_batches(batch_size=1000):
        for row in batch.to_pylist():
            yield {
                "source": "HF_albion_online_market_prices",
                "dataset_sha": revision,
                "extract_date": extraction_date,
                "data": row
            }


if __name__ == "__main__":
    sha, is_new = check_new_version()
    print(f"Version du dataset : {sha} ({'nouvelle' if is_new else 'déjà ingérée'})")

    extracted_data = extract_data(sha)

    # Afficher un message pour vérifier l'extraction
    print(next(extracted_data))
