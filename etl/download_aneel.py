"""
Download dos CSVs de Geração Distribuída da ANEEL (Dados Abertos).
Salva em etl/raw/ (gitignored).

Uso: python download_aneel.py
"""
import os
import sys
import requests
from pathlib import Path

RAW_DIR = Path(__file__).parent / "raw"
RAW_DIR.mkdir(exist_ok=True)

DATASETS = {
    "empreendimento-geracao-distribuida.csv": (
        "https://dadosabertos.aneel.gov.br/dataset/"
        "5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/"
        "b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/"
        "empreendimento-geracao-distribuida.csv"
    ),
    "empreendimento-gd-informacoes-tecnicas-fotovoltaica.csv": (
        "https://dadosabertos.aneel.gov.br/dataset/"
        "5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/"
        "49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a/download/"
        "empreendimento-gd-informacoes-tecnicas-fotovoltaica.csv"
    ),
}


def download_file(url: str, dest: Path) -> None:
    print(f"Baixando {dest.name}...")
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0

    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                mb = downloaded / 1024 / 1024
                print(f"\r  {mb:.0f} MB ({pct:.1f}%)", end="", flush=True)
            else:
                mb = downloaded / 1024 / 1024
                print(f"\r  {mb:.0f} MB", end="", flush=True)

    print(f"\n  Salvo: {dest} ({dest.stat().st_size / 1024 / 1024:.1f} MB)")


def main():
    for filename, url in DATASETS.items():
        dest = RAW_DIR / filename
        if dest.exists():
            size_mb = dest.stat().st_size / 1024 / 1024
            print(f"{filename} já existe ({size_mb:.1f} MB). Pulando.")
            print(f"  (Delete o arquivo para re-baixar)")
            continue
        try:
            download_file(url, dest)
        except Exception as e:
            print(f"ERRO ao baixar {filename}: {e}", file=sys.stderr)
            if dest.exists():
                dest.unlink()
            sys.exit(1)

    print("\nDownload concluído!")


if __name__ == "__main__":
    main()
