# Dashboard GD Brasil

Dashboard interativo sobre **Micro e Minigeração Distribuída (MMGD)** no Brasil, usando dados públicos da ANEEL.

**[Acessar Dashboard](https://danielbluz.github.io/dashboard-gd-aneel/)**

## Dados

- **Fonte**: [ANEEL Dados Abertos](https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida)
- **Licença**: Open Database License (ODbL)
- **Registros**: 4M+ empreendimentos de geração distribuída

## Como atualizar os dados

```bash
# 1. Instalar dependências
pip install -r etl/requirements.txt

# 2. Baixar CSVs da ANEEL (~1.2 GB)
python etl/download_aneel.py

# 3. Processar e gerar JSONs agregados
python etl/processar_dados.py
```

Os JSONs agregados (~2-5 MB total) ficam em `docs/data/` e são commitados no repo.
Os CSVs brutos (~1.2 GB) ficam em `etl/raw/` e são gitignored.

## Stack

- **ETL**: Python (pandas)
- **Gráficos**: Chart.js v4
- **Mapa**: ECharts
- **Hosting**: GitHub Pages (`/docs`)

## Créditos

Dados: ANEEL Dados Abertos | EPE
