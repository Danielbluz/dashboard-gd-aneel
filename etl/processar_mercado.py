"""
Processa CSVs SAMP da ANEEL para gerar dados de mercado por distribuidora.
Extrai: energia compensada GD, injetada GD, consumo cativo, mercado livre.

Uso: python processar_mercado.py
"""
import json
import sys
from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).parent / "raw"
DATA_DIR = Path(__file__).parent.parent / "docs" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Anos SAMP a processar (GD relevante a partir de 2018)
ANOS = range(2018, 2027)

# Métricas de energia que nos interessam
METRICAS_ENERGIA = [
    "Energia compensada (kWh)",
    "Energia Injetada (kWh)",
    "Energia TUSD (kWh)",
    "Energia Consumida (kWh)",
]


def carregar_samp() -> pd.DataFrame:
    """Carrega e concatena CSVs SAMP de todos os anos."""
    frames = []
    for ano in ANOS:
        path = RAW_DIR / f"samp-{ano}.csv"
        if not path.exists():
            print(f"  samp-{ano}.csv nao encontrado, pulando")
            continue
        print(f"  Carregando samp-{ano}.csv...")
        df = pd.read_csv(
            path,
            sep=";",
            encoding="latin1",
            decimal=",",
            usecols=[
                "SigAgenteDistribuidora",
                "NomAgenteDistribuidora",
                "NomTipoMercado",
                "DscClasseConsumoMercado",
                "DscOpcaoEnergia",
                "DscDetalheMercado",
                "DatCompetencia",
                "VlrMercado",
            ],
            on_bad_lines="skip",
            low_memory=False,
        )
        frames.append(df)

    if not frames:
        print("Nenhum arquivo SAMP encontrado!")
        sys.exit(1)

    df = pd.concat(frames, ignore_index=True)
    print(f"  Total: {len(df):,} registros SAMP")

    # Parse data
    df["DatCompetencia"] = pd.to_datetime(df["DatCompetencia"], errors="coerce")
    df["ano"] = df["DatCompetencia"].dt.year
    df["mes"] = df["DatCompetencia"].dt.month
    df["ano_mes"] = df["DatCompetencia"].dt.to_period("M").astype(str)

    # Converter VlrMercado para numérico
    df["VlrMercado"] = pd.to_numeric(df["VlrMercado"], errors="coerce").fillna(0)

    # Filtrar apenas métricas de energia (kWh)
    df = df[df["DscDetalheMercado"].isin(METRICAS_ENERGIA)].copy()
    print(f"  Filtrado energia: {len(df):,} registros")

    return df


def classificar_mercado(row):
    """Classifica o registro em categorias de mercado."""
    tipo = str(row["NomTipoMercado"])
    opcao = str(row["DscOpcaoEnergia"])
    detalhe = str(row["DscDetalheMercado"])

    if "Compensa" in tipo:
        # É GD
        if detalhe == "Energia compensada (kWh)":
            return "GD Compensada"
        elif detalhe == "Energia Injetada (kWh)":
            return "GD Injetada"
        elif detalhe in ("Energia TUSD (kWh)", "Energia Consumida (kWh)"):
            return "GD Consumo Próprio"
        return "GD Outros"
    elif "LIVRE" in opcao:
        return "Mercado Livre"
    elif opcao == "CATIVO":
        return "Mercado Cativo"
    else:
        return "Outros"


def gerar_mercado_distribuidora(df: pd.DataFrame) -> dict:
    """Gera dados de mercado agregados por distribuidora e evolução temporal."""
    print("Gerando mercado_distribuidora.json...")

    # Classificar cada registro
    df["categoria"] = df.apply(classificar_mercado, axis=1)

    # Converter kWh para GWh
    df["gwh"] = df["VlrMercado"] / 1_000_000

    # === 1. Evolução mensal nacional por categoria ===
    evo_nacional = (
        df.groupby(["ano_mes", "categoria"])["gwh"]
        .sum()
        .unstack(fill_value=0)
        .sort_index()
        .round(1)
    )
    evolucao = []
    for periodo in evo_nacional.index:
        row = {"periodo": periodo}
        for col in evo_nacional.columns:
            row[col] = float(evo_nacional.loc[periodo, col])
        evolucao.append(row)

    categorias = list(evo_nacional.columns)

    # === 2. Evolução anual nacional ===
    evo_anual = (
        df.groupby(["ano", "categoria"])["gwh"]
        .sum()
        .unstack(fill_value=0)
        .sort_index()
        .round(1)
    )
    evolucao_anual = []
    for ano in evo_anual.index:
        row = {"ano": int(ano)}
        for col in evo_anual.columns:
            row[col] = float(evo_anual.loc[ano, col])
        evolucao_anual.append(row)

    # === 3. Por distribuidora (último ano completo) ===
    ano_max = int(df["ano"].max())
    meses_max = df[df["ano"] == ano_max]["mes"].nunique()
    ano_ref = ano_max if meses_max >= 10 else ano_max - 1
    df_ref = df[df["ano"] == ano_ref]

    por_dist = (
        df_ref.groupby(["SigAgenteDistribuidora", "NomAgenteDistribuidora", "categoria"])["gwh"]
        .sum()
        .unstack(fill_value=0)
        .sort_values(
            by=[c for c in ["Mercado Cativo", "GD Compensada"] if c in df_ref["categoria"].unique()],
            ascending=False,
        )
    )

    distribuidoras = []
    for (sigla, nome), row in por_dist.iterrows():
        d = {
            "sigla": str(sigla),
            "nome": str(nome),
        }
        for col in por_dist.columns:
            d[col] = round(float(row.get(col, 0)), 1)
        # Calcular total e % GD
        total = sum(d.get(c, 0) for c in categorias)
        gd_total = d.get("GD Compensada", 0) + d.get("GD Injetada", 0)
        d["total_gwh"] = round(total, 1)
        d["pct_gd"] = round(gd_total / total * 100, 1) if total > 0 else 0
        # Penetração: consumo_GD / (consumo_GD + consumo_regulado)
        consumo_gd = d.get("GD Consumo Próprio", 0)
        consumo_reg = d.get("Mercado Cativo", 0)
        d["penetracao"] = round(consumo_gd / (consumo_gd + consumo_reg) * 100, 1) if (consumo_gd + consumo_reg) > 0 else 0
        distribuidoras.append(d)

    # Ordenar por total
    distribuidoras.sort(key=lambda x: x["total_gwh"], reverse=True)

    # === 4. Top 15 distribuidoras evolução GD compensada ===
    top15_siglas = [d["sigla"] for d in distribuidoras[:15]]
    df_top = df[
        (df["SigAgenteDistribuidora"].isin(top15_siglas)) &
        (df["categoria"] == "GD Compensada")
    ]
    evo_dist_gd = (
        df_top.groupby(["ano_mes", "SigAgenteDistribuidora"])["gwh"]
        .sum()
        .unstack(fill_value=0)
        .sort_index()
        .round(1)
    )
    evolucao_dist_gd = []
    # Amostrar 1 a cada 3 meses
    indices = list(evo_dist_gd.index)
    sampled = [indices[i] for i in range(0, len(indices), 3)]
    if indices[-1] not in sampled:
        sampled.append(indices[-1])
    for periodo in sampled:
        row = {"periodo": periodo}
        for col in evo_dist_gd.columns:
            row[str(col)] = float(evo_dist_gd.loc[periodo, col])
        evolucao_dist_gd.append(row)

    # === 5. Breakdown por classe de consumo (GD) ===
    df_gd = df[df["categoria"].str.startswith("GD")]
    por_classe = (
        df_gd.groupby("DscClasseConsumoMercado")["gwh"]
        .sum()
        .sort_values(ascending=False)
        .round(1)
    )
    classes_gd = [
        {"classe": str(k), "gwh": float(v)}
        for k, v in por_classe.items()
    ]

    # === 6. Proporção GD / Total por ano ===
    proporcao_anual = []
    for row in evolucao_anual:
        ano = row["ano"]
        gd = row.get("GD Compensada", 0) + row.get("GD Injetada", 0)
        cativo = row.get("Mercado Cativo", 0)
        livre = row.get("Mercado Livre", 0)
        total = gd + cativo + livre + row.get("GD Consumo Próprio", 0) + row.get("Outros", 0)
        proporcao_anual.append({
            "ano": ano,
            "pct_gd": round(gd / total * 100, 1) if total > 0 else 0,
            "pct_cativo": round(cativo / total * 100, 1) if total > 0 else 0,
            "pct_livre": round(livre / total * 100, 1) if total > 0 else 0,
        })

    # === 7. Penetração da GD ===
    # consumo_GD / (consumo_GD + consumo_regulado) por ano
    penetracao_anual = []
    for row in evolucao_anual:
        ano = row["ano"]
        consumo_gd = row.get("GD Consumo Próprio", 0)
        consumo_reg = row.get("Mercado Cativo", 0)
        denom = consumo_gd + consumo_reg
        penetracao_anual.append({
            "ano": ano,
            "penetracao": round(consumo_gd / denom * 100, 1) if denom > 0 else 0,
            "consumo_gd_gwh": round(consumo_gd, 1),
            "consumo_reg_gwh": round(consumo_reg, 1),
        })

    # Penetração mensal nacional
    mensal_cat = (
        df.groupby(["ano_mes", "categoria"])["gwh"]
        .sum()
        .unstack(fill_value=0)
        .sort_index()
    )
    penetracao_mensal = []
    for periodo in mensal_cat.index:
        gd_c = float(mensal_cat.loc[periodo].get("GD Consumo Próprio", 0))
        reg_c = float(mensal_cat.loc[periodo].get("Mercado Cativo", 0))
        denom = gd_c + reg_c
        penetracao_mensal.append({
            "periodo": periodo,
            "penetracao": round(gd_c / denom * 100, 1) if denom > 0 else 0,
        })

    # Penetração anual por distribuidora (top 15)
    top15_pen = [d["sigla"] for d in sorted(distribuidoras, key=lambda x: x["penetracao"], reverse=True)[:15]]
    df_pen = df[df["SigAgenteDistribuidora"].isin(top15_pen)]
    pen_dist_anual = (
        df_pen.groupby(["ano", "SigAgenteDistribuidora", "categoria"])["gwh"]
        .sum()
        .reset_index()
    )
    pen_pivot = pen_dist_anual.pivot_table(
        index=["ano", "SigAgenteDistribuidora"],
        columns="categoria",
        values="gwh",
        fill_value=0,
    ).reset_index()
    pen_evo = []
    for ano in sorted(pen_pivot["ano"].unique()):
        row = {"ano": int(ano)}
        for sigla in top15_pen:
            sub = pen_pivot[(pen_pivot["ano"] == ano) & (pen_pivot["SigAgenteDistribuidora"] == sigla)]
            if len(sub):
                gd_c = float(sub["GD Consumo Próprio"].iloc[0]) if "GD Consumo Próprio" in sub.columns else 0
                reg_c = float(sub["Mercado Cativo"].iloc[0]) if "Mercado Cativo" in sub.columns else 0
                denom = gd_c + reg_c
                row[sigla] = round(gd_c / denom * 100, 1) if denom > 0 else 0
            else:
                row[sigla] = 0
        pen_evo.append(row)

    return {
        "ano_referencia": int(ano_ref),
        "categorias": categorias,
        "evolucao_mensal": evolucao,
        "evolucao_anual": evolucao_anual,
        "distribuidoras": distribuidoras,
        "top15_siglas": top15_siglas,
        "evolucao_dist_gd": evolucao_dist_gd,
        "classes_gd": classes_gd,
        "proporcao_anual": proporcao_anual,
        "penetracao_anual": penetracao_anual,
        "penetracao_mensal": penetracao_mensal,
        "penetracao_dist": {"siglas": top15_pen, "evolucao": pen_evo},
    }


def salvar_json(data: dict, filename: str) -> None:
    dest = DATA_DIR / filename
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = dest.stat().st_size / 1024
    print(f"  -> {filename} ({size_kb:.0f} KB)")


def main():
    print("Processando dados SAMP (mercado distribuidoras)...")
    df = carregar_samp()
    mercado = gerar_mercado_distribuidora(df)
    salvar_json(mercado, "mercado_distribuidora.json")
    print("\nProcessamento SAMP concluido!")


if __name__ == "__main__":
    main()
