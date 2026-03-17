"""
Processa CSV da ANEEL (4M+ registros) e gera JSONs agregados para o dashboard.

Uso: python processar_dados.py
"""
import json
import sys
from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).parent / "raw"
DATA_DIR = Path(__file__).parent.parent / "docs" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CSV_PRINCIPAL = RAW_DIR / "empreendimento-geracao-distribuida.csv"
CSV_FOTO = RAW_DIR / "empreendimento-gd-informacoes-tecnicas-fotovoltaica.csv"

# Colunas que vamos usar do CSV principal (economiza memória)
COLS_PRINCIPAL = [
    "DthAtualizaCadastralEmpreend",  # Data da conexão
    "SigUF",                         # UF (ex: MG, SP)
    "NomMunicipio",                  # Município
    "DscModalidadeHabilitado",       # Texto: "Geracao na propria UC", etc.
    "DscClasseConsumo",              # Classe de consumo
    "DscSubGrupoTarifario",          # Subgrupo tarifário
    "DscPorte",                      # Micro/Mini
    "SigTipoGeracao",                # Fonte (UFV, UTE, EOL, CGH, UHE)
    "DscFonteGeracao",               # Fonte descritiva
    "MdaPotenciaInstaladaKW",        # Potência kW
    "QtdUCRecebeCredito",            # UCs que recebem crédito
    "NomTitularEmpreendimento",      # Nome do titular/empresa
    "SigTipoConsumidor",             # PF ou PJ
    "NomAgente",                     # Nome da distribuidora
    "SigAgente",                     # Sigla distribuidora
]

# Mapeamento de fontes para nomes amigáveis
FONTES_MAP = {
    "UFV": "Solar",
    "UTE": "Térmica",
    "EOL": "Eólica",
    "CGH": "Hidráulica",
    "UHE": "Hidráulica",
    "PCH": "Hidráulica",
}

# Modalidade - padronizar nomes (CSV vem em latin1 com acentos parciais)
MODALIDADES_MAP = {
    "Geracao na propria UC": "Geração na própria UC",
    "Auto consumo remoto": "Autoconsumo remoto",
    "Compartilhada": "Geração compartilhada",
}

# Faixas de potência para gráficos de barras
FAIXAS_POTENCIA = [
    ("1-10 kW", 1, 10),
    ("10-25 kW", 10, 25),
    ("25-75 kW", 25, 75),
    ("75 kW-1 MW", 75, 1000),
    ("1-2,5 MW", 1000, 2500),
    ("> 2,5 MW", 2500, float("inf")),
]

CLASSES_PRINCIPAIS = ["Residencial", "Comercial", "Industrial", "Rural"]


def classificar_faixa(kw: float) -> str:
    for label, low, high in FAIXAS_POTENCIA:
        if low <= kw < high:
            return label
    return "> 2,5 MW" if kw >= 2500 else "1-10 kW"


def agrupar_classe(classe: str) -> str:
    return classe if classe in CLASSES_PRINCIPAIS else "Outras"


def gerar_faixas_por_classe(df: pd.DataFrame) -> dict:
    """Gera dados de faixa de potência segmentados por classe agrupada."""
    tmp = df.copy()
    tmp["faixa"] = tmp["MdaPotenciaInstaladaKW"].apply(classificar_faixa)
    tmp["classe_agrup"] = tmp["DscClasseConsumo"].astype(str).apply(agrupar_classe)

    labels = [f[0] for f in FAIXAS_POTENCIA]
    classes = CLASSES_PRINCIPAIS + ["Outras"]

    pot = tmp.groupby(["faixa", "classe_agrup"])["potencia_mw"].sum().unstack(fill_value=0)
    qtd = tmp.groupby(["faixa", "classe_agrup"]).size().unstack(fill_value=0)

    potencia = {}
    quantidade = {}
    for c in classes:
        potencia[c] = [
            round(float(pot.loc[f, c]), 1) if f in pot.index and c in pot.columns else 0
            for f in labels
        ]
        quantidade[c] = [
            int(qtd.loc[f, c]) if f in qtd.index and c in qtd.columns else 0
            for f in labels
        ]

    return {"labels": labels, "potencia": potencia, "quantidade": quantidade}


def gerar_breakdowns(df: pd.DataFrame) -> dict:
    """Gera breakdowns compactos (KPIs + faixas + classes/modalidades/portes/fontes)."""
    result = {
        "total_mw": round(float(df["potencia_mw"].sum()), 1),
        "total_conexoes": len(df),
        "total_ucs_credito": int(df["QtdUCRecebeCredito"].sum()),
        "faixas": gerar_faixas_por_classe(df),
    }

    for campo, col_name in [
        ("classes", "DscClasseConsumo"),
        ("modalidades", "modalidade"),
        ("portes", "DscPorte"),
        ("fontes", "fonte"),
    ]:
        grp = (
            df.groupby(col_name)
            .agg(conexoes=("potencia_mw", "count"), potencia_mw=("potencia_mw", "sum"))
            .sort_values("potencia_mw", ascending=False)
        )
        grp["potencia_mw"] = grp["potencia_mw"].round(1)
        key_name = campo.rstrip("s") if campo != "classes" else "classe"
        result[campo] = [
            {key_name: str(k), "conexoes": int(v["conexoes"]), "potencia_mw": float(v["potencia_mw"])}
            for k, v in grp.iterrows()
        ]

    return result


def carregar_csv() -> pd.DataFrame:
    """Carrega CSV principal com tipos otimizados."""
    print(f"Carregando {CSV_PRINCIPAL.name}...")

    dtype = {
        "SigUF": "category",
        "DscModalidadeHabilitado": "category",
        "DscClasseConsumo": "category",
        "DscPorte": "category",
        "SigTipoGeracao": "category",
        "DscFonteGeracao": "category",
        "SigAgente": "category",
        "NomAgente": "category",
        "DscSubGrupoTarifario": "category",
        "SigTipoConsumidor": "category",
        "MdaPotenciaInstaladaKW": "float64",
        "QtdUCRecebeCredito": "float64",
    }

    df = pd.read_csv(
        CSV_PRINCIPAL,
        usecols=COLS_PRINCIPAL,
        dtype=dtype,
        sep=";",
        encoding="latin1",
        on_bad_lines="skip",
        low_memory=False,
        quotechar='"',
        decimal=",",
    )

    print(f"  {len(df):,.0f} registros carregados")

    # Parse da data de conexão → mês/ano
    df["data_conexao"] = pd.to_datetime(
        df["DthAtualizaCadastralEmpreend"], format="%Y-%m-%d", errors="coerce"
    )
    df["ano"] = df["data_conexao"].dt.year
    df["mes"] = df["data_conexao"].dt.month
    df["ano_mes"] = df["data_conexao"].dt.to_period("M").astype(str)

    # Potência em MW
    df["potencia_mw"] = df["MdaPotenciaInstaladaKW"] / 1000

    # Fonte amigável
    df["fonte"] = (
        df["SigTipoGeracao"]
        .astype(str)
        .map(FONTES_MAP)
        .fillna("Outras")
    )

    # Modalidade amigável
    mod_raw = df["DscModalidadeHabilitado"].astype(str)
    df["modalidade"] = mod_raw.map(MODALIDADES_MAP)
    # Valores não mapeados: manter o original, tratar "Condom" como Condomínio
    mask_unmapped = df["modalidade"].isna()
    df.loc[mask_unmapped & mod_raw.str.startswith("Condom"), "modalidade"] = "Condomínio"
    df.loc[df["modalidade"].isna(), "modalidade"] = mod_raw[df["modalidade"].isna()]

    # Filtrar datas inválidas (antes de 2012, quando GD começou no Brasil)
    df = df[df["ano"] >= 2012].copy()

    return df


def gerar_resumo_geral(df: pd.DataFrame) -> dict:
    """KPIs totais + evolução mensal + breakdowns."""
    print("Gerando resumo_geral.json...")

    total_mw = round(df["potencia_mw"].sum(), 1)
    total_conexoes = len(df)
    total_ucs = int(df["QtdUCRecebeCredito"].sum())

    # Evolução mensal (acumulada)
    mensal = (
        df.groupby("ano_mes")
        .agg(
            novas_conexoes=("potencia_mw", "count"),
            potencia_mw=("potencia_mw", "sum"),
            ucs=("QtdUCRecebeCredito", "sum"),
        )
        .sort_index()
    )
    mensal["potencia_acumulada_mw"] = mensal["potencia_mw"].cumsum().round(1)
    mensal["conexoes_acumuladas"] = mensal["novas_conexoes"].cumsum()
    mensal["potencia_mw"] = mensal["potencia_mw"].round(1)
    mensal["ucs"] = mensal["ucs"].astype(int)

    evolucao = []
    for periodo, row in mensal.iterrows():
        evolucao.append({
            "periodo": periodo,
            "novas_conexoes": int(row["novas_conexoes"]),
            "potencia_mw": float(row["potencia_mw"]),
            "potencia_acumulada_mw": float(row["potencia_acumulada_mw"]),
            "conexoes_acumuladas": int(row["conexoes_acumuladas"]),
        })

    # Breakdown por fonte
    por_fonte = (
        df.groupby("fonte")
        .agg(conexoes=("potencia_mw", "count"), potencia_mw=("potencia_mw", "sum"))
        .sort_values("potencia_mw", ascending=False)
    )
    por_fonte["potencia_mw"] = por_fonte["potencia_mw"].round(1)
    fontes = [
        {"fonte": k, "conexoes": int(v["conexoes"]), "potencia_mw": float(v["potencia_mw"])}
        for k, v in por_fonte.iterrows()
    ]

    # Evolução mensal por fonte (acumulada)
    fonte_mensal = (
        df.groupby(["ano_mes", "fonte"])["potencia_mw"]
        .sum()
        .unstack(fill_value=0)
        .sort_index()
        .cumsum()
        .round(1)
    )
    evolucao_fonte = []
    for periodo in fonte_mensal.index:
        row = {"periodo": periodo}
        for col in fonte_mensal.columns:
            row[col] = float(fonte_mensal.loc[periodo, col])
        evolucao_fonte.append(row)

    # Breakdown por modalidade
    por_modalidade = (
        df.groupby("modalidade")
        .agg(conexoes=("potencia_mw", "count"), potencia_mw=("potencia_mw", "sum"))
        .sort_values("potencia_mw", ascending=False)
    )
    por_modalidade["potencia_mw"] = por_modalidade["potencia_mw"].round(1)
    modalidades = [
        {"modalidade": k, "conexoes": int(v["conexoes"]), "potencia_mw": float(v["potencia_mw"])}
        for k, v in por_modalidade.iterrows()
    ]

    # Breakdown por porte
    por_porte = (
        df.groupby("DscPorte")
        .agg(conexoes=("potencia_mw", "count"), potencia_mw=("potencia_mw", "sum"))
        .sort_values("potencia_mw", ascending=False)
    )
    por_porte["potencia_mw"] = por_porte["potencia_mw"].round(1)
    portes = [
        {"porte": str(k), "conexoes": int(v["conexoes"]), "potencia_mw": float(v["potencia_mw"])}
        for k, v in por_porte.iterrows()
    ]

    # Breakdown por classe de consumo
    por_classe = (
        df.groupby("DscClasseConsumo")
        .agg(conexoes=("potencia_mw", "count"), potencia_mw=("potencia_mw", "sum"))
        .sort_values("potencia_mw", ascending=False)
    )
    por_classe["potencia_mw"] = por_classe["potencia_mw"].round(1)
    classes = [
        {"classe": str(k), "conexoes": int(v["conexoes"]), "potencia_mw": float(v["potencia_mw"])}
        for k, v in por_classe.iterrows()
    ]

    # YoY: último ano completo vs anterior (2026 é parcial, usar 2025 vs 2024)
    ano_max = int(df["ano"].max())
    # Se o ano mais recente tem menos de 10 meses de dados, é parcial
    meses_ultimo = df[df["ano"] == ano_max]["mes"].nunique()
    ano_ref = ano_max if meses_ultimo >= 10 else ano_max - 1
    ano_ant = ano_ref - 1
    mw_ref = df[df["ano"] == ano_ref]["potencia_mw"].sum()
    mw_ant = df[df["ano"] == ano_ant]["potencia_mw"].sum()
    yoy = round((mw_ref / mw_ant - 1) * 100, 1) if mw_ant > 0 else 0

    # Faixas de potência por classe (nível nacional)
    print("  Gerando faixas de potência por classe...")
    faixas = gerar_faixas_por_classe(df)

    # Per-UF breakdowns para filtros
    print("  Gerando breakdowns por UF...")
    ufs_list = sorted([u for u in df["SigUF"].dropna().astype(str).unique().tolist() if u != "nan"])
    por_uf = {}
    for uf in ufs_list:
        por_uf[uf] = gerar_breakdowns(df[df["SigUF"] == uf])

    # Per-distribuidora breakdowns (top 50)
    print("  Gerando breakdowns por distribuidora...")
    dist_mw = (
        df.groupby("NomAgente")["potencia_mw"].sum().sort_values(ascending=False)
    )
    top_dists = dist_mw.head(50).index.tolist()
    por_distribuidora = {}
    dist_uf_map = {}
    for dist_name in top_dists:
        df_dist = df[df["NomAgente"] == dist_name]
        por_distribuidora[dist_name] = gerar_breakdowns(df_dist)
        # UF principal da distribuidora (moda)
        uf_mode = df_dist["SigUF"].astype(str).mode()
        dist_uf_map[dist_name] = uf_mode.iloc[0] if len(uf_mode) > 0 else ""

    dists_list = [
        {"nome": d, "potencia_mw": round(float(dist_mw[d]), 1)}
        for d in top_dists
    ]

    return {
        "kpis": {
            "total_mw": total_mw,
            "total_gw": round(total_mw / 1000, 2),
            "total_conexoes": total_conexoes,
            "total_ucs_credito": total_ucs,
            "crescimento_yoy_pct": yoy,
            "ano_ref_yoy": int(ano_ref),
        },
        "evolucao_mensal": evolucao,
        "evolucao_fonte": evolucao_fonte,
        "fontes": fontes,
        "modalidades": modalidades,
        "portes": portes,
        "classes": classes,
        "faixas": faixas,
        "listas_uf": ufs_list,
        "listas_distribuidora": dists_list,
        "dist_uf_map": dist_uf_map,
        "por_uf": por_uf,
        "por_distribuidora": por_distribuidora,
    }


def gerar_por_estado(df: pd.DataFrame) -> dict:
    """Agregado por UF."""
    print("Gerando por_estado.json...")

    por_uf = (
        df.groupby("SigUF")
        .agg(
            conexoes=("potencia_mw", "count"),
            potencia_mw=("potencia_mw", "sum"),
            ucs_credito=("QtdUCRecebeCredito", "sum"),
        )
        .sort_values("potencia_mw", ascending=False)
    )
    total_mw = por_uf["potencia_mw"].sum()
    por_uf["potencia_mw"] = por_uf["potencia_mw"].round(1)
    por_uf["pct_total"] = (por_uf["potencia_mw"] / total_mw * 100).round(1)
    por_uf["ucs_credito"] = por_uf["ucs_credito"].astype(int)

    estados = []
    for uf, row in por_uf.iterrows():
        estados.append({
            "uf": str(uf),
            "conexoes": int(row["conexoes"]),
            "potencia_mw": float(row["potencia_mw"]),
            "ucs_credito": int(row["ucs_credito"]),
            "pct_total": float(row["pct_total"]),
        })

    # Série temporal top 10 estados (acumulado mensal)
    top10_ufs = [e["uf"] for e in estados[:10]]
    df_top = df[df["SigUF"].isin(top10_ufs)]
    serie_uf = (
        df_top.groupby(["ano_mes", "SigUF"])["potencia_mw"]
        .sum()
        .unstack(fill_value=0)
        .sort_index()
        .cumsum()
        .round(1)
    )
    evolucao_uf = []
    for periodo in serie_uf.index:
        row = {"periodo": periodo}
        for col in serie_uf.columns:
            row[str(col)] = float(serie_uf.loc[periodo, col])
        evolucao_uf.append(row)

    return {
        "estados": estados,
        "evolucao_top10": evolucao_uf,
        "top10_ufs": top10_ufs,
    }


def gerar_por_municipio(df: pd.DataFrame) -> dict:
    """Top 500 municípios."""
    print("Gerando por_municipio.json...")

    por_mun = (
        df.groupby(["NomMunicipio", "SigUF"])
        .agg(
            conexoes=("potencia_mw", "count"),
            potencia_mw=("potencia_mw", "sum"),
            ucs_credito=("QtdUCRecebeCredito", "sum"),
        )
        .sort_values("potencia_mw", ascending=False)
        .head(500)
    )
    por_mun["potencia_mw"] = por_mun["potencia_mw"].round(1)
    por_mun["ucs_credito"] = por_mun["ucs_credito"].astype(int)

    municipios = []
    for (mun, uf), row in por_mun.iterrows():
        municipios.append({
            "municipio": str(mun),
            "uf": str(uf),
            "conexoes": int(row["conexoes"]),
            "potencia_mw": float(row["potencia_mw"]),
            "ucs_credito": int(row["ucs_credito"]),
        })

    return {"municipios": municipios}


def gerar_por_distribuidora(df: pd.DataFrame) -> dict:
    """Agregado por distribuidora."""
    print("Gerando por_distribuidora.json...")

    por_dist = (
        df.groupby("NomAgente")
        .agg(
            conexoes=("potencia_mw", "count"),
            potencia_mw=("potencia_mw", "sum"),
            ucs_credito=("QtdUCRecebeCredito", "sum"),
            municipios=("NomMunicipio", "nunique"),
        )
        .sort_values("potencia_mw", ascending=False)
    )
    por_dist["potencia_mw"] = por_dist["potencia_mw"].round(1)
    por_dist["ucs_credito"] = por_dist["ucs_credito"].astype(int)

    distribuidoras = []
    for dist, row in por_dist.iterrows():
        distribuidoras.append({
            "distribuidora": str(dist),
            "conexoes": int(row["conexoes"]),
            "potencia_mw": float(row["potencia_mw"]),
            "ucs_credito": int(row["ucs_credito"]),
            "municipios": int(row["municipios"]),
        })

    # Série temporal top 10
    top10 = [d["distribuidora"] for d in distribuidoras[:10]]
    df_top = df[df["NomAgente"].isin(top10)]
    serie = (
        df_top.groupby(["ano_mes", "NomAgente"])["potencia_mw"]
        .sum()
        .unstack(fill_value=0)
        .sort_index()
        .cumsum()
        .round(1)
    )
    evolucao = []
    for periodo in serie.index:
        row = {"periodo": periodo}
        for col in serie.columns:
            row[str(col)] = float(serie.loc[periodo, col])
        evolucao.append(row)

    return {
        "distribuidoras": distribuidoras,
        "evolucao_top10": evolucao,
        "top10": top10,
    }


def gerar_empresas_top(df: pd.DataFrame) -> dict:
    """Top 100 empresas (PJ) por potência."""
    print("Gerando empresas_top.json...")

    # Filtrar apenas PJ
    df_pj = df[
        (df["NomTitularEmpreendimento"].notna()) &
        (df["SigTipoConsumidor"] == "PJ")
    ].copy()

    por_empresa = (
        df_pj.groupby("NomTitularEmpreendimento")
        .agg(
            potencia_mw=("potencia_mw", "sum"),
            empreendimentos=("potencia_mw", "count"),
            ufs=("SigUF", "nunique"),
            lista_ufs=("SigUF", lambda x: ",".join(sorted(x.unique().astype(str)))),
            lista_dists=("NomAgente", lambda x: ",".join(sorted(x.unique().astype(str)))),
        )
        .sort_values("potencia_mw", ascending=False)
        .head(100)
    )
    por_empresa["potencia_mw"] = por_empresa["potencia_mw"].round(1)

    empresas = []
    for nome, row in por_empresa.iterrows():
        empresas.append({
            "nome": str(nome),
            "potencia_mw": float(row["potencia_mw"]),
            "empreendimentos": int(row["empreendimentos"]),
            "qtd_ufs": int(row["ufs"]),
            "ufs": row["lista_ufs"],
            "distribuidoras": row["lista_dists"],
        })

    # Concentração de mercado (Pareto)
    total_mw = df["potencia_mw"].sum()
    acumulado = 0
    pareto = []
    for i, emp in enumerate(empresas):
        acumulado += emp["potencia_mw"]
        pareto.append({
            "rank": i + 1,
            "pct_acumulado": round(acumulado / total_mw * 100, 2),
        })

    return {"empresas": empresas, "pareto": pareto}


def gerar_fabricantes(df: pd.DataFrame) -> dict:
    """Top fabricantes de módulos e inversores (do CSV fotovoltaica)."""
    print("Gerando fabricantes.json...")

    result = {"modulos": [], "inversores": [], "potencia_media": []}

    if not CSV_FOTO.exists():
        print("  CSV fotovoltaica não encontrado, pulando fabricantes")
        return result

    # O CSV fotovoltaica pode ser grande, carregar só colunas necessárias
    try:
        cols_foto = [
            "NomFabricanteModulo",
            "NomFabricanteInversor",
            "MdaPotenciaInstalada",
            "DatConexao",
        ]
        df_foto = pd.read_csv(
            CSV_FOTO,
            usecols=cols_foto,
            sep=";",
            encoding="latin1",
            on_bad_lines="skip",
            low_memory=False,
            quotechar='"',
            decimal=",",
        )
        df_foto["MdaPotenciaInstalada"] = pd.to_numeric(
            df_foto["MdaPotenciaInstalada"], errors="coerce"
        )
        print(f"  {len(df_foto):,.0f} registros fotovoltaica carregados")
    except Exception as e:
        print(f"  Erro ao carregar fotovoltaica: {e}")
        return result

    # Top fabricantes módulos
    top_mod = (
        df_foto.groupby("NomFabricanteModulo")
        .agg(
            potencia_mw=("MdaPotenciaInstalada", lambda x: x.sum() / 1000),
            quantidade=("MdaPotenciaInstalada", "count"),
        )
        .sort_values("potencia_mw", ascending=False)
        .head(30)
    )
    top_mod["potencia_mw"] = top_mod["potencia_mw"].round(1)
    result["modulos"] = [
        {"fabricante": str(k), "potencia_mw": float(v["potencia_mw"]), "quantidade": int(v["quantidade"])}
        for k, v in top_mod.iterrows()
    ]

    # Top fabricantes inversores
    top_inv = (
        df_foto.groupby("NomFabricanteInversor")
        .agg(
            potencia_mw=("MdaPotenciaInstalada", lambda x: x.sum() / 1000),
            quantidade=("MdaPotenciaInstalada", "count"),
        )
        .sort_values("potencia_mw", ascending=False)
        .head(30)
    )
    top_inv["potencia_mw"] = top_inv["potencia_mw"].round(1)
    result["inversores"] = [
        {"fabricante": str(k), "potencia_mw": float(v["potencia_mw"]), "quantidade": int(v["quantidade"])}
        for k, v in top_inv.iterrows()
    ]

    # Potência média por sistema ao longo do tempo
    df_foto["DatConexao"] = pd.to_datetime(
        df_foto["DatConexao"], format="%Y-%m-%d", errors="coerce"
    )
    df_foto["ano_mes"] = df_foto["DatConexao"].dt.to_period("M").astype(str)
    pot_media = (
        df_foto.groupby("ano_mes")["MdaPotenciaInstalada"]
        .mean()
        .sort_index()
        .round(2)
    )
    result["potencia_media"] = [
        {"periodo": str(k), "kw_medio": float(v)}
        for k, v in pot_media.items()
    ]

    return result


def salvar_json(data: dict, filename: str) -> None:
    dest = DATA_DIR / filename
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = dest.stat().st_size / 1024
    print(f"  -> {filename} ({size_kb:.0f} KB)")


def main():
    if not CSV_PRINCIPAL.exists():
        print(f"CSV não encontrado: {CSV_PRINCIPAL}")
        print("Execute primeiro: python download_aneel.py")
        sys.exit(1)

    df = carregar_csv()

    # Gerar todos os JSONs
    resumo = gerar_resumo_geral(df)
    salvar_json(resumo, "resumo_geral.json")

    estados = gerar_por_estado(df)
    salvar_json(estados, "por_estado.json")

    municipios = gerar_por_municipio(df)
    salvar_json(municipios, "por_municipio.json")

    distribuidoras = gerar_por_distribuidora(df)
    salvar_json(distribuidoras, "por_distribuidora.json")

    empresas = gerar_empresas_top(df)
    salvar_json(empresas, "empresas_top.json")

    fabricantes = gerar_fabricantes(df)
    salvar_json(fabricantes, "fabricantes.json")

    print("\nProcessamento concluído! JSONs salvos em docs/data/")


if __name__ == "__main__":
    main()
