from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date

import httpx

from src.collectors import cauc as cauc_collector
from src.collectors import siconfi as siconfi_collector

ENTES_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/entes"


@dataclass(frozen=True)
class MunicipalityRef:
    cod_ibge: str
    cnpj: str
    uf: str
    ente: str
    populacao: int | None
    query_id: str
    output_cod_ibge: str
    esfera: str | None
    instituicao_filtro: str | None


def _normalize_digits(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _coerce_populacao(value) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


async def _fetch_entes() -> list[dict]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(ENTES_URL)
        response.raise_for_status()
    return response.json().get("items", [])


async def resolve_municipality(*, cnpj: str | None, ibge: str | None) -> MunicipalityRef:
    cnpj_digits = _normalize_digits(cnpj) if cnpj else None
    ibge_digits = _normalize_digits(ibge) if ibge else None

    if not cnpj_digits and not ibge_digits:
        raise ValueError("Informe cnpj ou ibge.")

    items = await _fetch_entes()
    for item in items:
        if item.get("esfera") != "M":
            continue
        cod_ibge = _normalize_digits(item.get("cod_ibge"))
        cnpj_item = _normalize_digits(item.get("cnpj"))
        if ibge_digits and cod_ibge != ibge_digits:
            continue
        if cnpj_digits and cnpj_item != cnpj_digits:
            continue

        uf = str(item.get("uf") or "").upper().strip()
        ente = str(item.get("ente") or "").strip()
        pop = _coerce_populacao(item.get("populacao"))

        if uf == "DF":
            query_id = siconfi_collector.DF_QUERY_ID_ENTE
            output_cod_ibge = siconfi_collector.DF_OUTPUT_COD_IBGE
            esfera = siconfi_collector.DF_SCOPE_ESFERA
            instituicao_filtro = siconfi_collector.DF_ENTITY_NAME
        else:
            query_id = cod_ibge.zfill(7)
            output_cod_ibge = cod_ibge.zfill(7)
            esfera = None
            instituicao_filtro = None

        return MunicipalityRef(
            cod_ibge=cod_ibge.zfill(7),
            cnpj=cnpj_item,
            uf=uf,
            ente=ente,
            populacao=pop,
            query_id=query_id,
            output_cod_ibge=output_cod_ibge,
            esfera=esfera,
            instituicao_filtro=instituicao_filtro,
        )

    raise ValueError("Municipio nao encontrado para os filtros informados.")


async def fetch_cauc_for_municipality(ref: MunicipalityRef) -> dict:
    bulk_csv = await asyncio.to_thread(cauc_collector.download_cauc_bulk_csv)
    df = bulk_csv.df_raw
    cod_ibge_series = df[bulk_csv.col_ibge].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(7)
    df_filtered = df[cod_ibge_series == ref.cod_ibge].copy()
    if df_filtered.empty:
        return {
            "data_pesquisa": bulk_csv.data_pesquisa,
            "found": False,
            "record": None,
        }

    record = df_filtered.iloc[0].to_dict()
    record["data_pesquisa"] = bulk_csv.data_pesquisa
    record["data_coleta"] = date.today().strftime("%Y-%m-%d")
    return {
        "data_pesquisa": bulk_csv.data_pesquisa,
        "found": True,
        "record": record,
    }


async def fetch_siconfi_for_municipality(
    ref: MunicipalityRef,
    *,
    mode: str = "incremental",
) -> dict:
    anos = siconfi_collector.ANOS_FULL if mode == "full" else siconfi_collector.ANOS_INCREMENTAL
    semaforo = asyncio.Semaphore(siconfi_collector.MAX_CONCORRENCIA)
    pausa_global = asyncio.Event()
    pausa_global.set()
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=15)

    rreo_all: list[dict] = []
    rgf_all: list[dict] = []
    extrato_counts: dict[int, int] = {}

    async with httpx.AsyncClient(timeout=45.0, limits=limits) as client:
        for ano in anos:
            extrato_items = await siconfi_collector.extrair_extrato_entregas(
                client,
                ano,
                ref.query_id,
                semaforo,
                pausa_global,
                progresso=None,
            )
            extrato_filtrado = siconfi_collector._filtrar_extrato_entidade(
                extrato_items,
                ref.instituicao_filtro,
            )
            extrato_counts[ano] = len(extrato_filtrado)

            rreo_ano, rgf_ano = await siconfi_collector._coletar_pacote_municipio_ano(
                client,
                ano=ano,
                query_id=ref.query_id,
                extrato_items=extrato_filtrado,
                populacao=ref.populacao,
                output_cod_ibge=ref.output_cod_ibge,
                esfera=ref.esfera,
                semaforo=semaforo,
                pausa_global=pausa_global,
            )
            rreo_all.extend(rreo_ano)
            rgf_all.extend(rgf_ano)

    return {
        "mode": mode,
        "anos": anos,
        "extrato_por_ano": extrato_counts,
        "rreo_count": len(rreo_all),
        "rgf_count": len(rgf_all),
        "rreo": rreo_all,
        "rgf": rgf_all,
    }

