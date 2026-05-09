from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from src.api.services import (
    fetch_cauc_for_municipality,
    fetch_siconfi_for_municipality,
    resolve_municipality,
)

app = FastAPI(
    title="SolveLicita CAUC + Siconfi API",
    version="1.0.0",
    description="API focada em CAUC e Siconfi por CNPJ ou IBGE.",
)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/v1/consulta")
async def consulta(
    cnpj: str | None = Query(default=None),
    ibge: str | None = Query(default=None),
    mode: str = Query(default="incremental"),
) -> dict:
    if mode not in {"full", "incremental"}:
        raise HTTPException(status_code=400, detail="mode deve ser 'full' ou 'incremental'.")

    try:
        ref = await resolve_municipality(cnpj=cnpj, ibge=ibge)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Falha ao consultar municipio: {exc}") from exc

    try:
        cauc_payload = await fetch_cauc_for_municipality(ref)
        siconfi_payload = await fetch_siconfi_for_municipality(ref, mode=mode)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Falha na coleta CAUC/Siconfi: {exc}") from exc

    return {
        "municipio": {
            "cod_ibge": ref.cod_ibge,
            "cnpj": ref.cnpj,
            "uf": ref.uf,
            "ente": ref.ente,
            "populacao": ref.populacao,
        },
        "cauc": cauc_payload,
        "siconfi": siconfi_payload,
    }

