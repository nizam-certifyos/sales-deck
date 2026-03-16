"""FastAPI web server exposing folder-first enterprise workspace workflow."""

from __future__ import annotations

import datetime
import json
import logging
import mimetypes
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from universal_roster_v2.config import get_settings
from universal_roster_v2.web.session_store import SessionStore


# ---------------------------------------------------------------------------
# PSV Lookup
# ---------------------------------------------------------------------------

_PSV_CACHE: Dict[str, dict] = {}
_PSV_PROJECT = os.getenv("PSV_BQ_PROJECT", "certifyos-production-platform")
_PSV_SECRET_NAME = os.getenv("PSV_SECRET_NAME", "bqsaprd")
_PSV_SECRET_PROJECT = os.getenv("PSV_SECRET_PROJECT", "certifyos-development")

_psv_logger = logging.getLogger("universal_roster_v2.web.psv")


_bq_psv_client_cache = None
_psv_sa_info_cache = None


def _load_psv_sa_from_secret_manager() -> dict:
    """Load the production SA JSON from Secret Manager."""
    global _psv_sa_info_cache
    if _psv_sa_info_cache is not None:
        return _psv_sa_info_cache

    # Try Secret Manager first
    try:
        from google.cloud import secretmanager
        sm_client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{_PSV_SECRET_PROJECT}/secrets/{_PSV_SECRET_NAME}/versions/latest"
        response = sm_client.access_secret_version(request={"name": secret_path})
        _psv_sa_info_cache = json.loads(response.payload.data.decode("UTF-8"))
        _psv_logger.info("Loaded PSV SA from Secret Manager: %s", _PSV_SECRET_NAME)
        return _psv_sa_info_cache
    except Exception as e:
        _psv_logger.warning("Secret Manager failed: %s. Trying file fallback.", e)

    # Fallback: try file path
    sa_path = os.getenv("PSV_SERVICE_ACCOUNT_KEY_PATH", "")
    if sa_path and os.path.isfile(sa_path):
        _psv_sa_info_cache = json.load(open(sa_path))
        _psv_logger.info("Loaded PSV SA from file: %s", sa_path)
        return _psv_sa_info_cache

    raise RuntimeError("Cannot load PSV SA: Secret Manager and file fallback both failed")


def _get_bq_psv_client():
    """Create (or return cached) BQ client using the production SA from Secret Manager."""
    global _bq_psv_client_cache
    if _bq_psv_client_cache is not None:
        return _bq_psv_client_cache
    from google.oauth2 import service_account as _sa
    from google.cloud import bigquery as _bq

    sa_info = _load_psv_sa_from_secret_manager()
    creds = _sa.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    _bq_psv_client_cache = _bq.Client(credentials=creds, project=_PSV_PROJECT)
    return _bq_psv_client_cache


def _safe_str(val) -> str:
    if val is None:
        return ""
    return str(val)


def _query_nppes(client, npi: str) -> dict:
    rows = list(client.query(f"""
        SELECT NPI, Provider_First_Name, Provider_Last_Name, Entity_Type_Code,
               NPI_Deactivation_Date,
               Provider_First_Line_Business_Practice_Location_Address AS address_1,
               Provider_Business_Practice_Location_Address_City_Name AS city,
               Provider_Business_Practice_Location_Address_State_Name AS state,
               Provider_Business_Practice_Location_Address_Postal_Code AS postal_code
        FROM `{_PSV_PROJECT}.nppes_data.nppes_raw`
        WHERE NPI = '{npi}' LIMIT 1
    """).result(timeout=30))
    if not rows:
        return {"found": False}
    r = rows[0]
    return {
        "found": True,
        "npi": _safe_str(r.NPI),
        "first_name": _safe_str(r.Provider_First_Name),
        "last_name": _safe_str(r.Provider_Last_Name),
        "entity_type": _safe_str(r.Entity_Type_Code),
        "deactivation_date": _safe_str(r.NPI_Deactivation_Date),
        "address": _safe_str(r.address_1),
        "city": _safe_str(r.city),
        "state": _safe_str(r.state),
        "postal_code": _safe_str(r.postal_code),
    }


def _query_state_licenses(client, npi: str) -> list:
    rows = list(client.query(f"""
        SELECT LicenseEntityStateOrProvinceCode as state, LicenseNumber,
               LicenseIssueDate, LicenseExpireDate, LicenseActiveFlag,
               LicenseStatus, StateLicenseType, FetchDataSource
        FROM `{_PSV_PROJECT}.provider_source_data.state_licenses`
        WHERE CAST(NPI AS STRING) = '{npi}'
    """).result(timeout=30))
    results = []
    for r in rows:
        results.append({
            "state": _safe_str(r.state),
            "number": _safe_str(r.LicenseNumber),
            "issue_date": _safe_str(r.LicenseIssueDate),
            "expiry": _safe_str(r.LicenseExpireDate),
            "active_flag": _safe_str(r.LicenseActiveFlag),
            "status": _safe_str(r.LicenseStatus),
            "type": _safe_str(r.StateLicenseType),
            "source": _safe_str(r.FetchDataSource),
        })
    return results


def _query_abms(client, npi: str) -> list:
    rows = list(client.query(f"""
        SELECT member_board_name, certificate_name, cert_start_year, cert_start_month,
               cert_end_year, cert_end_month, cert_end_day, type as cert_status,
               meeting_mocr_requirements
        FROM `{_PSV_PROJECT}.provider_source_data.abms_simplified`
        WHERE CAST(npi AS STRING) = '{npi}'
    """).result(timeout=30))
    results = []
    for r in rows:
        # Build expiry date string
        expiry = ""
        if r.cert_end_year:
            month = _safe_str(r.cert_end_month) if r.cert_end_month else "12"
            day = _safe_str(r.cert_end_day) if r.cert_end_day else "31"
            expiry = f"{r.cert_end_year}-{month.zfill(2)}-{day.zfill(2)}"
        results.append({
            "board": _safe_str(r.member_board_name),
            "cert_name": _safe_str(r.certificate_name),
            "status": _safe_str(r.cert_status),
            "expiry": expiry,
            "meeting_moc": _safe_str(r.meeting_mocr_requirements),
        })
    return results


def _query_dea(client, npi: str) -> list:
    rows = list(client.query(f"""
        SELECT dea_num, activity, expiry as dea_expiry, state, schedules,
               degree, state_license_number
        FROM `{_PSV_PROJECT}.provider_source_data.dea_simplified`
        WHERE CAST(npi AS STRING) = '{npi}' LIMIT 3
    """).result(timeout=30))
    results = []
    for r in rows:
        results.append({
            "number": _safe_str(r.dea_num),
            "status": _safe_str(r.activity),
            "expiry": _safe_str(r.dea_expiry),
            "state": _safe_str(r.state),
            "schedules": _safe_str(r.schedules),
            "degree": _safe_str(r.degree),
        })
    return results


def _query_oig(client, npi: str) -> dict:
    rows = list(client.query(f"""
        SELECT firstName, lastName, exclusionType, exclusionDate,
               reincarnationDate, specialty
        FROM `{_PSV_PROJECT}.sanctions_data.oig`
        WHERE CAST(npi AS STRING) = '{npi}'
    """).result(timeout=30))
    records = []
    for r in rows:
        records.append({
            "first_name": _safe_str(r.firstName),
            "last_name": _safe_str(r.lastName),
            "exclusion_type": _safe_str(r.exclusionType),
            "exclusion_date": _safe_str(r.exclusionDate),
            "reinstatement_date": _safe_str(r.reincarnationDate),
            "specialty": _safe_str(r.specialty),
        })
    return {"found": len(records) > 0, "records": records}


def _query_sam(client, npi: str) -> dict:
    rows = list(client.query(f"""
        SELECT firstName, lastName, exclusionType, activeDate,
               terminationDate, excludingAgency, exclusionProgram
        FROM `{_PSV_PROJECT}.sanctions_data.sam`
        WHERE CAST(npi AS INT64) = CAST('{npi}' AS INT64)
    """).result(timeout=30))
    records = []
    for r in rows:
        records.append({
            "first_name": _safe_str(r.firstName),
            "last_name": _safe_str(r.lastName),
            "exclusion_type": _safe_str(r.exclusionType),
            "active_date": _safe_str(r.activeDate),
            "termination_date": _safe_str(r.terminationDate),
            "agency": _safe_str(r.excludingAgency),
            "program": _safe_str(r.exclusionProgram),
        })
    return {"found": len(records) > 0, "records": records}


def _query_state_sanctions(client, npi: str) -> dict:
    rows = list(client.query(f"""
        SELECT FIRST_NAME, LAST_NAME, LICENSE_JURISDICTION, LICENSE_NUMBER,
               ACTION_OR_REASON, END_DATE
        FROM `{_PSV_PROJECT}.provider_source_data.state_sanctions`
        WHERE CAST(NPI AS STRING) = '{npi}'
    """).result(timeout=30))
    records = []
    for r in rows:
        records.append({
            "first_name": _safe_str(r.FIRST_NAME),
            "last_name": _safe_str(r.LAST_NAME),
            "jurisdiction": _safe_str(r.LICENSE_JURISDICTION),
            "license_number": _safe_str(r.LICENSE_NUMBER),
            "action": _safe_str(r.ACTION_OR_REASON),
            "end_date": _safe_str(r.END_DATE),
        })
    return {"found": len(records) > 0, "records": records}


def _query_medicare_optout(client, npi: str) -> dict:
    rows = list(client.query(f"""
        SELECT First_Name, Last_Name, Specialty, Optout_Effective_Date, Optout_End_Date
        FROM `{_PSV_PROJECT}.provider_source_data.medicare_opt_out`
        WHERE CAST(npi AS STRING) = '{npi}'
    """).result(timeout=30))
    if not rows:
        return {"found": False}
    r = rows[0]
    return {
        "found": True,
        "first_name": _safe_str(r.First_Name),
        "last_name": _safe_str(r.Last_Name),
        "specialty": _safe_str(r.Specialty),
        "effective_date": _safe_str(r.Optout_Effective_Date),
        "end_date": _safe_str(r.Optout_End_Date),
    }


def _query_deceased(client, npi: str) -> dict:
    rows = list(client.query(f"""
        SELECT * FROM `{_PSV_PROJECT}.provider_source_data.deceased`
        WHERE CAST(npi AS STRING) = '{npi}' LIMIT 1
    """).result(timeout=30))
    return {"found": len(rows) > 0}


def _query_board_actions(client, npi: str) -> dict:
    rows = list(client.query(f"""
        SELECT * FROM `{_PSV_PROJECT}.BoardActions.board_actions`
        WHERE CAST(npi AS STRING) = '{npi}'
    """).result(timeout=30))
    records = []
    for r in rows:
        rec = {}
        for key in r.keys():
            rec[key] = _safe_str(r[key])
        records.append(rec)
    return {"found": len(records) > 0, "records": records}


def _query_sanctions_summary(client, npi: str) -> dict:
    rows = list(client.query(f"""
        SELECT oig_count, sam_count, state_sanction_count, ofac_count, medicare_opt_out_count
        FROM `{_PSV_PROJECT}.provider_source_data.provider_sanctions_summary`
        WHERE CAST(npi AS STRING) = '{npi}'
    """).result(timeout=30))
    if not rows:
        return {"oig": 0, "sam": 0, "state": 0, "ofac": 0, "moo": 0}
    r = rows[0]
    return {
        "oig": int(r.oig_count or 0),
        "sam": int(r.sam_count or 0),
        "state": int(r.state_sanction_count or 0),
        "ofac": int(r.ofac_count or 0),
        "moo": int(r.medicare_opt_out_count or 0),
    }


def lookup_psv_data(npi: str) -> dict:
    """Query all PSV tables in parallel for a given NPI. Results are cached."""
    if npi in _PSV_CACHE:
        _psv_logger.info("PSV cache hit for NPI %s", npi)
        return _PSV_CACHE[npi]

    _psv_logger.info("Querying PSV data for NPI %s", npi)
    client = _get_bq_psv_client()

    result: Dict[str, Any] = {}
    queries = {
        "nppes": lambda: _query_nppes(client, npi),
        "state_licenses": lambda: _query_state_licenses(client, npi),
        "abms": lambda: _query_abms(client, npi),
        "dea": lambda: _query_dea(client, npi),
        "oig": lambda: _query_oig(client, npi),
        "sam": lambda: _query_sam(client, npi),
        "state_sanctions": lambda: _query_state_sanctions(client, npi),
        "medicare_optout": lambda: _query_medicare_optout(client, npi),
        "deceased": lambda: _query_deceased(client, npi),
        "board_actions": lambda: _query_board_actions(client, npi),
        "sanctions_summary": lambda: _query_sanctions_summary(client, npi),
    }

    with ThreadPoolExecutor(max_workers=11) as executor:
        futures = {executor.submit(fn): key for key, fn in queries.items()}
        for future in as_completed(futures):
            key = futures[future]
            try:
                result[key] = future.result(timeout=60)
            except Exception as exc:
                _psv_logger.warning("PSV query '%s' failed for NPI %s: %s", key, npi, exc)
                # Provide sensible defaults
                if key in ("oig", "sam", "state_sanctions", "board_actions"):
                    result[key] = {"found": False, "records": []}
                elif key in ("deceased", "medicare_optout", "nppes"):
                    result[key] = {"found": False}
                elif key == "sanctions_summary":
                    result[key] = {"oig": 0, "sam": 0, "state": 0, "ofac": 0, "moo": 0}
                else:
                    result[key] = []

    _PSV_CACHE[npi] = result
    _psv_logger.info("PSV data cached for NPI %s", npi)
    return result


NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}
IMMUTABLE_CACHE_HEADERS = {
    "Cache-Control": "public, max-age=31536000, immutable",
}


class CreateWorkspaceRequest(BaseModel):
    workspace_path: Optional[str] = None
    tenant_id: str = "default-tenant"
    client_id: str = "default-client"
    thread_id: Optional[str] = None


class CreateWorkspaceResponse(BaseModel):
    workspace_id: str


class LoadFileRequest(BaseModel):
    file_path: str
    roster_type: Optional[str] = None
    profile_full_roster_learning: Optional[bool] = None
    profile_max_rows: Optional[int] = None


class ChatRequest(BaseModel):
    message: str
    output_dir: Optional[str] = None


class ToggleRequest(BaseModel):
    item_type: str
    item_id: str
    approved: bool


class GenerateRequest(BaseModel):
    mode: str
    output_dir: Optional[str] = None
    pipeline_name: str = "UniversalRosterPipeline"


class TrainingExportRequest(BaseModel):
    output_dir: Optional[str] = None


class TrainingRunRequest(BaseModel):
    output_dir: Optional[str] = None
    extra_args: Optional[List[str]] = None


class RunGeneratedRequest(BaseModel):
    input_file: str
    generated_dir: Optional[str] = None
    output_dir: Optional[str] = None
    table_id: str = "project.dataset.staging"


class NoteContextRequest(BaseModel):
    free_text: Optional[str] = None
    client_rules: Optional[List[str]] = None
    schema_caveats: Optional[List[str]] = None
    exceptions: Optional[List[str]] = None


class StartOperationRequest(BaseModel):
    kind: str
    input: Dict[str, Any] = Field(default_factory=dict)
    parent_operation_id: Optional[str] = None


class RetryOperationRequest(BaseModel):
    request_id: Optional[str] = None


def create_app(workspace_root: Optional[str | Path] = None) -> FastAPI:
    app = FastAPI(title="CertifyOS AI - Roster Processing")
    store = SessionStore(workspace_root=workspace_root)
    settings = get_settings()
    static_dir = Path(__file__).resolve().parent / "static"
    spa_dist_dir = static_dir / "dist"
    spa_assets_dir = spa_dist_dir / "assets"
    ui_build_id = "ur2-ui-20260315-cloudrun"

    # ── Startup warmup ──
    _logger = logging.getLogger("universal_roster_v2.web.startup")

    @app.on_event("startup")
    async def _warmup():
        """Pre-load knowledge base, BQ client, and Gemini context cache at startup."""
        try:
            from universal_roster_v2.llm import knowledge_loader as _kl
            _kl.load_field_ontology()
            _kl.load_header_synonyms()
            _kl.load_schema_field_rules()
            _kl.load_platform_accepted_values()
            _logger.info("Knowledge base warmed up successfully")
        except Exception as exc:
            _logger.warning("Knowledge base warmup failed (non-fatal): %s", exc)
        try:
            _get_bq_psv_client()
            _logger.info("BQ PSV client initialized successfully")
        except Exception as exc:
            _logger.warning("BQ PSV client warmup failed (non-fatal): %s", exc)
        # Pre-create Gemini context cache for the mapping system prompt (40K tokens)
        # This runs at startup so the first user request doesn't pay the cache creation cost
        try:
            from universal_roster_v2.llm.gemini_provider import GeminiVertexProvider
            gp = GeminiVertexProvider(settings=settings)
            gp.warmup_cache()
            _logger.info("Gemini context cache warmed up successfully")
        except Exception as exc:
            _logger.warning("Gemini cache warmup failed (non-fatal): %s", exc)

    @app.middleware("http")
    async def request_correlation_middleware(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["x-request-id"] = request_id
        return response

    def spa_index_response() -> FileResponse:
        index_path = spa_dist_dir / "index.html"
        if not index_path.exists():
            raise HTTPException(status_code=500, detail="New UI bundle missing: static/dist/index.html")
        response = FileResponse(index_path, headers=NO_CACHE_HEADERS)
        response.headers["x-ui-build-id"] = ui_build_id
        return response

    @app.get("/")
    def index() -> FileResponse:
        return spa_index_response()

    # All other routes removed — "/" serves the full pipeline demo

    @app.get("/app.js")
    def app_js() -> FileResponse:
        raise HTTPException(status_code=404, detail="Legacy route removed")

    @app.get("/styles.css")
    def styles_css() -> FileResponse:
        raise HTTPException(status_code=404, detail="Legacy route removed")

    @app.get("/static/assets/{asset_path:path}")
    def static_asset(asset_path: str) -> FileResponse:
        resolved = (spa_assets_dir / asset_path).resolve()
        if not resolved.exists() or not resolved.is_file() or spa_assets_dir.resolve() not in resolved.parents:
            raise HTTPException(status_code=404, detail="Asset not found")
        media_type = mimetypes.guess_type(str(resolved))[0]
        response = FileResponse(resolved, media_type=media_type, headers=IMMUTABLE_CACHE_HEADERS)
        response.headers["x-ui-build-id"] = ui_build_id
        return response

    @app.get("/assets/{asset_path:path}")
    def static_asset_alias(asset_path: str) -> FileResponse:
        return static_asset(asset_path)

    @app.get("/api/nppes/{npi}")
    def nppes_lookup(npi: str) -> Dict[str, Any]:
        """NPPES lookup: PSV data first, CMS API fallback."""
        # Try PSV lookup first
        try:
            psv = lookup_psv_data(npi)
            nppes = psv.get("nppes", {})
            if nppes.get("found"):
                return {
                    "number": nppes.get("npi", ""),
                    "basic": {
                        "first_name": nppes.get("first_name", ""),
                        "last_name": nppes.get("last_name", ""),
                        "deactivation_date": nppes.get("deactivation_date", ""),
                    },
                    "enumeration_type": "NPI-1" if nppes.get("entity_type") == "1" else "NPI-2",
                    "addresses": [{
                        "address_1": nppes.get("address", ""),
                        "city": nppes.get("city", ""),
                        "state": nppes.get("state", ""),
                        "postal_code": nppes.get("postal_code", ""),
                    }],
                    "source": "psv",
                }
        except Exception:
            pass

        # Fallback to CMS NPPES API
        import urllib.request
        try:
            url = f"https://npiregistry.cms.hhs.gov/api/?number={npi}&version=2.1&limit=1"
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                results = data.get("results", [])
                if results:
                    result = results[0]
                    result["source"] = "api"
                    return result
        except Exception:
            pass

        return {}

    @app.get("/api/oig/{npi}")
    def oig_check(npi: str) -> Dict[str, Any]:
        """Proxy for OIG exclusion check to avoid CORS issues."""
        import urllib.request
        import urllib.error
        try:
            url = f"https://exclusions.oig.hhs.gov/api/exclusions/search?npi={npi}"
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data
        except urllib.error.URLError:
            return {"results": []}
        except Exception:
            return {"results": []}

    @app.get("/api/psv/{npi}")
    def psv_lookup(npi: str) -> Dict[str, Any]:
        """Return raw PSV data for a given NPI from all source tables."""
        try:
            data = lookup_psv_data(npi)
            return {"npi": npi, "data": data, "timestamp": datetime.datetime.utcnow().isoformat()}
        except Exception as exc:
            _psv_logger.error("PSV lookup failed for NPI %s: %s", npi, exc, exc_info=True)
            raise HTTPException(status_code=500, detail=f"PSV lookup failed: {str(exc)[:200]}")

    @app.post("/api/psv/batch")
    def psv_batch(request_body: dict) -> Dict[str, Any]:
        """Batch PSV lookup for multiple NPIs."""
        npis = request_body.get("npis", [])
        results: Dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(lookup_psv_data, npi): npi for npi in npis[:10]}
            for future in as_completed(futures):
                npi = futures[future]
                try:
                    results[npi] = future.result(timeout=60)
                except Exception:
                    results[npi] = {}
        return {"results": results}

    @app.get("/api/providers/{workspace_id}")
    def get_providers(workspace_id: str):
        """Extract unique providers from the workspace's roster data for credentialing/monitoring tabs."""
        import pandas as pd

        try:
            web_session = store.get(workspace_id)
        except Exception:
            raise HTTPException(status_code=404, detail="Workspace not found")

        session = web_session.session
        profile = session.state.profile or {}
        plan = session.state.plan or {}

        # Get mappings
        from universal_roster_v2.core.plan import PlanManager
        pm = PlanManager()
        raw_mappings = pm.combined_items(plan, "mappings") if plan else []
        mappings = [m for m in raw_mappings if m.get("target_field")]

        # Build target -> source lookup
        tgt_to_src: Dict[str, str] = {}
        for m in mappings:
            src = str(m.get("source_column", "")).strip()
            tgt = str(m.get("target_field", "")).strip()
            if src and tgt:
                tgt_to_src[tgt] = src

        # Get sample rows
        sample_rows = profile.get("sample_rows", [])
        _psv_logger.warning(f"PROVIDERS API: profile_keys={list(profile.keys())}, sample_rows={len(sample_rows)}, mappings={len(mappings)}, tgt_to_src_keys={list(tgt_to_src.keys())[:5]}")
        if not sample_rows:
            return {"providers": [], "debug": f"no sample_rows in profile. profile_keys={list(profile.keys())}"}

        # Helper to get value from a row using target field name
        def get_val(row: dict, *target_fields: str) -> str:
            for tf in target_fields:
                src = tgt_to_src.get(tf, "")
                if src and src in row:
                    val = str(row.get(src, "")).strip()
                    if val:
                        return val
            return ""

        # Extract unique providers (by NPI, limit to 10 most interesting)
        providers = []
        seen_npi = set()
        for row in sample_rows:
            npi = get_val(row, "practitionerNpi")
            if not npi or npi in seen_npi:
                continue
            seen_npi.add(npi)

            provider = {
                "npi": npi,
                "firstName": get_val(row, "firstName"),
                "lastName": get_val(row, "lastName"),
                "middleName": get_val(row, "middleName"),
                "degree": get_val(row, "providerType"),
                "specialty": get_val(row, "practitionerPrimarySpecialtyName", "practitionerSpecialtyName"),
                "gender": get_val(row, "gender"),
                "dob": get_val(row, "dateOfBirth"),
                "taxonomy": get_val(row, "practitionerPrimaryTaxonomy"),
                "licenseNumber": get_val(row, "stateLicenseNumber"),
                "licenseState": get_val(row, "stateLicenseIssuingState"),
                "licenseExpiry": get_val(row, "stateLicenseExpirationDate"),
                "deaNumber": get_val(row, "practitionerDea"),
                "deaExpiry": get_val(row, "practitionerDeaExpirationDate"),
                "boardName": get_val(row, "practitionerPrimarySpecialtyBoardCertificationName"),
                "boardExpiry": get_val(row, "practitionerPrimarySpecialtyBoardCertificationExpirationDate"),
                "practiceAddress": get_val(row, "groupPracticeLocationAddressLine1"),
                "practiceCity": get_val(row, "groupPracticeLocationCity"),
                "practiceState": get_val(row, "groupPracticeLocationState"),
                "practiceZip": get_val(row, "groupPracticeLocationZip"),
                "practicePhone": get_val(row, "groupPracticeLocationPhone"),
                "groupName": get_val(row, "groupName", "groupLegalBusinessName", "groupDba"),
                "groupTin": get_val(row, "groupTin"),
                "groupNpi": get_val(row, "groupNpi"),
                "credDate": get_val(row, "initialCredentialingDate"),
                "recredDate": get_val(row, "recredentialingDate", "nextCredentialingDate"),
                "malpracticeCarrier": get_val(row, "practitionerMalpracticeCarrierName"),
                "malpracticePolicy": get_val(row, "practitionerMalpracticePolicyNumber"),
                "malpracticeCoverage": get_val(row, "practitionerMalpracticeAggregateCoverageAmount"),
                "email": get_val(row, "credentialingContactEmail", "primaryEmail"),
                "caqhId": get_val(row, "practitionerCaqhId"),
                "medSchool": get_val(row, "educationSchoolName"),
                "gradDate": get_val(row, "educationToDate"),
            }
            providers.append(provider)

        # Sort: providers with issues first (expired creds, missing data)
        import datetime
        today = datetime.date.today().isoformat()

        def issue_score(p):
            score = 0
            for date_field in ["licenseExpiry", "deaExpiry", "boardExpiry"]:
                val = p.get(date_field, "")
                if val and val < today:
                    score += 10  # Expired = high priority
                elif val and val < (datetime.date.today() + datetime.timedelta(days=90)).isoformat():
                    score += 5  # Expiring soon
            if not p.get("npi"):
                score += 20
            return -score  # Negative so higher issues come first

        providers.sort(key=issue_score)

        # Limit to 10 most interesting providers
        return {"providers": providers[:10]}

    @app.post("/api/credential-check")
    def credential_check(request_body: dict) -> Dict[str, Any]:
        """LLM-powered credentialing assessment and monitoring flags for a provider,
        enriched with primary source verification data."""
        cred_logger = logging.getLogger("universal_roster_v2.web.credential_check")

        provider = request_body.get("provider", {})
        nppes_data = request_body.get("nppesData", {})
        oig_clear = request_body.get("oigClear", True)

        # --- Fetch real PSV data ---
        npi = provider.get("npi", "")
        psv_data: Dict[str, Any] = {}
        if npi:
            try:
                psv_data = lookup_psv_data(npi)
                cred_logger.info("PSV data fetched for NPI %s: %d sources", npi, len(psv_data))
            except Exception as psv_exc:
                cred_logger.warning("PSV lookup failed for NPI %s: %s", npi, psv_exc)

        # Format PSV data for the LLM prompt
        psv_sections = []
        if psv_data.get("nppes", {}).get("found"):
            n = psv_data["nppes"]
            psv_sections.append(
                f"NPPES (Source: NPPES Registry): Name={n.get('first_name','')} {n.get('last_name','')}, "
                f"NPI={n.get('npi','')}, Entity Type={n.get('entity_type','')}, "
                f"Deactivation={n.get('deactivation_date','None')}, "
                f"Address={n.get('address','')}, {n.get('city','')}, {n.get('state','')} {n.get('postal_code','')}"
            )
        else:
            psv_sections.append("NPPES: No record found")

        licenses = psv_data.get("state_licenses", [])
        if licenses:
            lic_lines = []
            for lic in licenses:
                lic_lines.append(
                    f"  - {lic.get('state','')} #{lic.get('number','')}: Status={lic.get('status','')}, "
                    f"Active={lic.get('active_flag','')}, Expiry={lic.get('expiry','')}, "
                    f"Type={lic.get('type','')}, Source={lic.get('source','')}"
                )
            psv_sections.append("State Licenses (Source: State Medical Boards):\n" + "\n".join(lic_lines))
        else:
            psv_sections.append("State Licenses: No records found")

        abms = psv_data.get("abms", [])
        if abms:
            abms_lines = []
            for cert in abms:
                abms_lines.append(
                    f"  - Board={cert.get('board','')}, Cert={cert.get('cert_name','')}, "
                    f"Status={cert.get('status','')}, Expiry={cert.get('expiry','')}, "
                    f"MOC={cert.get('meeting_moc','')}"
                )
            psv_sections.append("ABMS Board Certifications (Source: ABMS):\n" + "\n".join(abms_lines))
        else:
            psv_sections.append("ABMS Board Certifications: No records found")

        dea = psv_data.get("dea", [])
        if dea:
            dea_lines = []
            for d in dea:
                dea_lines.append(
                    f"  - DEA#={d.get('number','')}, Activity={d.get('status','')}, "
                    f"Expiry={d.get('expiry','')}, State={d.get('state','')}, "
                    f"Schedules={d.get('schedules','')}"
                )
            psv_sections.append("DEA (Source: DEA):\n" + "\n".join(dea_lines))
        else:
            psv_sections.append("DEA: No records found")

        oig = psv_data.get("oig", {})
        psv_sections.append(
            f"OIG Exclusion (Source: OIG): "
            f"{'FOUND - ' + json.dumps(oig.get('records',[])) if oig.get('found') else 'CLEAR - No exclusions'}"
        )

        sam = psv_data.get("sam", {})
        psv_sections.append(
            f"SAM Exclusion (Source: SAM.gov): "
            f"{'FOUND - ' + json.dumps(sam.get('records',[])) if sam.get('found') else 'CLEAR - No exclusions'}"
        )

        state_sanc = psv_data.get("state_sanctions", {})
        psv_sections.append(
            f"State Sanctions (Source: State Boards): "
            f"{'FOUND - ' + json.dumps(state_sanc.get('records',[])) if state_sanc.get('found') else 'CLEAR - No sanctions'}"
        )

        moo = psv_data.get("medicare_optout", {})
        psv_sections.append(
            f"Medicare Opt-Out (Source: CMS): "
            f"{'YES - Provider has opted out' if moo.get('found') else 'CLEAR - Not opted out'}"
        )

        deceased = psv_data.get("deceased", {})
        psv_sections.append(
            f"Deceased Check (Source: NPPES/DMF): "
            f"{'FLAGGED - Provider appears deceased' if deceased.get('found') else 'CLEAR - Not deceased'}"
        )

        ba = psv_data.get("board_actions", {})
        psv_sections.append(
            f"Board Actions (Source: State Boards): "
            f"{'FOUND - ' + str(len(ba.get('records',[]))) + ' action(s)' if ba.get('found') else 'CLEAR - No board actions'}"
        )

        summary = psv_data.get("sanctions_summary", {})
        psv_sections.append(
            f"Sanctions Summary: OIG={summary.get('oig',0)}, SAM={summary.get('sam',0)}, "
            f"State={summary.get('state',0)}, OFAC={summary.get('ofac',0)}, "
            f"Medicare Opt-Out={summary.get('moo',0)}"
        )

        psv_text = "\n\n".join(psv_sections)

        today_str = datetime.date.today().isoformat()

        # Build the prompt
        system_prompt = (
            "You are a healthcare credentialing analyst performing Primary Source Verification (PSV) "
            "for a provider. You have access to real primary source data.\n\n"
            "Data sources available:\n"
            "1. NPPES Registry - Real NPI registry data\n"
            "2. State Licenses - Real state medical board license data\n"
            "3. ABMS Board Certifications - Real ABMS board certification data\n"
            "4. DEA - Real DEA registration data\n"
            "5. OIG Exclusion - Real OIG exclusion list check\n"
            "6. SAM Exclusion - Real SAM.gov exclusion check\n"
            "7. State Sanctions - Real state board sanction data\n"
            "8. Medicare Opt-Out - Real CMS opt-out check\n"
            "9. Deceased Check - Real death master file check\n"
            "10. Board Actions - Real board disciplinary actions\n"
            "11. Malpractice Insurance - From roster data\n"
            "12. CAQH ProView - From roster data\n\n"
            "IMPORTANT: The REAL PSV DATA takes precedence over roster data when there are discrepancies.\n"
            "For each source, determine status: 'verified', 'active', 'expired', 'warning', 'missing', 'clear', 'excluded', 'on_file', 'mismatch'.\n"
            "Generate monitoring flags for:\n"
            "- Expired credentials (severity: 'critical')\n"
            "- Credentials expiring within 90 days (severity: 'warning')\n"
            "- Name mismatches between roster and NPPES (severity: 'warning')\n"
            "- OIG/SAM exclusions (severity: 'critical')\n"
            "- State sanctions or board actions (severity: 'critical')\n"
            "- Medicare opt-out (severity: 'warning')\n"
            "- Deceased flag (severity: 'critical')\n"
            "- Missing required credentials (severity: 'info')\n\n"
            "Use the ACTUAL data provided - be specific with dates, numbers, and names.\n"
            f"TODAY'S DATE IS: {today_str}. "
            "Compare ALL expiration dates against this date. If an expiration date is BEFORE today, "
            "the credential is EXPIRED (status='expired', severity='critical'). "
            "If it expires within 90 days of today, it is EXPIRING SOON (severity='warning').\n\n"
            "For each PSV check, include the source (e.g., 'Source: ABMS', 'Source: Texas Medical Board') in the detail.\n\n"
            "Return JSON with this exact structure:\n"
            "{\n"
            '  "credentialing": {\n'
            '    "status": "PSV Complete" | "Review Required" | "In Progress",\n'
            '    "summary": "string",\n'
            '    "psvChecks": [\n'
            '      {"source": "string", "status": "verified|active|expired|warning|missing|clear|excluded|on_file|mismatch", "detail": "string"}\n'
            "    ]\n"
            "  },\n"
            '  "monitoring": {\n'
            '    "status": "No Issues" | "Flags Found" | "Review Required",\n'
            '    "flags": [\n'
            '      {"category": "string", "severity": "critical|warning|info", "title": "string", "detail": "string"}\n'
            "    ],\n"
            '    "summary": "string"\n'
            "  }\n"
            "}"
        )

        user_prompt = (
            f"Perform a credentialing assessment for this provider.\n\n"
            f"PRIMARY SOURCE VERIFICATION DATA:\n\n"
            f"{psv_text}\n\n"
            f"ROSTER DATA (from client's uploaded file):\n{json.dumps(provider, indent=2)}\n\n"
            f"Analyze BOTH sources. The real PSV data takes precedence over roster data when there are discrepancies.\n"
            f"Today's date: {today_str}\n\n"
            f"Return the structured JSON credentialing report and monitoring flags."
        )

        try:
            from universal_roster_v2.llm.gemini_provider import GeminiVertexProvider

            gemini = GeminiVertexProvider(settings=settings, model=settings.gemini_flash_model)
            if not gemini.is_available():
                raise RuntimeError("Gemini provider not available")

            from google.genai import types as genai_types

            client = gemini._get_client()
            config = genai_types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.1,
                max_output_tokens=8192,
                response_mime_type="application/json",
            )

            response = client.models.generate_content(
                model=settings.gemini_flash_model,
                contents=user_prompt,
                config=config,
            )

            text = response.text.strip() if response.text else ""
            if not text:
                raise RuntimeError("Gemini returned empty response")

            result = json.loads(text)

            # Validate structure
            if "credentialing" not in result:
                result["credentialing"] = {"status": "In Progress", "summary": "Analysis incomplete", "psvChecks": []}
            if "monitoring" not in result:
                result["monitoring"] = {"status": "No Issues", "flags": [], "summary": "No monitoring data"}

            return result

        except Exception as exc:
            cred_logger.warning("Credential check LLM call failed: %s", exc, exc_info=True)
            # Return a fallback response so the frontend still works
            return {
                "credentialing": {
                    "status": "In Progress",
                    "summary": f"LLM analysis unavailable: {str(exc)[:200]}",
                    "psvChecks": [],
                },
                "monitoring": {
                    "status": "No Issues",
                    "flags": [],
                    "summary": "Monitoring unavailable - LLM analysis failed.",
                },
            }

    @app.post("/api/credential-check/batch")
    def credential_check_batch(request_body: dict) -> Dict[str, Any]:
        """Batch credentialing assessment for multiple providers in ONE LLM call."""
        batch_logger = logging.getLogger("universal_roster_v2.web.credential_check_batch")
        providers_input = request_body.get("providers", [])
        if not providers_input:
            return {"assessments": {}}

        import time as _bt
        t0 = _bt.time()
        today_str = datetime.date.today().isoformat()

        # 1. Fetch PSV data for all NPIs in parallel
        from concurrent.futures import ThreadPoolExecutor, as_completed

        npi_list = [str(p.get("npi", "")) for p in providers_input if p.get("npi")]
        psv_cache: Dict[str, Dict[str, Any]] = {}

        def _fetch_psv(npi: str) -> tuple:
            try:
                return npi, lookup_psv_data(npi)
            except Exception:
                return npi, {}

        with ThreadPoolExecutor(max_workers=min(10, len(npi_list))) as ex:
            for npi, data in ex.map(lambda n: _fetch_psv(n), npi_list):
                psv_cache[npi] = data

        # 2. Build ONE combined prompt with all providers
        provider_blocks = []
        for i, prov in enumerate(providers_input[:10]):  # max 10
            npi = str(prov.get("npi", ""))
            psv = psv_cache.get(npi, {})
            nppes = prov.get("nppesData", {})
            oig_clear = prov.get("oigClear", True)

            # Format PSV text (compact version)
            psv_parts = []
            if psv.get("nppes", {}).get("found"):
                n = psv["nppes"]
                psv_parts.append(f"NPPES: {n.get('first_name','')} {n.get('last_name','')}, NPI={npi}, Entity={n.get('entity_type','')}")
            for lic in psv.get("state_licenses", [])[:3]:
                psv_parts.append(f"License: {lic.get('state','')} #{lic.get('number','')}, Status={lic.get('status','')}, Expiry={lic.get('expiry','')}")
            for cert in psv.get("abms", [])[:2]:
                psv_parts.append(f"ABMS: {cert.get('board','')}, Status={cert.get('status','')}, Expiry={cert.get('expiry','')}")
            for dea in psv.get("dea", [])[:2]:
                psv_parts.append(f"DEA: #{dea.get('number','')}, Active={dea.get('activity','')}, Expiry={dea.get('expiry','')}")
            oig = psv.get("oig", {})
            psv_parts.append(f"OIG: {'FOUND - EXCLUDED' if oig.get('found') else 'CLEAR'}")
            sam = psv.get("sam", {})
            psv_parts.append(f"SAM: {'FOUND - EXCLUDED' if sam.get('found') else 'CLEAR'}")
            sanctions = psv.get("state_sanctions", {})
            psv_parts.append(f"State Sanctions: {'FOUND' if sanctions.get('found') else 'CLEAR'}")
            psv_parts.append(f"Medicare Opt-Out: {'YES' if psv.get('medicare_opt_out', {}).get('opted_out') else 'CLEAR'}")
            psv_parts.append(f"Deceased: {'FLAGGED' if psv.get('deceased', {}).get('flagged') else 'CLEAR'}")
            ba = psv.get("board_actions", {})
            psv_parts.append(f"Board Actions: {ba.get('count', 0)} found")

            roster_info = f"Name={prov.get('firstName','')} {prov.get('lastName','')}, NPI={npi}, " \
                          f"Specialty={prov.get('specialty','')}, License={prov.get('licenseState','')}/{prov.get('licenseNumber','')}, " \
                          f"DEA={prov.get('deaNumber','')}, Board={prov.get('boardName','')}"

            provider_blocks.append(
                f"--- PROVIDER {i+1} (NPI: {npi}) ---\n"
                f"PSV DATA:\n" + "\n".join(psv_parts) + "\n"
                f"ROSTER DATA: {roster_info}"
            )

        batch_prompt = (
            f"Perform credentialing assessment for {len(provider_blocks)} providers.\n"
            f"Today's date: {today_str}\n\n"
            + "\n\n".join(provider_blocks) +
            "\n\nReturn JSON with assessments keyed by NPI:\n"
            '{"assessments": {"<npi>": {"credentialing": {"status": "PSV Complete|Review Required", '
            '"summary": "...", "psvChecks": [{"source": "...", "status": "verified|expired|warning|missing|clear|excluded", "detail": "..."}]}, '
            '"monitoring": {"status": "No Issues|Flags Found", "flags": [{"category": "...", "severity": "critical|warning|info", '
            '"title": "...", "detail": "..."}], "summary": "..."}}, ...}}\n'
            "Mark expired credentials as critical. Compare ALL dates against today."
        )

        batch_system = (
            "You are a healthcare credentialing analyst performing Primary Source Verification (PSV) "
            "for multiple providers. Use the REAL PSV data provided. Be specific with dates and names. "
            f"TODAY'S DATE: {today_str}. Credentials with expiry BEFORE today are EXPIRED (critical). "
            "Within 90 days = EXPIRING SOON (warning). Return valid JSON only."
        )

        # 3. ONE Gemini call for all providers
        try:
            from universal_roster_v2.llm.gemini_provider import GeminiVertexProvider
            from google.genai import types as genai_types

            gemini = GeminiVertexProvider(settings=settings, model=settings.gemini_flash_model)
            if not gemini.is_available():
                raise RuntimeError("Gemini not available")

            client = gemini._get_client()
            config = genai_types.GenerateContentConfig(
                system_instruction=batch_system,
                temperature=0.1,
                max_output_tokens=16384,
                response_mime_type="application/json",
            )

            batch_logger.info("Batch credential check: %d providers, prompt_chars=%d", len(provider_blocks), len(batch_prompt))
            response = client.models.generate_content(
                model=settings.gemini_flash_model,
                contents=batch_prompt,
                config=config,
            )
            text = response.text.strip() if response.text else ""
            if not text:
                raise RuntimeError("Empty response")

            result = json.loads(text)
            assessments = result.get("assessments", result)  # Handle both wrapped and unwrapped
            elapsed = _bt.time() - t0
            batch_logger.info("Batch credential check OK: %.1fs, %d assessments", elapsed, len(assessments))
            return {"assessments": assessments, "elapsed_seconds": round(elapsed, 1)}

        except Exception as exc:
            batch_logger.warning("Batch credential check failed: %s", exc)
            return {"assessments": {}, "error": str(exc)}

    @app.get("/health")
    def health() -> Dict[str, Any]:
        return {"ok": True}

    @app.get("/workspaces")
    def list_workspaces() -> Dict[str, Any]:
        try:
            return {"items": store.list_workspaces()}
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces", response_model=CreateWorkspaceResponse)
    def create_workspace(req: CreateWorkspaceRequest) -> CreateWorkspaceResponse:
        try:
            workspace_id = store.create_workspace(
                workspace_path=req.workspace_path,
                tenant_id=req.tenant_id,
                client_id=req.client_id,
                thread_id=req.thread_id,
            )
            return CreateWorkspaceResponse(workspace_id=workspace_id)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Backward-compatible alias
    @app.post("/sessions", response_model=CreateWorkspaceResponse)
    def create_session_compat() -> CreateWorkspaceResponse:
        try:
            workspace_id = store.new_session()
            return CreateWorkspaceResponse(workspace_id=workspace_id)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/workspaces/{workspace_id}")
    def get_workspace(workspace_id: str) -> Dict[str, Any]:
        try:
            return store.state_payload(workspace_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    # Backward-compatible alias
    @app.get("/sessions/{session_id}")
    def get_session_compat(session_id: str) -> Dict[str, Any]:
        try:
            return store.state_payload(session_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/load")
    def load_file(workspace_id: str, req: LoadFileRequest) -> Dict[str, Any]:
        try:
            return store.load_file(
                workspace_id,
                file_path=req.file_path,
                roster_type=req.roster_type,
                profile_full_roster_learning=req.profile_full_roster_learning,
                profile_max_rows=req.profile_max_rows,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/upload")
    async def upload_file(
        workspace_id: str,
        file: UploadFile = File(...),
        roster_type: Optional[str] = Form(default=None),
        profile_full_roster_learning: Optional[bool] = Form(default=None),
        profile_max_rows: Optional[int] = Form(default=None),
    ) -> Dict[str, Any]:
        try:
            payload = await file.read()
            return store.upload_file(
                session_id=workspace_id,
                filename=file.filename or "",
                content=payload,
                roster_type=roster_type,
                profile_full_roster_learning=profile_full_roster_learning,
                profile_max_rows=profile_max_rows,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/notes")
    def add_notes(workspace_id: str, req: NoteContextRequest) -> Dict[str, Any]:
        try:
            context = store.add_instruction_context(
                session_id=workspace_id,
                free_text=req.free_text,
                client_rules=req.client_rules,
                schema_caveats=req.schema_caveats,
                exceptions=req.exceptions,
            )
            return {"instructions_context": context}
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/notes/upload")
    async def upload_note(workspace_id: str, file: UploadFile = File(...)) -> Dict[str, Any]:
        try:
            payload = await file.read()
            return store.upload_note_attachment(
                session_id=workspace_id,
                filename=file.filename or "",
                content=payload,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/suggest")
    def suggest(workspace_id: str, request: Request) -> Dict[str, Any]:
        try:
            result = store.suggest(workspace_id)
            return {
                **result,
                "operation_id": None,
                "request_id": getattr(request.state, "request_id", ""),
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/chat")
    def chat(workspace_id: str, req: ChatRequest, request: Request) -> Dict[str, Any]:
        try:
            payload = store.handle_chat(
                workspace_id,
                message=req.message,
                output_dir=req.output_dir,
                request_id=getattr(request.state, "request_id", ""),
            )
            payload["request_id"] = getattr(request.state, "request_id", "")
            return payload
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/chat/system")
    def chat_system(workspace_id: str, req: ChatRequest, request: Request) -> Dict[str, Any]:
        try:
            payload = store.handle_chat(
                workspace_id,
                message=req.message,
                output_dir=req.output_dir,
                skip_user_append=True,
                request_id=getattr(request.state, "request_id", ""),
            )
            payload["request_id"] = getattr(request.state, "request_id", "")
            return payload
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/toggle")
    def toggle(workspace_id: str, req: ToggleRequest) -> Dict[str, Any]:
        try:
            return store.toggle_item(workspace_id, req.item_type, req.item_id, req.approved)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/generate")
    def generate(workspace_id: str, req: GenerateRequest, request: Request) -> Dict[str, Any]:
        try:
            if settings.enable_async_operations:
                try:
                    operation = store.start_operation(
                        workspace_id,
                        kind="generate",
                        input_payload={
                            "mode": req.mode,
                            "output_dir": req.output_dir,
                            "pipeline_name": req.pipeline_name,
                        },
                        request_id=getattr(request.state, "request_id", ""),
                    )
                except RuntimeError as exc:
                    if "already running" not in str(exc).lower():
                        raise
                else:
                    return {
                        "operation_id": operation.get("id"),
                        "operation": operation,
                        "request_id": getattr(request.state, "request_id", ""),
                    }
            result = store.generate(
                session_id=workspace_id,
                mode=req.mode,
                output_dir=req.output_dir,
                pipeline_name=req.pipeline_name,
            )
            return {
                **result,
                "operation_id": None,
                "request_id": getattr(request.state, "request_id", ""),
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/training/export")
    def training_export(workspace_id: str, req: TrainingExportRequest, request: Request) -> Dict[str, Any]:
        try:
            if settings.enable_async_operations:
                try:
                    operation = store.start_operation(
                        workspace_id,
                        kind="export_training",
                        input_payload={"output_dir": req.output_dir},
                        request_id=getattr(request.state, "request_id", ""),
                    )
                except RuntimeError as exc:
                    if "already running" not in str(exc).lower():
                        raise
                else:
                    return {
                        "operation_id": operation.get("id"),
                        "operation": operation,
                        "request_id": getattr(request.state, "request_id", ""),
                    }
            result = store.export_training(session_id=workspace_id, output_dir=req.output_dir)
            return {
                **result,
                "operation_id": None,
                "request_id": getattr(request.state, "request_id", ""),
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/training/run")
    def training_run(workspace_id: str, req: TrainingRunRequest, request: Request) -> Dict[str, Any]:
        try:
            if settings.enable_async_operations:
                try:
                    operation = store.start_operation(
                        workspace_id,
                        kind="run_training",
                        input_payload={
                            "output_dir": req.output_dir,
                            "extra_args": req.extra_args or [],
                        },
                        request_id=getattr(request.state, "request_id", ""),
                    )
                except RuntimeError as exc:
                    if "already running" not in str(exc).lower():
                        raise
                else:
                    return {
                        "operation_id": operation.get("id"),
                        "operation": operation,
                        "request_id": getattr(request.state, "request_id", ""),
                    }
            result = store.run_training(session_id=workspace_id, output_dir=req.output_dir, extra_args=req.extra_args)
            return {
                **result,
                "operation_id": None,
                "request_id": getattr(request.state, "request_id", ""),
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/workspaces/{workspace_id}/learning/stats")
    def learning_stats(workspace_id: str) -> Dict[str, Any]:
        try:
            payload = store.state_payload(workspace_id)
            return payload.get("status", {}).get("learning", {})
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/run")
    def run_generated(workspace_id: str, req: RunGeneratedRequest, request: Request) -> Dict[str, Any]:
        try:
            if settings.enable_async_operations:
                try:
                    operation = store.start_operation(
                        workspace_id,
                        kind="run_generated",
                        input_payload={
                            "input_file": req.input_file,
                            "generated_dir": req.generated_dir,
                            "output_dir": req.output_dir,
                            "table_id": req.table_id,
                        },
                        request_id=getattr(request.state, "request_id", ""),
                    )
                except RuntimeError as exc:
                    if "already running" not in str(exc).lower():
                        raise
                else:
                    return {
                        "operation_id": operation.get("id"),
                        "operation": operation,
                        "request_id": getattr(request.state, "request_id", ""),
                    }
            result = store.run_generated(
                session_id=workspace_id,
                input_file=req.input_file,
                generated_dir=req.generated_dir,
                output_dir=req.output_dir,
                table_id=req.table_id,
            )
            return {
                **result,
                "operation_id": None,
                "request_id": getattr(request.state, "request_id", ""),
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/operations")
    def start_operation(workspace_id: str, req: StartOperationRequest, request: Request) -> Dict[str, Any]:
        try:
            operation = store.start_operation(
                workspace_id,
                kind=req.kind,
                input_payload=req.input,
                request_id=getattr(request.state, "request_id", ""),
                parent_operation_id=req.parent_operation_id,
            )
            return {
                "operation": operation,
                "operation_id": operation.get("id"),
                "request_id": getattr(request.state, "request_id", ""),
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/workspaces/{workspace_id}/operations")
    def list_operations(workspace_id: str) -> Dict[str, Any]:
        try:
            return {"items": store.list_operations(workspace_id)}
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/workspaces/{workspace_id}/operations/{operation_id}")
    def get_operation(workspace_id: str, operation_id: str) -> Dict[str, Any]:
        try:
            return store.get_operation(workspace_id, operation_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/operations/{operation_id}/cancel")
    def cancel_operation(workspace_id: str, operation_id: str) -> Dict[str, Any]:
        try:
            return store.cancel_operation(workspace_id, operation_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/workspaces/{workspace_id}/operations/{operation_id}/retry")
    def retry_operation(workspace_id: str, operation_id: str, req: RetryOperationRequest, request: Request) -> Dict[str, Any]:
        try:
            operation = store.retry_operation(
                workspace_id,
                operation_id,
                request_id=req.request_id or getattr(request.state, "request_id", ""),
            )
            return {
                "operation": operation,
                "operation_id": operation.get("id"),
                "request_id": getattr(request.state, "request_id", ""),
            }
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/workspaces/{workspace_id}/export/csv")
    def export_csv(workspace_id: str):
        """Apply approved mappings to the uploaded roster and return a clean CSV for download."""
        try:
            import io
            import pandas as pd
            from universal_roster_v2.core.output_builder import build_target_output
            from universal_roster_v2.core.transforms import apply_transformation

            web_session = store.get(workspace_id)
            session = web_session.session
            plan = session.state.plan or {}
            profile = session.state.profile or {}
            file_name = profile.get("file_name") or "roster"
            stem = Path(str(file_name)).stem

            # In demo mode, check if preprocessed file is ready
            if getattr(settings, "demo_mode", False):
                workspace_dir = store.workspace_root / workspace_id
                preprocessed_path = workspace_dir / f"{stem}_preprocessed.csv"
                if preprocessed_path.exists():
                    csv_bytes = preprocessed_path.read_bytes()
                    return StreamingResponse(
                        iter([csv_bytes]),
                        media_type="text/csv",
                        headers={
                            "Content-Disposition": f'attachment; filename="{stem}_processed.csv"',
                            "Content-Length": str(len(csv_bytes)),
                            **NO_CACHE_HEADERS,
                        },
                    )
                # Check if preprocessing is still running
                ops = store.list_operations(workspace_id)
                for op in ops:
                    if isinstance(op, dict) and op.get("kind") == "preprocess_roster" and op.get("status") in ("queued", "running"):
                        from fastapi.responses import JSONResponse
                        return JSONResponse(
                            status_code=202,
                            content={"status": "processing", "message": "Preprocessing still in progress. Please wait."},
                            headers=NO_CACHE_HEADERS,
                        )

            # Fallback: standard export path
            # Find the uploaded file
            upload_dir = store.workspace_root / workspace_id / "uploads"
            upload_files = list(upload_dir.glob("*")) if upload_dir.exists() else []
            if not upload_files:
                raise HTTPException(status_code=400, detail="No uploaded file found. Please upload a roster first.")
            source_path = sorted(upload_files, key=lambda p: p.stat().st_mtime)[-1]

            # Load into DataFrame
            suffix = source_path.suffix.lower()
            if suffix in (".xlsx", ".xls"):
                df = pd.read_excel(source_path, dtype=str)
            else:
                df = pd.read_csv(source_path, dtype=str)
            df = df.fillna("")

            # Apply approved transformations
            from universal_roster_v2.core.plan import PlanManager
            pm = PlanManager()
            raw_transforms = pm.combined_items(plan, "transformations") if plan else []
            approved_transforms = [t for t in raw_transforms if t.get("approved") is not False]
            for transform in approved_transforms:
                name = transform.get("name") or transform.get("id") or ""
                params = transform.get("params") or {}
                for col in (transform.get("source_columns") or []):
                    if col in df.columns:
                        try:
                            df[col] = apply_transformation(df[col], name, params)
                        except Exception:
                            pass  # skip failed transforms gracefully

            # Apply approved mappings to rename columns
            raw_mappings = pm.combined_items(plan, "mappings") if plan else []
            approved_mappings = [m for m in raw_mappings if m.get("approved") is not False and m.get("target_field")]
            output_df, _ = build_target_output(df, approved_mappings)

            # Serialize to CSV
            buf = io.StringIO()
            output_df.to_csv(buf, index=False)
            csv_bytes = buf.getvalue().encode("utf-8")

            return StreamingResponse(
                iter([csv_bytes]),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f'attachment; filename="{stem}_processed.csv"',
                    "Content-Length": str(len(csv_bytes)),
                    **NO_CACHE_HEADERS,
                },
            )
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/workspaces/{workspace_id}/operations/{operation_id}/logs")
    def operation_logs(workspace_id: str, operation_id: str) -> Dict[str, Any]:
        try:
            return store.list_operation_logs(workspace_id, operation_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/workspaces/{workspace_id}/events")
    def workspace_events(workspace_id: str, once: bool = False):
        if not settings.enable_sse_progress:
            raise HTTPException(status_code=404, detail="SSE disabled")

        def stream():
            emitted = False
            while True:
                event = store.next_event(workspace_id, timeout=15.0)
                if event is None:
                    yield ": heartbeat\n\n"
                    if once:
                        break
                    continue
                yield f"event: operation\ndata: {json.dumps(event, ensure_ascii=False)}\n\n"
                emitted = True
                if once and emitted:
                    break

        return StreamingResponse(stream(), media_type="text/event-stream")

    # Backward-compatible aliases for session routes
    @app.post("/sessions/{session_id}/load")
    def load_file_compat(session_id: str, req: LoadFileRequest) -> Dict[str, Any]:
        return load_file(session_id, req)

    @app.post("/sessions/{session_id}/upload")
    async def upload_file_compat(
        session_id: str,
        file: UploadFile = File(...),
        roster_type: Optional[str] = Form(default=None),
        profile_full_roster_learning: Optional[bool] = Form(default=None),
        profile_max_rows: Optional[int] = Form(default=None),
    ) -> Dict[str, Any]:
        return await upload_file(
            session_id,
            file,
            roster_type,
            profile_full_roster_learning,
            profile_max_rows,
        )

    @app.post("/sessions/{session_id}/suggest")
    def suggest_compat(session_id: str, request: Request) -> Dict[str, Any]:
        return suggest(session_id, request)

    @app.post("/sessions/{session_id}/chat")
    def chat_compat(session_id: str, req: ChatRequest, request: Request) -> Dict[str, Any]:
        return chat(session_id, req, request)

    @app.post("/sessions/{session_id}/chat/system")
    def chat_system_compat(session_id: str, req: ChatRequest, request: Request) -> Dict[str, Any]:
        return chat_system(session_id, req, request)

    @app.post("/sessions/{session_id}/toggle")
    def toggle_compat(session_id: str, req: ToggleRequest) -> Dict[str, Any]:
        return toggle(session_id, req)

    @app.post("/sessions/{session_id}/generate")
    def generate_compat(session_id: str, req: GenerateRequest, request: Request) -> Dict[str, Any]:
        return generate(session_id, req, request)

    @app.post("/sessions/{session_id}/training/export")
    def training_export_compat(session_id: str, req: TrainingExportRequest, request: Request) -> Dict[str, Any]:
        return training_export(session_id, req, request)

    @app.post("/sessions/{session_id}/training/run")
    def training_run_compat(session_id: str, req: TrainingRunRequest, request: Request) -> Dict[str, Any]:
        return training_run(session_id, req, request)

    @app.get("/sessions/{session_id}/learning/stats")
    def learning_stats_compat(session_id: str) -> Dict[str, Any]:
        return learning_stats(session_id)

    @app.post("/sessions/{session_id}/operations")
    def operations_start_compat(session_id: str, req: StartOperationRequest, request: Request) -> Dict[str, Any]:
        return start_operation(session_id, req, request)

    @app.get("/sessions/{session_id}/operations")
    def operations_list_compat(session_id: str) -> Dict[str, Any]:
        return list_operations(session_id)

    @app.get("/sessions/{session_id}/operations/{operation_id}")
    def operations_get_compat(session_id: str, operation_id: str) -> Dict[str, Any]:
        return get_operation(session_id, operation_id)

    @app.post("/sessions/{session_id}/operations/{operation_id}/cancel")
    def operations_cancel_compat(session_id: str, operation_id: str) -> Dict[str, Any]:
        return cancel_operation(session_id, operation_id)

    @app.post("/sessions/{session_id}/operations/{operation_id}/retry")
    def operations_retry_compat(
        session_id: str,
        operation_id: str,
        req: RetryOperationRequest,
        request: Request,
    ) -> Dict[str, Any]:
        return retry_operation(session_id, operation_id, req, request)

    @app.get("/sessions/{session_id}/operations/{operation_id}/logs")
    def operations_logs_compat(session_id: str, operation_id: str) -> Dict[str, Any]:
        return operation_logs(session_id, operation_id)

    @app.get("/sessions/{session_id}/events")
    def events_compat(session_id: str, once: bool = False):
        return workspace_events(session_id, once=once)

    return app


def run() -> None:
    try:
        import uvicorn
    except Exception as exc:
        raise RuntimeError("uvicorn is required to run the web server") from exc

    app = create_app()
    uvicorn.run(app, host="127.0.0.1", port=8000)


app = create_app()
