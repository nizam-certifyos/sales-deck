from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional

from universal_roster_v2.config import Settings


@dataclass
class _CacheEntry:
    value: Optional[Dict[str, Any]]
    expires_at: float


class _AuthContext:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _service_account_token(self) -> str:
        key_path = str(getattr(self.settings, "quality_audit_service_account_key_path", "") or "").strip()
        if not key_path:
            return ""
        try:
            from google.auth.transport.requests import Request  # type: ignore
            from google.oauth2 import service_account  # type: ignore

            creds = service_account.Credentials.from_service_account_file(
                key_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            creds.refresh(Request())
            return str(getattr(creds, "token", "") or "").strip()
        except Exception:
            return ""

    def bearer_token(self, explicit_api_key: str) -> str:
        if str(explicit_api_key or "").strip():
            return str(explicit_api_key or "").strip()
        return self._service_account_token()


class _BaseReferenceClient:
    def __init__(
        self,
        *,
        endpoint: str,
        api_key: str,
        timeout_seconds: float,
        cache_ttl_seconds: int,
        auth: Optional[_AuthContext] = None,
    ):
        self.endpoint = str(endpoint or "").strip()
        self.api_key = str(api_key or "").strip()
        self.timeout_seconds = max(0.1, float(timeout_seconds or 2.0))
        self.cache_ttl_seconds = max(0, int(cache_ttl_seconds or 0))
        self._cache: Dict[str, _CacheEntry] = {}
        self._auth = auth

    def _cache_get(self, key: str) -> Optional[Dict[str, Any]]:
        entry = self._cache.get(key)
        if not entry:
            return None
        if entry.expires_at < time.time():
            self._cache.pop(key, None)
            return None
        return entry.value

    def _cache_put(self, key: str, value: Optional[Dict[str, Any]]) -> None:
        if self.cache_ttl_seconds <= 0:
            return
        self._cache[key] = _CacheEntry(value=value, expires_at=time.time() + float(self.cache_ttl_seconds))

    def _request_json(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.endpoint:
            return None
        params = {str(k): str(v) for k, v in query.items() if str(v or "").strip()}
        url = self.endpoint
        if params:
            glue = "&" if "?" in url else "?"
            url = f"{url}{glue}{urllib.parse.urlencode(params)}"

        headers = {"Accept": "application/json"}
        token = self._auth.bearer_token(self.api_key) if self._auth else self.api_key
        if token:
            headers["Authorization"] = f"Bearer {token}"

        req = urllib.request.Request(url=url, method="GET", headers=headers)
        with urllib.request.urlopen(req, timeout=self.timeout_seconds) as response:
            payload = response.read().decode("utf-8", errors="ignore")
            if not payload.strip():
                return None
            parsed = json.loads(payload)
            return parsed if isinstance(parsed, dict) else None


class NPPESClient(_BaseReferenceClient):
    @staticmethod
    def _normalize_nppes_record(raw: Dict[str, Any]) -> Dict[str, Any]:
        basic = raw.get("basic") if isinstance(raw.get("basic"), dict) else {}
        taxonomies = raw.get("taxonomies") if isinstance(raw.get("taxonomies"), list) else []
        first_taxonomy = next((item for item in taxonomies if isinstance(item, dict)), {})

        organization_name = str(basic.get("organization_name") or "").strip()
        first_name = str(basic.get("first_name") or "").strip()
        last_name = str(basic.get("last_name") or "").strip()
        full_name = " ".join(part for part in [first_name, last_name] if part).strip()

        return {
            **raw,
            "provider_name": organization_name or full_name or str(raw.get("provider_name") or "").strip(),
            "taxonomy": str(first_taxonomy.get("code") or raw.get("taxonomy") or "").strip(),
            "primary_taxonomy": str(first_taxonomy.get("code") or raw.get("primary_taxonomy") or "").strip(),
            "taxonomy_code": str(first_taxonomy.get("code") or raw.get("taxonomy_code") or "").strip(),
        }

    def lookup_npi(self, npi: str) -> Optional[Dict[str, Any]]:
        key = f"npi::{str(npi or '').strip()}"
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        endpoint_l = self.endpoint.lower()
        query = {"npi": npi}
        if "npiregistry.cms.hhs.gov" in endpoint_l:
            query = {"number": npi, "version": "2.1", "limit": "1"}

        payload = self._request_json(query)
        result: Optional[Dict[str, Any]] = None
        if isinstance(payload, dict):
            if isinstance(payload.get("result"), dict):
                result = payload.get("result")
            elif isinstance(payload.get("results"), list) and payload.get("results"):
                first = payload.get("results")[0]
                if isinstance(first, dict):
                    result = first
            else:
                result = payload

        if isinstance(result, dict):
            result = self._normalize_nppes_record(result)

        self._cache_put(key, result)
        return result


class NUCCClient(_BaseReferenceClient):
    def lookup_taxonomy(self, code: str) -> Optional[Dict[str, Any]]:
        key = f"taxonomy::{str(code or '').strip()}"
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        payload = self._request_json({"taxonomy": code})
        result: Optional[Dict[str, Any]] = None
        if isinstance(payload, dict):
            if isinstance(payload.get("result"), dict):
                result = payload.get("result")
            elif isinstance(payload.get("results"), list) and payload.get("results"):
                first = payload.get("results")[0]
                if isinstance(first, dict):
                    result = first
            else:
                result = payload
        self._cache_put(key, result)
        return result


class ClientReferenceClient(_BaseReferenceClient):
    def lookup_npi(self, npi: str) -> Optional[Dict[str, Any]]:
        key = f"client_npi::{str(npi or '').strip()}"
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        payload = self._request_json({"npi": npi})
        result: Optional[Dict[str, Any]] = payload if isinstance(payload, dict) else None
        self._cache_put(key, result)
        return result


class BigQueryReferenceClient:
    def __init__(self, *, settings: Settings):
        self.settings = settings
        self._cache: Dict[str, _CacheEntry] = {}
        self.cache_ttl_seconds = max(0, int(getattr(settings, "quality_audit_enrichment_cache_ttl_seconds", 900) or 900))

    def _cache_get(self, key: str) -> Optional[Dict[str, Any]]:
        entry = self._cache.get(key)
        if not entry:
            return None
        if entry.expires_at < time.time():
            self._cache.pop(key, None)
            return None
        return entry.value

    def _cache_put(self, key: str, value: Optional[Dict[str, Any]]) -> None:
        if self.cache_ttl_seconds <= 0:
            return
        self._cache[key] = _CacheEntry(value=value, expires_at=time.time() + float(self.cache_ttl_seconds))

    def _client(self):
        from google.cloud import bigquery  # type: ignore

        key_path = str(getattr(self.settings, "quality_audit_service_account_key_path", "") or "").strip()
        project_id = str(getattr(self.settings, "quality_audit_bq_project_id", "") or "").strip() or None
        if key_path:
            return bigquery.Client.from_service_account_json(key_path, project=project_id)
        return bigquery.Client(project=project_id)

    def lookup_npi(self, npi: str) -> Optional[Dict[str, Any]]:
        project = str(getattr(self.settings, "quality_audit_bq_project_id", "") or "").strip()
        dataset = str(getattr(self.settings, "quality_audit_bq_dataset", "") or "").strip()
        if not project or not dataset or not str(npi or "").strip():
            return None

        cache_key = f"bq_npi::{project}.{dataset}::{str(npi).strip()}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        client = self._client()
        query = f"""
            SELECT
              CAST(npi AS STRING) AS npi,
              CAST(provider_name AS STRING) AS provider_name,
              CAST(taxonomy AS STRING) AS taxonomy,
              CAST(primary_taxonomy AS STRING) AS primary_taxonomy,
              CAST(source AS STRING) AS source
            FROM `{project}.{dataset}.npi_reference`
            WHERE CAST(npi AS STRING) = @npi
            LIMIT 1
        """.strip()

        from google.cloud import bigquery  # type: ignore

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("npi", "STRING", str(npi).strip()),
            ]
        )
        rows = list(client.query(query, job_config=job_config).result())
        if not rows:
            self._cache_put(cache_key, None)
            return None

        row = rows[0]
        result = {
            "npi": str(getattr(row, "npi", "") or ""),
            "provider_name": str(getattr(row, "provider_name", "") or ""),
            "taxonomy": str(getattr(row, "taxonomy", "") or ""),
            "primary_taxonomy": str(getattr(row, "primary_taxonomy", "") or ""),
            "source": str(getattr(row, "source", "") or "bq"),
        }
        self._cache_put(cache_key, result)
        return result


    def bulk_lookup_npis(self, npis: list[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """Bulk NPI lookup — single BQ query for all NPIs. Returns {npi: result_or_None}."""
        project = str(getattr(self.settings, "quality_audit_bq_project_id", "") or "").strip()
        dataset = str(getattr(self.settings, "quality_audit_bq_dataset", "") or "").strip()
        if not project or not dataset:
            return {}

        clean_npis = [str(n).strip() for n in npis if str(n or "").strip()]
        if not clean_npis:
            return {}

        # Check cache first
        results: Dict[str, Optional[Dict[str, Any]]] = {}
        uncached: list[str] = []
        for npi in clean_npis:
            cache_key = f"bq_npi::{project}.{dataset}::{npi}"
            cached = self._cache_get(cache_key)
            if cached is not None:
                results[npi] = cached
            else:
                uncached.append(npi)

        if not uncached:
            return results

        try:
            from google.cloud import bigquery

            client = self._client()
            npi_list = ", ".join(f"'{n}'" for n in uncached[:500])
            query = f"""
                SELECT
                  NPI AS npi,
                  Provider_First_Name AS first_name,
                  Provider_Last_Name AS last_name,
                  NPI_Deactivation_Date AS deactivation_date,
                  Entity_Type_Code AS entity_type
                FROM `{project}.{dataset}.nppes_raw`
                WHERE NPI IN ({npi_list})
            """.strip()

            rows = list(client.query(query).result())
            found_npis = set()
            for row in rows:
                npi = str(getattr(row, "npi", "") or "")
                if not npi:
                    continue
                found_npis.add(npi)
                result = {
                    "npi": npi,
                    "first_name": str(getattr(row, "first_name", "") or ""),
                    "last_name": str(getattr(row, "last_name", "") or ""),
                    "provider_name": f"{getattr(row, 'first_name', '')} {getattr(row, 'last_name', '')}".strip(),
                    "deactivation_date": str(getattr(row, "deactivation_date", "") or ""),
                    "entity_type": str(getattr(row, "entity_type", "") or ""),
                    "source": "bq_nppes_raw",
                }
                results[npi] = result
                cache_key = f"bq_npi::{project}.{dataset}::{npi}"
                self._cache_put(cache_key, result)

            # Mark not-found NPIs
            for npi in uncached:
                if npi not in found_npis:
                    results[npi] = None
                    cache_key = f"bq_npi::{project}.{dataset}::{npi}"
                    self._cache_put(cache_key, None)

        except Exception:
            pass

        return results


class ReferenceClientFactory:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._auth = _AuthContext(settings)

    def nppes(self) -> Optional[NPPESClient]:
        if not bool(getattr(self.settings, "quality_audit_nppes_enabled", False)):
            return None
        return NPPESClient(
            endpoint=str(getattr(self.settings, "quality_audit_nppes_endpoint", "") or ""),
            api_key=str(getattr(self.settings, "quality_audit_nppes_api_key", "") or ""),
            timeout_seconds=float(getattr(self.settings, "quality_audit_enrichment_timeout_seconds", 2.0) or 2.0),
            cache_ttl_seconds=int(getattr(self.settings, "quality_audit_enrichment_cache_ttl_seconds", 900) or 900),
            auth=self._auth,
        )

    def nucc(self) -> Optional[NUCCClient]:
        if not bool(getattr(self.settings, "quality_audit_nucc_enabled", False)):
            return None
        return NUCCClient(
            endpoint=str(getattr(self.settings, "quality_audit_nucc_endpoint", "") or ""),
            api_key=str(getattr(self.settings, "quality_audit_nucc_api_key", "") or ""),
            timeout_seconds=float(getattr(self.settings, "quality_audit_enrichment_timeout_seconds", 2.0) or 2.0),
            cache_ttl_seconds=int(getattr(self.settings, "quality_audit_enrichment_cache_ttl_seconds", 900) or 900),
            auth=self._auth,
        )

    def client_refs(self) -> Optional[ClientReferenceClient]:
        if not bool(getattr(self.settings, "quality_audit_client_refs_enabled", False)):
            return None
        return ClientReferenceClient(
            endpoint=str(getattr(self.settings, "quality_audit_client_refs_endpoint", "") or ""),
            api_key=str(getattr(self.settings, "quality_audit_client_refs_api_key", "") or ""),
            timeout_seconds=float(getattr(self.settings, "quality_audit_enrichment_timeout_seconds", 2.0) or 2.0),
            cache_ttl_seconds=int(getattr(self.settings, "quality_audit_enrichment_cache_ttl_seconds", 900) or 900),
            auth=self._auth,
        )

    def bq(self) -> Optional[BigQueryReferenceClient]:
        if not bool(getattr(self.settings, "quality_audit_bq_enabled", False)):
            return None
        if not str(getattr(self.settings, "quality_audit_bq_project_id", "") or "").strip():
            return None
        if not str(getattr(self.settings, "quality_audit_bq_dataset", "") or "").strip():
            return None
        return BigQueryReferenceClient(settings=self.settings)
