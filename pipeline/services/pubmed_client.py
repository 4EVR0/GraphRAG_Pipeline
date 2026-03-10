import time
from typing import Any, Dict, List, Optional

import requests

from pipeline.config.settings import settings


class PubMedClient:
    def __init__(self) -> None:
        self.base_url = settings.ncbi_base

    def _get(self, endpoint: str, params: Dict[str, Any]) -> requests.Response:
        request_params = {
            **params,
            "tool": settings.ncbi_tool,
            "email": settings.ncbi_email,
        }

        if settings.ncbi_api_key:
            request_params["api_key"] = settings.ncbi_api_key

        response = requests.get(
            f"{self.base_url}/{endpoint}",
            params=request_params,
            timeout=settings.request_timeout,
        )
        response.raise_for_status()
        time.sleep(settings.request_sleep)
        return response

    def search_pmids(self, query: str, retmax: int) -> List[str]:
        response = self._get(
            "esearch.fcgi",
            {
                "db": "pubmed",
                "term": query,
                "retmax": retmax,
                "retmode": "json",
            },
        )
        data = response.json()
        return data.get("esearchresult", {}).get("idlist", [])

    def fetch_pubmed_xml(self, pmids: List[str]) -> Optional[str]:
        if not pmids:
            return None

        response = self._get(
            "efetch.fcgi",
            {
                "db": "pubmed",
                "id": ",".join(pmids),
                "retmode": "xml",
            },
        )
        return response.text