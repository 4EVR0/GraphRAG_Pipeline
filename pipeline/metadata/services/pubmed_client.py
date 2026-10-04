import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from pipeline.common.config.settings import settings

logger = logging.getLogger(__name__)

# 일시적 오류(요청 과다, 서버 오류)만 재시도한다.
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})


class PubMedClient:
    def __init__(
        self,
        session: Optional[requests.Session] = None,
        sleep: float | None = None,
        max_retries: int | None = None,
    ) -> None:
        self.base_url = settings.ncbi_base
        self.session = session or requests.Session()
        self.sleep = settings.request_sleep if sleep is None else sleep
        self.max_retries = settings.request_max_retries if max_retries is None else max_retries
        self.request_count = 0

    def _request(self, method: str, endpoint: str, params: Dict[str, Any]) -> requests.Response:
        request_params = {
            **params,
            "tool": settings.ncbi_tool,
            "email": settings.ncbi_email,
        }

        if settings.ncbi_api_key:
            request_params["api_key"] = settings.ncbi_api_key

        url = f"{self.base_url}/{endpoint}"
        for attempt in range(self.max_retries + 1):
            self.request_count += 1
            try:
                if method == "POST":
                    response = self.session.post(url, data=request_params, timeout=settings.request_timeout)
                else:
                    response = self.session.get(url, params=request_params, timeout=settings.request_timeout)
                if response.status_code not in RETRY_STATUS:
                    response.raise_for_status()
                    time.sleep(self.sleep)
                    return response
                error: Exception = requests.HTTPError(f"HTTP {response.status_code}", response=response)
            except (requests.ConnectionError, requests.Timeout) as exc:
                error = exc
            if attempt == self.max_retries:
                raise error
            wait = self.sleep + 2 ** attempt
            logger.warning("NCBI %s 재시도 %s/%s (%s), %.1fs 대기", endpoint, attempt + 1, self.max_retries, error, wait)
            time.sleep(wait)
        raise AssertionError("unreachable")

    def _get(self, endpoint: str, params: Dict[str, Any]) -> requests.Response:
        return self._request("GET", endpoint, params)

    def search(self, query: str, retmax: int, retstart: int = 0) -> Tuple[int, List[str]]:
        """검색 결과 전체 건수와 PMID 목록(retstart부터 retmax건)을 반환한다."""
        response = self._get(
            "esearch.fcgi",
            {
                "db": "pubmed",
                "term": query,
                "retmax": retmax,
                "retstart": retstart,
                "retmode": "json",
            },
        )
        data = response.json().get("esearchresult", {})
        return int(data.get("count", 0)), list(data.get("idlist", []))

    def search_pmids(self, query: str, retmax: int) -> List[str]:
        return self.search(query, retmax)[1]

    def fetch_pubmed_xml(self, pmids: List[str]) -> Optional[str]:
        if not pmids:
            return None

        # PMID가 많으면 URL 길이 제한에 걸리므로 POST로 보낸다.
        response = self._request(
            "POST",
            "efetch.fcgi",
            {
                "db": "pubmed",
                "id": ",".join(pmids),
                "retmode": "xml",
            },
        )
        return response.text
