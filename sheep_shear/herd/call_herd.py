import time
import random
import requests
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Iterable


@dataclass(frozen=True)
class Form4Filing:
    cik: str
    form: str
    filingDate: Optional[str]
    acceptanceDateTime: Optional[str]
    accessionNumber: Optional[str]
    primaryDocument: Optional[str]
    primaryDocUrl: Optional[str]


class SecEdgarClient:
    """
    Minimal OO client for pulling Form 4 filings from SEC 'submissions' JSON.

    Notes:
    - SEC expects a descriptive User-Agent with contact info.
    - CIK in the submissions URL must be 10-digit zero-padded.
    - Archive URLs use the non-padded integer CIK path segment.
    """

    BASE_SUBMISSIONS = "https://data.sec.gov/submissions"
    BASE_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"

    def __init__(
        self,
        user_agent: str,
        timeout: int = 30,
        max_retries: int = 5,
        backoff_base: float = 2.0,
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,  # MUST be real contact info
                "Accept-Encoding": "gzip, deflate",
                "Accept": "application/json, text/plain, */*",
            }
        )

    # ---------- Public API ----------

    def get_submissions(self, cik: str) -> Dict[str, Any]:
        cik10 = self._cik_pad10(cik)
        url = f"{self.BASE_SUBMISSIONS}/CIK{cik10}.json"
        return self._get_json(url)

    def get_form4_filings(
        self,
        cik: str,
        include_amended: bool = True,
    ) -> List[Form4Filing]:
        """
        Returns newest-first Form 4 (and optionally 4/A) from filings.recent.
        """
        cik10 = self._cik_pad10(cik)
        sub = self.get_submissions(cik10)

        recent = sub.get("filings", {}).get("recent", {}) or {}
        forms: List[str] = recent.get("form", []) or []

        wanted = {"4"}
        if include_amended:
            wanted.add("4/A")

        filings: List[Form4Filing] = []
        for i, form in enumerate(forms):
            if form not in wanted:
                continue

            accession = self._col(recent, "accessionNumber", i)
            primary_doc = self._col(recent, "primaryDocument", i)

            primary_url = (
                self.build_primary_doc_url(cik10, accession, primary_doc)
                if accession and primary_doc
                else None
            )

            filings.append(
                Form4Filing(
                    cik=cik10,
                    form=form,
                    filingDate=self._col(recent, "filingDate", i),
                    acceptanceDateTime=self._col(recent, "acceptanceDateTime", i),
                    accessionNumber=accession,
                    primaryDocument=primary_doc,
                    primaryDocUrl=primary_url,
                )
            )

        filings.sort(
            key=lambda x: (x.filingDate or "", x.acceptanceDateTime or ""),
            reverse=True,
        )
        return filings

    def build_primary_doc_url(
        self,
        cik: str,
        accession_number: str,
        primary_document: str,
    ) -> str:
        """
        https://www.sec.gov/Archives/edgar/data/{cik_int}/{accessionNoNoDashes}/{primaryDocument}
        """
        cik_int = str(int(self._cik_pad10(cik)))  # unpadded for archive path
        acc_nodash = accession_number.replace("-", "")
        return f"{self.BASE_ARCHIVES}/{cik_int}/{acc_nodash}/{primary_document}"

    def build_index_json_url(self, cik: str, accession_number: str) -> str:
        """
        Optional helper:
        https://www.sec.gov/Archives/edgar/data/{cik_int}/{accessionNoNoDashes}/index.json
        """
        cik_int = str(int(self._cik_pad10(cik)))
        acc_nodash = accession_number.replace("-", "")
        return f"{self.BASE_ARCHIVES}/{cik_int}/{acc_nodash}/index.json"

    # ---------- Internals ----------

    def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        r = self._get(url, params=params)
        return r.json()

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        for attempt in range(self.max_retries):
            r = self.session.get(url, params=params, timeout=self.timeout)
            if r.status_code == 200:
                return r

            if r.status_code in (429, 500, 502, 503, 504):
                # exponential backoff + jitter
                sleep = (self.backoff_base ** attempt) + random.random()
                time.sleep(sleep)
                continue

            r.raise_for_status()

        raise RuntimeError(f"Failed after {self.max_retries} retries: {url} (last={r.status_code})")

    @staticmethod
    def _cik_pad10(cik: str) -> str:
        return str(int(cik)).zfill(10)

    @staticmethod
    def _col(recent: Dict[str, Any], field: str, i: int) -> Optional[str]:
        arr = recent.get(field) or []
        return arr[i] if i < len(arr) else None


# ---------------- Example usage ----------------
if __name__ == "__main__":
    client = SecEdgarClient(user_agent="MyEdgarNotebook youremail@yourdomain.com")

    filings = client.get_form4_filings("0001652044")  # Alphabet
    for f in filings[:5]:
        print(f.form, f.filingDate, f.primaryDocUrl)
