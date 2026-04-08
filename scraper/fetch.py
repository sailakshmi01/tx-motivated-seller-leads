#!/usr/bin/env python3
"""
Texas Multi-County Motivated Seller Lead Scraper
=================================================
Covers the 10 largest Texas counties by population + Tyler County

Counties and their portals:
  Harris      (~4.7M)  cclerk.hctx.net          (Fidlar iDOC / custom)
  Dallas      (~2.6M)  dallas.tx.publicsearch.us (Fidlar PublicSearch)
  Tarrant     (~2.1M)  tarrant.tx.publicsearch.us (Fidlar PublicSearch)
  Bexar       (~2.0M)  bexar.tx.publicsearch.us  (Fidlar PublicSearch)
  Travis      (~1.3M)  travis.tx.publicsearch.us  (Fidlar PublicSearch)
  Collin      (~1.1M)  collin.tx.publicsearch.us  (Fidlar PublicSearch)
  Hidalgo     (~900K)  hidalgo.tx.publicsearch.us (Fidlar PublicSearch)
  Denton      (~900K)  denton.tx.publicsearch.us  (Fidlar PublicSearch)
  Fort Bend   (~800K)  fortbend.tx.publicsearch.us (Fidlar PublicSearch)
  Montgomery  (~700K)  montgomery.tx.publicsearch.us (Fidlar PublicSearch)
  Tyler       (~21K)   co.tyler.tx.us/page/tyler.Forclosures (HTML scrape)

Lead types collected:
  LP, NOFC, TAXDEED, JUD/CCJ/DRJUD,
  LNCORPTX/LNIRS/LNFED, LN/LNMECH/LNHOA, MEDLN, PRO, NOC, RELLP

Output:
  dashboard/records.json, data/records.json, data/ghl_export.csv
  One JSON per county: data/{county}_records.json
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlencode, quote

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("tx-scraper")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
SOURCE = "Texas County Public Records"

OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]

# ---------------------------------------------------------------------------
# Document type mappings (Fidlar codes → our categories)
# ---------------------------------------------------------------------------
DOC_TYPE_MAP = {
    # Lis Pendens
    "LP": "LP", "LIS PENDENS": "LP", "LISPEN": "LP", "LISPEND": "LP",
    # Notice of Foreclosure / Substitute Trustee Sale
    "NOFC": "NOFC", "NOTICE OF FORECLOSURE": "NOFC", "NOTFORSALE": "NOFC",
    "NFORSALE": "NOFC", "STS": "NOFC", "SUBTRUSTEE": "NOFC",
    "NOTICE OF SUBSTITUTE TRUSTEE": "NOFC", "NOTTRSALE": "NOFC",
    "NOTICEOFTRUSTEE": "NOFC", "NTS": "NOFC",
    # Tax Deed
    "TAXDEED": "TAXDEED", "TAX DEED": "TAXDEED", "TXDEED": "TAXDEED",
    # Judgments
    "JUD": "JUD", "JUDGMENT": "JUD", "JUDGEMENT": "JUD", "ABJUDGMENT": "JUD",
    "CCJ": "CCJ", "CERT JUDGMENT": "CCJ", "CERTIFIED JUDGMENT": "CCJ",
    "DRJUD": "DRJUD", "DOMESTIC JUDGMENT": "DRJUD",
    # Corp / IRS / Federal Liens
    "LNCORPTX": "LNCORPTX", "CORP TAX LIEN": "LNCORPTX", "CORPTAXLIEN": "LNCORPTX",
    "TXLIEN": "LNCORPTX", "STATE TAX LIEN": "LNCORPTX", "STLIEN": "LNCORPTX",
    "LNIRS": "LNIRS", "IRS LIEN": "LNIRS", "IRSLIEN": "LNIRS", "FTL": "LNIRS",
    "FEDTAXLIEN": "LNIRS",
    "LNFED": "LNFED", "FEDERAL LIEN": "LNFED", "FED LN": "LNFED", "FEDLIEN": "LNFED",
    # General Liens
    "LN": "LN", "LIEN": "LN",
    "LNMECH": "LNMECH", "MECHANIC LIEN": "LNMECH", "MECH LIEN": "LNMECH",
    "MECHANIC'S LIEN": "LNMECH", "MECHLIEN": "LNMECH", "ML": "LNMECH",
    "LNHOA": "LNHOA", "HOA LIEN": "LNHOA", "HOALIEN": "LNHOA",
    # Medicaid
    "MEDLN": "MEDLN", "MEDICAID LIEN": "MEDLN", "MEDLIEN": "MEDLN",
    # Probate
    "PRO": "PRO", "PROBATE": "PRO", "PROBATEDEED": "PRO", "PROBINVENT": "PRO",
    "LETTERS TESTAMENTARY": "PRO", "LETTEST": "PRO", "LETADMIN": "PRO",
    "APPLICATION FOR PROBATE": "PRO", "WILL": "PRO",
    # Notice of Commencement
    "NOC": "NOC", "NOTICE OF COMMENCEMENT": "NOC", "NOTCOMM": "NOC",
    # Release Lis Pendens
    "RELLP": "RELLP", "RELEASE LIS PENDENS": "RELLP", "RELLISPEN": "RELLP",
}

TARGET_CATS = {
    "LP", "NOFC", "TAXDEED", "JUD", "CCJ", "DRJUD",
    "LNCORPTX", "LNIRS", "LNFED", "LN", "LNMECH", "LNHOA",
    "MEDLN", "PRO", "NOC", "RELLP",
}

CAT_LABELS = {
    "LP": "Lis Pendens",
    "NOFC": "Notice of Foreclosure",
    "TAXDEED": "Tax Deed",
    "JUD": "Judgment",
    "CCJ": "Certified Judgment",
    "DRJUD": "Domestic Judgment",
    "LNCORPTX": "Corp Tax Lien",
    "LNIRS": "IRS Lien",
    "LNFED": "Federal Lien",
    "LN": "Lien",
    "LNMECH": "Mechanic Lien",
    "LNHOA": "HOA Lien",
    "MEDLN": "Medicaid Lien",
    "PRO": "Probate",
    "NOC": "Notice of Commencement",
    "RELLP": "Release Lis Pendens",
}

# ---------------------------------------------------------------------------
# County registry
# ---------------------------------------------------------------------------
COUNTIES = {
    "tyler": {
        "name": "Tyler County",
        "state": "TX",
        "seat": "Woodville",
        "seat_zip": "75979",
        "portal": "https://www.co.tyler.tx.us/page/tyler.Forclosures",
        "type": "tyler_foreclosure_page",
        "population": "~21K",
    },
    "harris": {
        "name": "Harris County",
        "state": "TX",
        "seat": "Houston",
        "seat_zip": "77002",
        "portal": "https://www.cclerk.hctx.net/applications/websearch/",
        "api_base": "https://www.cclerk.hctx.net",
        "type": "harris_idoc",
        "population": "~4.7M",
    },
    "dallas": {
        "name": "Dallas County",
        "state": "TX",
        "seat": "Dallas",
        "seat_zip": "75201",
        "portal": "https://dallas.tx.publicsearch.us/",
        "api_base": "https://dallas.tx.publicsearch.us",
        "type": "fidlar_publicsearch",
        "population": "~2.6M",
    },
    "tarrant": {
        "name": "Tarrant County",
        "state": "TX",
        "seat": "Fort Worth",
        "seat_zip": "76102",
        "portal": "https://tarrant.tx.publicsearch.us/",
        "api_base": "https://tarrant.tx.publicsearch.us",
        "type": "fidlar_publicsearch",
        "population": "~2.1M",
    },
    "bexar": {
        "name": "Bexar County",
        "state": "TX",
        "seat": "San Antonio",
        "seat_zip": "78205",
        "portal": "https://bexar.tx.publicsearch.us/",
        "api_base": "https://bexar.tx.publicsearch.us",
        "type": "fidlar_publicsearch",
        "population": "~2.0M",
    },
    "travis": {
        "name": "Travis County",
        "state": "TX",
        "seat": "Austin",
        "seat_zip": "78701",
        "portal": "https://travis.tx.publicsearch.us/",
        "api_base": "https://travis.tx.publicsearch.us",
        "type": "fidlar_publicsearch",
        "population": "~1.3M",
    },
    "collin": {
        "name": "Collin County",
        "state": "TX",
        "seat": "McKinney",
        "seat_zip": "75069",
        "portal": "https://collin.tx.publicsearch.us/",
        "api_base": "https://collin.tx.publicsearch.us",
        "type": "fidlar_publicsearch",
        "population": "~1.1M",
    },
    "hidalgo": {
        "name": "Hidalgo County",
        "state": "TX",
        "seat": "Edinburg",
        "seat_zip": "78539",
        "portal": "https://hidalgo.tx.publicsearch.us/",
        "api_base": "https://hidalgo.tx.publicsearch.us",
        "type": "fidlar_publicsearch",
        "population": "~900K",
    },
    "denton": {
        "name": "Denton County",
        "state": "TX",
        "seat": "Denton",
        "seat_zip": "76201",
        "portal": "https://denton.tx.publicsearch.us/",
        "api_base": "https://denton.tx.publicsearch.us",
        "type": "fidlar_publicsearch",
        "population": "~900K",
    },
    "fortbend": {
        "name": "Fort Bend County",
        "state": "TX",
        "seat": "Richmond",
        "seat_zip": "77469",
        "portal": "https://fortbend.tx.publicsearch.us/",
        "api_base": "https://fortbend.tx.publicsearch.us",
        "type": "fidlar_publicsearch",
        "population": "~800K",
    },
    "montgomery": {
        "name": "Montgomery County",
        "state": "TX",
        "seat": "Conroe",
        "seat_zip": "77301",
        "portal": "https://montgomery.tx.publicsearch.us/",
        "api_base": "https://montgomery.tx.publicsearch.us",
        "type": "fidlar_publicsearch",
        "population": "~700K",
    },
}

# Which counties to scrape (env var override supported, e.g. COUNTIES=harris,dallas)
ENABLED_COUNTIES = os.getenv("COUNTIES", ",".join(COUNTIES.keys())).split(",")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def make_session(referer: str = "") -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })
    if referer:
        s.headers["Referer"] = referer
    return s


def safe_get(session, url, attempts=3, **kwargs):
    for attempt in range(1, attempts + 1):
        try:
            r = session.get(url, timeout=30, **kwargs)
            if r.status_code < 500:
                return r
            log.warning("HTTP %d on attempt %d: %s", r.status_code, attempt, url[:80])
        except Exception as exc:
            log.warning("GET attempt %d/%d failed [%s]: %s", attempt, attempts, url[:80], exc)
        if attempt < attempts:
            time.sleep(3)
    return None


def safe_post(session, url, attempts=3, **kwargs):
    for attempt in range(1, attempts + 1):
        try:
            r = session.post(url, timeout=30, **kwargs)
            if r.status_code < 500:
                return r
            log.warning("POST HTTP %d on attempt %d: %s", r.status_code, attempt, url[:80])
        except Exception as exc:
            log.warning("POST attempt %d/%d failed [%s]: %s", attempt, attempts, url[:80], exc)
        if attempt < attempts:
            time.sleep(3)
    return None


# ---------------------------------------------------------------------------
# Fidlar PublicSearch API client (used by 9 big TX counties)
# ---------------------------------------------------------------------------
class FidlarPublicSearchClient:
    """
    Client for the Fidlar Technologies PublicSearch platform used by most
    large Texas counties at https://{county}.tx.publicsearch.us

    The platform exposes a JSON REST API used by its React SPA frontend.
    We replicate the same API calls the browser makes.
    """

    # Fidlar doc type codes that match our TARGET_CATS
    FIDLAR_DOC_TYPES = [
        # Lis Pendens
        "LP",
        # Foreclosure / Trustee Sale
        "NOFC", "NTS", "STS",
        # Tax Deed
        "TAXDEED",
        # Judgments
        "JUD", "CCJ", "DRJUD", "ABJUDGMENT",
        # Tax / Federal Liens
        "LNCORPTX", "LNIRS", "LNFED", "TXLIEN", "FEDTAXLIEN", "FTL", "STLIEN",
        # General Liens
        "LN", "LNMECH", "MECHLIEN", "ML", "LNHOA",
        # Medicaid
        "MEDLN",
        # Probate
        "PRO", "LETTEST", "LETADMIN",
        # Notice of Commencement
        "NOC",
        # Release Lis Pendens
        "RELLP",
    ]

    def __init__(self, county_key: str, config: dict, start_date: datetime, end_date: datetime):
        self.county_key = county_key
        self.config = config
        self.start_date = start_date
        self.end_date = end_date
        self.api_base = config["api_base"]
        self.session = make_session(referer=config["portal"])

    def _prime_session(self):
        """Load the SPA homepage to pick up any session cookies."""
        r = safe_get(self.session, self.config["portal"])
        if r:
            log.info("[%s] Primed session (HTTP %d)", self.county_key, r.status_code)

    def _build_search_params(self, page: int = 1, page_size: int = 250) -> dict:
        """Build query params for the Fidlar instruments search endpoint."""
        params = {
            "DateRange.StartDate": self.start_date.strftime("%m/%d/%Y"),
            "DateRange.EndDate": self.end_date.strftime("%m/%d/%Y"),
            "Page": page,
            "PageSize": page_size,
        }
        # Append each doc type as a repeated key
        for dt in self.FIDLAR_DOC_TYPES:
            params.setdefault("DocTypes", [])
            if isinstance(params["DocTypes"], list):
                params["DocTypes"].append(dt)
            else:
                params["DocTypes"] = [params["DocTypes"], dt]
        return params

    def _parse_instrument(self, item: dict) -> Optional[dict]:
        """Map a Fidlar API instrument object to our common record schema."""
        try:
            # Document type
            doc_type_raw = ""
            if isinstance(item.get("documentType"), dict):
                doc_type_raw = (item["documentType"].get("code") or
                                item["documentType"].get("label") or "").upper()
            elif isinstance(item.get("documentType"), str):
                doc_type_raw = item["documentType"].upper()
            elif item.get("docType"):
                doc_type_raw = str(item["docType"]).upper()
            elif item.get("type"):
                doc_type_raw = str(item["type"]).upper()

            cat = DOC_TYPE_MAP.get(doc_type_raw, "")
            if not cat:
                # Try partial match
                for k, v in DOC_TYPE_MAP.items():
                    if k in doc_type_raw or doc_type_raw in k:
                        cat = v
                        break
            if not cat or cat not in TARGET_CATS:
                return None

            # Document number
            doc_num = (
                str(item.get("instrumentNumber") or "")
                or str(item.get("docNumber") or "")
                or str(item.get("id") or "")
            ).strip()

            # Filed date
            filed_raw = (
                item.get("recordedDate")
                or item.get("filedDate")
                or item.get("entryDate")
                or ""
            )
            filed = _normalise_date(str(filed_raw))

            # Grantor (owner)
            owner = self._extract_name(item, "grantor")

            # Grantee
            grantee = self._extract_name(item, "grantee")

            # Amount
            amount = None
            for field in ("consideration", "amount", "debt", "loanAmount"):
                val = item.get(field)
                if val is not None:
                    amount = _parse_amount(str(val))
                    if amount is not None:
                        break

            # Legal description
            legal = str(item.get("legalDescription") or item.get("legal") or "").strip()[:300]

            # Direct URL to document
            clerk_url = (
                item.get("directImageUrl")
                or item.get("imageUrl")
                or item.get("documentUrl")
                or f"{self.api_base}/results/{doc_num}"
            )

            return {
                "doc_num": doc_num,
                "doc_type": doc_type_raw,
                "filed": filed,
                "cat": cat,
                "cat_label": CAT_LABELS.get(cat, cat),
                "owner": owner,
                "grantee": grantee,
                "amount": amount,
                "legal": legal,
                "prop_address": "",
                "prop_city": self.config.get("seat", ""),
                "prop_state": "TX",
                "prop_zip": self.config.get("seat_zip", ""),
                "mail_address": "",
                "mail_city": "",
                "mail_state": "",
                "mail_zip": "",
                "clerk_url": clerk_url,
                "county": self.config["name"],
                "county_key": self.county_key,
                "flags": [],
                "score": 0,
            }
        except Exception as exc:
            log.debug("Instrument parse error: %s | item=%s", exc, str(item)[:200])
            return None

    def _extract_name(self, item: dict, role: str) -> str:
        """Extract grantor or grantee name from various Fidlar field layouts."""
        # Try list of party objects
        parties = item.get(role) or item.get(f"{role}s") or []
        if isinstance(parties, list) and parties:
            names = []
            for p in parties:
                if isinstance(p, dict):
                    name = (p.get("fullName") or p.get("name") or
                            " ".join(filter(None, [p.get("firstName"), p.get("lastName")])))
                    if name:
                        names.append(name.strip())
                elif isinstance(p, str):
                    names.append(p.strip())
            return "; ".join(names)
        elif isinstance(parties, str):
            return parties.strip()

        # Try flat string fields
        for key in (f"{role}Name", f"{role}1", role):
            val = item.get(key)
            if val and isinstance(val, str):
                return val.strip()
        return ""

    def _search_page(self, page: int) -> tuple[list[dict], int]:
        """
        Fetch one page of search results.
        Returns (records, total_count).
        Tries GET /api/instruments first, then POST variants.
        """
        params = self._build_search_params(page=page)

        # --- Attempt 1: GET /api/instruments ---
        try:
            # Build query string with repeated DocTypes
            qstring_parts = []
            for k, v in params.items():
                if k == "DocTypes" and isinstance(v, list):
                    for dt in v:
                        qstring_parts.append(f"DocTypes={quote(dt)}")
                else:
                    qstring_parts.append(f"{k}={quote(str(v))}")
            url = f"{self.api_base}/api/instruments?" + "&".join(qstring_parts)
            self.session.headers["Accept"] = "application/json, text/plain, */*"
            r = safe_get(self.session, url)
            if r and r.status_code == 200 and "json" in r.headers.get("content-type", ""):
                data = r.json()
                items = data.get("data") or data.get("results") or data.get("instruments") or []
                total = data.get("totalCount") or data.get("total") or len(items)
                log.info("[%s] GET /api/instruments p%d → %d items (total=%d)",
                         self.county_key, page, len(items), total)
                return [r for r in (self._parse_instrument(i) for i in items) if r], total
        except Exception as exc:
            log.debug("[%s] GET instruments failed: %s", self.county_key, exc)

        # --- Attempt 2: POST /api/instruments ---
        try:
            post_body = {
                "dateRange": {
                    "startDate": self.start_date.strftime("%m/%d/%Y"),
                    "endDate": self.end_date.strftime("%m/%d/%Y"),
                },
                "docTypes": self.FIDLAR_DOC_TYPES,
                "page": page,
                "pageSize": 250,
            }
            url = f"{self.api_base}/api/instruments"
            self.session.headers["Content-Type"] = "application/json"
            self.session.headers["Accept"] = "application/json"
            r = safe_post(self.session, url, json=post_body)
            if r and r.status_code == 200 and "json" in r.headers.get("content-type", ""):
                data = r.json()
                items = data.get("data") or data.get("results") or data.get("instruments") or []
                total = data.get("totalCount") or data.get("total") or len(items)
                log.info("[%s] POST /api/instruments p%d → %d items (total=%d)",
                         self.county_key, page, len(items), total)
                return [r for r in (self._parse_instrument(i) for i in items) if r], total
        except Exception as exc:
            log.debug("[%s] POST instruments failed: %s", self.county_key, exc)

        # --- Attempt 3: GET /results (HTML scrape fallback) ---
        try:
            qstring_parts = []
            for k, v in params.items():
                if k == "DocTypes" and isinstance(v, list):
                    for dt in v:
                        qstring_parts.append(f"DocTypes={quote(dt)}")
                else:
                    qstring_parts.append(f"{k}={quote(str(v))}")
            url = f"{self.api_base}/results?" + "&".join(qstring_parts)
            self.session.headers["Accept"] = "text/html"
            r = safe_get(self.session, url)
            if r and r.status_code == 200:
                records = _parse_html_results_table(r.text, self.api_base,
                                                    self.county_key, self.config)
                log.info("[%s] HTML fallback p%d → %d items", self.county_key, page, len(records))
                return records, len(records)
        except Exception as exc:
            log.debug("[%s] HTML fallback failed: %s", self.county_key, exc)

        return [], 0

    def scrape(self) -> list[dict]:
        """Scrape all pages and return matching records."""
        log.info("[%s] Starting Fidlar PublicSearch scrape | %s → %s",
                 self.county_key,
                 self.start_date.strftime("%Y-%m-%d"),
                 self.end_date.strftime("%Y-%m-%d"))

        self._prime_session()
        all_records: list[dict] = []

        page = 1
        while True:
            try:
                records, total = self._search_page(page)
                all_records.extend(records)
                if not records or len(all_records) >= total:
                    break
                page += 1
                time.sleep(1)  # be polite
            except Exception as exc:
                log.error("[%s] Page %d failed: %s", self.county_key, page, exc)
                break

        log.info("[%s] Done. %d matching records", self.county_key, len(all_records))
        return all_records


# ---------------------------------------------------------------------------
# Harris County scraper (cclerk.hctx.net — custom Fidlar iDOC portal)
# ---------------------------------------------------------------------------
class HarrisCountyScraper:
    """
    Harris County Clerk's portal: https://www.cclerk.hctx.net/applications/websearch/
    This is a Fidlar iDOC/Eagle system with its own REST API.
    The frontend is Angular JS and makes XHR calls to backend endpoints.
    """

    PORTAL = "https://www.cclerk.hctx.net/applications/websearch/"
    API_BASE = "https://www.cclerk.hctx.net"

    def __init__(self, start_date: datetime, end_date: datetime):
        self.start_date = start_date
        self.end_date = end_date
        self.session = make_session(referer=self.PORTAL)
        self.config = COUNTIES["harris"]

    def _prime_session(self):
        """Load the portal page to pick up session cookies."""
        r = safe_get(self.session, self.PORTAL)
        log.info("[harris] Primed session (HTTP %d)", r.status_code if r else 0)

    def _search(self, page: int = 1) -> tuple[list[dict], int]:
        """Attempt to search Harris County records."""

        # Harris County iDOC has a different API structure
        # Attempt known patterns
        start_str = self.start_date.strftime("%m/%d/%Y")
        end_str = self.end_date.strftime("%m/%d/%Y")

        # Pattern 1: /api/search endpoint with JSON POST
        endpoints_to_try = [
            (
                "POST", f"{self.API_BASE}/api/instruments",
                {
                    "dateRange": {"startDate": start_str, "endDate": end_str},
                    "docTypes": list(DOC_TYPE_MAP.keys())[:20],
                    "page": page, "pageSize": 200,
                }
            ),
            (
                "GET",
                (f"{self.API_BASE}/api/instruments?"
                 f"DateRange.StartDate={quote(start_str)}"
                 f"&DateRange.EndDate={quote(end_str)}"
                 f"&Page={page}&PageSize=200"),
                None
            ),
            (
                "GET",
                (f"{self.API_BASE}/applications/websearch/api/instruments?"
                 f"DateRange.StartDate={quote(start_str)}"
                 f"&DateRange.EndDate={quote(end_str)}"
                 f"&Page={page}&PageSize=200"),
                None
            ),
        ]

        self.session.headers["Accept"] = "application/json"
        self.session.headers["Content-Type"] = "application/json"

        for method, url, body in endpoints_to_try:
            try:
                if method == "POST":
                    r = safe_post(self.session, url, json=body)
                else:
                    r = safe_get(self.session, url)

                if not r or r.status_code not in (200, 201):
                    continue
                if "json" not in r.headers.get("content-type", ""):
                    continue

                data = r.json()
                items = data.get("data") or data.get("results") or []
                total = data.get("totalCount") or len(items)
                records = []
                for item in items:
                    rec = self._parse_instrument(item)
                    if rec:
                        records.append(rec)
                log.info("[harris] %s %s → %d items", method, url[:60], len(records))
                return records, total

            except Exception as exc:
                log.debug("[harris] %s %s failed: %s", method, url[:60], exc)

        # HTML fallback
        try:
            url = (f"{self.API_BASE}/applications/websearch/?startDate={quote(start_str)}"
                   f"&endDate={quote(end_str)}&page={page}")
            self.session.headers["Accept"] = "text/html"
            r = safe_get(self.session, url)
            if r and r.status_code == 200:
                records = _parse_html_results_table(r.text, self.API_BASE,
                                                    "harris", self.config)
                return records, len(records)
        except Exception as exc:
            log.debug("[harris] HTML fallback failed: %s", exc)

        return [], 0

    def _parse_instrument(self, item: dict) -> Optional[dict]:
        """Parse a Harris County API result item."""
        doc_type_raw = ""
        if isinstance(item.get("documentType"), dict):
            doc_type_raw = (item["documentType"].get("code") or
                            item["documentType"].get("label") or "").upper()
        else:
            doc_type_raw = str(item.get("documentType") or item.get("docType") or "").upper()

        cat = DOC_TYPE_MAP.get(doc_type_raw, "")
        if not cat or cat not in TARGET_CATS:
            return None

        parties = item.get("grantor") or item.get("grantors") or []
        if isinstance(parties, list):
            owner = "; ".join(
                p.get("fullName") or p.get("name") or "" for p in parties if isinstance(p, dict)
            )
        else:
            owner = str(parties)

        grantees = item.get("grantee") or item.get("grantees") or []
        if isinstance(grantees, list):
            grantee = "; ".join(
                g.get("fullName") or g.get("name") or "" for g in grantees if isinstance(g, dict)
            )
        else:
            grantee = str(grantees)

        return {
            "doc_num": str(item.get("instrumentNumber") or item.get("docNumber") or ""),
            "doc_type": doc_type_raw,
            "filed": _normalise_date(str(item.get("recordedDate") or item.get("filedDate") or "")),
            "cat": cat,
            "cat_label": CAT_LABELS.get(cat, cat),
            "owner": owner.strip(),
            "grantee": grantee.strip(),
            "amount": _parse_amount(str(item.get("consideration") or item.get("amount") or "")),
            "legal": str(item.get("legalDescription") or "").strip()[:300],
            "prop_address": "",
            "prop_city": "Houston",
            "prop_state": "TX",
            "prop_zip": "77002",
            "mail_address": "",
            "mail_city": "",
            "mail_state": "",
            "mail_zip": "",
            "clerk_url": (item.get("directImageUrl") or
                          f"{self.API_BASE}/applications/websearch/#/results/{item.get('instrumentNumber','')}"),
            "county": "Harris County",
            "county_key": "harris",
            "flags": [],
            "score": 0,
        }

    def scrape(self) -> list[dict]:
        log.info("[harris] Starting Harris County scrape")
        self._prime_session()
        all_records = []
        page = 1
        while True:
            records, total = self._search(page)
            all_records.extend(records)
            if not records or len(all_records) >= total:
                break
            page += 1
            time.sleep(1)
        log.info("[harris] Done. %d matching records", len(all_records))
        return all_records


# ---------------------------------------------------------------------------
# Tyler County foreclosure page scraper (HTML-based, verified working)
# ---------------------------------------------------------------------------
def scrape_tyler_foreclosure_page(session: requests.Session,
                                  start_date: datetime,
                                  end_date: datetime) -> list[dict]:
    """
    Scrapes https://www.co.tyler.tx.us/page/tyler.Forclosures
    Returns NOFC records from the county's foreclosure notice PDF listing.
    This is the only publicly accessible online motivated-seller data for Tyler County TX.
    """
    FORECLOSURE_PAGE = "https://www.co.tyler.tx.us/page/tyler.Forclosures"
    BASE_URL = "https://www.co.tyler.tx.us"
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    config = COUNTIES["tyler"]

    log.info("[tyler] Fetching foreclosure page: %s", FORECLOSURE_PAGE)
    r = safe_get(session, FORECLOSURE_PAGE)
    if not r or r.status_code != 200:
        log.error("[tyler] Could not fetch foreclosure page")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    records = []

    for a in soup.find_all("a", href=True):
        href = a["href"]

        if not re.search(r"\.pdf$", href, re.IGNORECASE):
            continue
        is_fc_upload = "/page/3326/" in href or "/upload/page/3326/" in href
        is_cira = "cira.state.tx.us" in href and "3326" in href
        if not (is_fc_upload or is_cira):
            continue

        owner_raw = a.get_text(strip=True)
        skip_keywords = ["application", "resolution", "guidelines", "budget", "regulation",
                         "permit", "ordinance", "subdivision", "abatement", "procedures",
                         "how property"]
        if any(kw in owner_raw.lower() for kw in skip_keywords):
            continue
        if not owner_raw or len(owner_raw) < 2:
            continue

        pdf_url = href if href.startswith("http") else urljoin(BASE_URL, href)
        owner = re.sub(r"\s+", " ", owner_raw).strip().upper()
        owner = re.sub(r",(\S)", r", \1", owner)

        fname = href.split("/")[-1]
        doc_num = re.sub(r"\.pdf$", "", fname, flags=re.I)[:60]

        records.append({
            "doc_num": doc_num,
            "doc_type": "NOFC",
            "filed": today_str,
            "cat": "NOFC",
            "cat_label": "Notice of Foreclosure",
            "owner": owner,
            "grantee": "",
            "amount": None,
            "legal": "",
            "prop_address": "",
            "prop_city": config["seat"],
            "prop_state": "TX",
            "prop_zip": config["seat_zip"],
            "mail_address": "",
            "mail_city": "",
            "mail_state": "",
            "mail_zip": "",
            "clerk_url": pdf_url,
            "county": config["name"],
            "county_key": "tyler",
            "flags": [],
            "score": 0,
        })

    log.info("[tyler] Scraped %d foreclosure notices", len(records))
    return records


# ---------------------------------------------------------------------------
# HTML table result parser (fallback for any county)
# ---------------------------------------------------------------------------
def _parse_html_results_table(html: str, base_url: str, county_key: str, config: dict) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    records = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        if not any(kw in " ".join(headers) for kw in ("type", "grantor", "doc", "date")):
            continue

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            cell_texts = [c.get_text(strip=True) for c in cells]
            cell_map = dict(zip(headers, cell_texts))

            doc_type_raw = (cell_map.get("type") or cell_map.get("doc type") or "").upper()
            cat = DOC_TYPE_MAP.get(doc_type_raw, "")
            if not cat:
                for k, v in DOC_TYPE_MAP.items():
                    if k in doc_type_raw:
                        cat = v
                        break
            if not cat or cat not in TARGET_CATS:
                continue

            link = None
            for cell in cells:
                a = cell.find("a", href=True)
                if a:
                    href = a["href"]
                    link = href if href.startswith("http") else base_url + href
                    break

            records.append({
                "doc_num": cell_map.get("instrument #") or cell_map.get("doc #") or "",
                "doc_type": doc_type_raw,
                "filed": _normalise_date(cell_map.get("date") or cell_map.get("filed") or ""),
                "cat": cat,
                "cat_label": CAT_LABELS.get(cat, cat),
                "owner": cell_map.get("grantor") or cell_map.get("owner") or "",
                "grantee": cell_map.get("grantee") or "",
                "amount": _parse_amount(cell_map.get("amount") or ""),
                "legal": cell_map.get("legal") or "",
                "prop_address": "",
                "prop_city": config.get("seat", ""),
                "prop_state": "TX",
                "prop_zip": config.get("seat_zip", ""),
                "mail_address": "",
                "mail_city": "",
                "mail_state": "",
                "mail_zip": "",
                "clerk_url": link or base_url,
                "county": config["name"],
                "county_key": county_key,
                "flags": [],
                "score": 0,
            })
    return records


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalise_date(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    # ISO format first (most Fidlar APIs return ISO)
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
    return raw[:10]


def _parse_amount(raw: str) -> Optional[float]:
    raw = (raw or "").strip()
    if not raw or raw in ("-", "N/A", "None", "null", "0"):
        return None
    raw = re.sub(r"[^0-9.]", "", raw)
    try:
        return float(raw) or None
    except ValueError:
        return None


def _split_name(owner: str) -> tuple[str, str]:
    if not owner:
        return "", ""
    if any(kw in owner for kw in ("LLC", "INC", "CORP", "LTD", "TRUST", "BANK", "FARMS")):
        return "", owner
    if "," in owner:
        parts = owner.split(",", 1)
        return parts[1].strip(), parts[0].strip()
    parts = owner.split()
    if len(parts) == 1:
        return "", parts[0]
    return parts[0], parts[-1]


# ---------------------------------------------------------------------------
# Seller score computation
# ---------------------------------------------------------------------------
def compute_score(rec: dict, all_records: list) -> tuple[int, list]:
    score = 30
    flags: list[str] = []

    cat = rec.get("cat", "")
    owner = (rec.get("owner") or "").upper()
    amount = rec.get("amount") or 0
    filed = rec.get("filed") or ""

    if cat == "LP":
        flags.append("Lis pendens")
        score += 10
    if cat == "NOFC":
        flags.append("Pre-foreclosure")
        score += 10
    if cat in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
        score += 10
    if cat in ("LNCORPTX", "LNIRS", "LNFED"):
        flags.append("Tax lien")
        score += 10
    if cat == "LNMECH":
        flags.append("Mechanic lien")
        score += 10
    if cat in ("LN", "LNHOA", "MEDLN"):
        flags.append("Judgment lien")
        score += 10
    if cat == "PRO":
        flags.append("Probate / estate")
        score += 10

    # LP + NOFC combo on same owner
    if cat in ("LP", "NOFC"):
        sister_cats = {"LP", "NOFC"} - {cat}
        county_key = rec.get("county_key")
        has_sister = any(
            r.get("owner", "").upper() == owner
            and r.get("cat") in sister_cats
            and r.get("county_key") == county_key
            for r in all_records if r is not rec
        )
        if has_sister:
            score += 20

    try:
        amt = float(amount)
        if amt > 100_000:
            score += 15
        elif amt > 50_000:
            score += 10
    except (TypeError, ValueError):
        pass

    try:
        filed_dt = datetime.strptime(filed[:10], "%Y-%m-%d")
        if (datetime.utcnow() - filed_dt).days <= 7:
            flags.append("New this week")
            score += 5
    except (ValueError, TypeError):
        pass

    if rec.get("prop_address") or rec.get("mail_address"):
        score += 5

    if any(kw in owner for kw in ("LLC", "INC", "CORP", "LTD", "LP ", "TRUST", "FARMS")):
        flags.append("LLC / corp owner")
        score += 10

    flags = list(dict.fromkeys(flags))
    return min(score, 100), flags


# ---------------------------------------------------------------------------
# GHL CSV export
# ---------------------------------------------------------------------------
def export_ghl_csv(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "County", "Source", "Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            first, last = _split_name(rec.get("owner") or "")
            writer.writerow({
                "First Name": first,
                "Last Name": last,
                "Mailing Address": rec.get("mail_address") or "",
                "Mailing City": rec.get("mail_city") or "",
                "Mailing State": rec.get("mail_state") or "",
                "Mailing Zip": rec.get("mail_zip") or "",
                "Property Address": rec.get("prop_address") or "",
                "Property City": rec.get("prop_city") or "",
                "Property State": rec.get("prop_state") or "TX",
                "Property Zip": rec.get("prop_zip") or "",
                "Lead Type": rec.get("cat_label") or "",
                "Document Type": rec.get("doc_type") or "",
                "Date Filed": rec.get("filed") or "",
                "Document Number": rec.get("doc_num") or "",
                "Amount/Debt Owed": rec.get("amount") or "",
                "Seller Score": rec.get("score") or "",
                "Motivated Seller Flags": "; ".join(rec.get("flags") or []),
                "County": rec.get("county") or "",
                "Source": SOURCE,
                "Public Records URL": rec.get("clerk_url") or "",
            })
    log.info("GHL CSV exported: %s (%d rows)", path, len(records))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    log.info(
        "Texas multi-county scraper | counties=%s | %s → %s",
        ",".join(ENABLED_COUNTIES),
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )

    all_records: list[dict] = []
    county_results: dict = {}

    # --- Tyler County (verified working HTML scraper) ---
    if "tyler" in ENABLED_COUNTIES:
        try:
            session = make_session()
            recs = scrape_tyler_foreclosure_page(session, start_date, end_date)
            all_records.extend(recs)
            county_results["tyler"] = recs
        except Exception as exc:
            log.error("[tyler] Scrape failed: %s\n%s", exc, traceback.format_exc())
            county_results["tyler"] = []

    # --- Harris County (custom Fidlar iDOC) ---
    if "harris" in ENABLED_COUNTIES:
        try:
            scraper = HarrisCountyScraper(start_date, end_date)
            recs = scraper.scrape()
            all_records.extend(recs)
            county_results["harris"] = recs
        except Exception as exc:
            log.error("[harris] Scrape failed: %s", exc)
            county_results["harris"] = []

    # --- All Fidlar PublicSearch counties ---
    fidlar_counties = [k for k in ENABLED_COUNTIES
                       if k not in ("tyler", "harris") and k in COUNTIES
                       and COUNTIES[k].get("type") == "fidlar_publicsearch"]

    for county_key in fidlar_counties:
        config = COUNTIES[county_key]
        try:
            client = FidlarPublicSearchClient(county_key, config, start_date, end_date)
            recs = client.scrape()
            all_records.extend(recs)
            county_results[county_key] = recs
        except Exception as exc:
            log.error("[%s] Scrape failed: %s", county_key, exc)
            county_results[county_key] = []

    # --- Deduplicate across all counties (doc_num + county_key) ---
    seen: set = set()
    deduped: list[dict] = []
    for rec in all_records:
        key = f"{rec.get('county_key')}:{rec.get('doc_num') or rec.get('clerk_url') or rec.get('owner')}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rec)
    all_records = deduped
    log.info("After dedup: %d records across %d counties", len(all_records),
             len([k for k, v in county_results.items() if v]))

    # --- Compute scores ---
    for rec in all_records:
        score, flags = compute_score(rec, all_records)
        rec["score"] = score
        rec["flags"] = flags

    # --- Sort by score ---
    all_records.sort(key=lambda r: r.get("score", 0), reverse=True)

    # --- Build payload ---
    with_address = sum(1 for r in all_records if r.get("prop_address") or r.get("mail_address"))
    county_summary = {
        k: len(v) for k, v in county_results.items()
    }

    payload = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": SOURCE,
        "date_range": {
            "start": start_date.strftime("%Y-%m-%d"),
            "end": end_date.strftime("%Y-%m-%d"),
        },
        "total": len(all_records),
        "with_address": with_address,
        "county_summary": county_summary,
        "records": all_records,
    }

    # --- Save combined output ---
    for out_path in OUTPUT_PATHS:
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
            log.info("Saved %d records → %s", len(all_records), out_path)
        except Exception as exc:
            log.error("Failed to write %s: %s", out_path, exc)

    # --- Save per-county JSON files ---
    for county_key, recs in county_results.items():
        try:
            county_payload = {
                **payload,
                "county": COUNTIES[county_key]["name"],
                "total": len(recs),
                "records": [r for r in all_records if r.get("county_key") == county_key],
            }
            out = Path(f"data/{county_key}_records.json")
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(county_payload, indent=2, default=str), encoding="utf-8")
        except Exception as exc:
            log.error("Failed to write county file for %s: %s", county_key, exc)

    # --- GHL CSV export ---
    try:
        export_ghl_csv(all_records, Path("data/ghl_export.csv"))
    except Exception as exc:
        log.error("GHL CSV export failed: %s", exc)

    log.info(
        "Done. Total=%d, WithAddress=%d, MaxScore=%s | County breakdown: %s",
        len(all_records),
        with_address,
        all_records[0]["score"] if all_records else "N/A",
        county_summary,
    )

    return len(all_records)


if __name__ == "__main__":
    count = main()
    sys.exit(0 if count >= 0 else 1)
