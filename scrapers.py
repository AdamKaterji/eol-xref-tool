"""
scrapers.py — no pandas, no lxml. Pure Python + openpyxl + requests.
"""
import re
import logging
from io import BytesIO
from datetime import date, datetime
from typing import Optional

import requests
import openpyxl
from bs4 import BeautifulSoup

from models import EOLPart
from session import get_session, polite_delay

log = logging.getLogger(__name__)


def _parse_date(raw) -> Optional[date]:
    if not raw or str(raw).strip() in ("", "nan", "None"):
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y", "%B %d, %Y", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).date()
        except ValueError:
            continue
    return None


def _soup(html):
    return BeautifulSoup(html, "html.parser")


def _emit(fn, supplier, message):
    log.info(f"[{supplier}] {message}")
    if fn:
        fn(supplier, message)


def _filter(parts, since):
    if not since:
        return parts
    return [p for p in parts if (p.eol_date or p.last_buy_date) is None
            or (p.eol_date or p.last_buy_date) >= since]


def _read_xlsx(content: bytes) -> list[dict]:
    """
    Read an xlsx file from bytes using openpyxl.
    Returns list of dicts with lowercased/underscored column names.
    """
    wb = openpyxl.load_workbook(BytesIO(content), read_only=True, data_only=True)
    all_rows = []
    for ws in wb.worksheets:
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue
        # Find header row (first row with enough non-None values)
        header_row = None
        data_start = 0
        for i, row in enumerate(rows[:10]):
            non_empty = [c for c in row if c is not None]
            if len(non_empty) >= 2:
                header_row = row
                data_start = i + 1
                break
        if not header_row:
            continue
        headers = [str(c).strip().lower().replace(" ", "_") if c else f"col_{i}"
                   for i, c in enumerate(header_row)]
        for row in rows[data_start:]:
            if all(c is None for c in row):
                continue
            all_rows.append({headers[i]: (str(v).strip() if v is not None else "")
                             for i, v in enumerate(row) if i < len(headers)})
    wb.close()
    return all_rows


def _v(d, keys):
    for k in keys:
        v = d.get(k, "")
        if v and str(v).strip() not in ("", "nan", "None"):
            return str(v).strip()
    return ""


# ── Texas Instruments ─────────────────────────────────────────────────────────

def fetch_ti(session, since=None, emit=None):
    _emit(emit, "Texas Instruments", "Downloading PDCA list from ti.com...")
    url = "https://www.ti.com/pdca/download.xls"
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        rows = _read_xlsx(r.content)
        parts = []
        for row in rows:
            pn = _v(row, ["part_number", "device", "ti_part_number", "mpn"])
            if not pn:
                continue
            parts.append(EOLPart(
                part_number=pn.upper(), supplier="Texas Instruments",
                description=_v(row, ["description", "product_name"]),
                eol_date=_parse_date(_v(row, ["discontinuance_date", "eol_date", "disc_date"])),
                last_buy_date=_parse_date(_v(row, ["last_time_buy_date", "ltb_date"])),
                replacement_pn=_v(row, ["suggested_replacement", "replacement"]),
                product_family=_v(row, ["product_family", "family"]),
                package=_v(row, ["package", "pkg"]),
                source_url=url, source_doc="ti_pdca_download.xls",
            ))
        parts = _filter(parts, since)
        _emit(emit, "Texas Instruments", f"Found {len(parts)} EOL parts")
        return parts
    except Exception as e:
        _emit(emit, "Texas Instruments", f"Failed: {e}")
        return []


# ── STMicroelectronics ────────────────────────────────────────────────────────

def fetch_stm(session, since=None, emit=None):
    _emit(emit, "STMicroelectronics", "Querying ST PDN API...")
    try:
        r = session.get(
            "https://www.st.com/bin/st/getPDN.json",
            params={"start": 0, "rows": 5000, "type": "PDN"},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        entries = data.get("response", {}).get("docs", []) or data.get("docs", []) or []
        parts = []
        for e in entries:
            pn = e.get("partNumber") or e.get("part_number") or ""
            if not pn:
                continue
            parts.append(EOLPart(
                part_number=str(pn).upper(), supplier="STMicroelectronics",
                description=e.get("description") or e.get("productName") or "",
                eol_date=_parse_date(e.get("pdnDate") or e.get("eolDate") or ""),
                last_buy_date=_parse_date(e.get("lastTimeBuyDate") or ""),
                replacement_pn=e.get("replacement") or "",
                source_url="https://www.st.com/content/st_com/en/support/resources/pdn.html",
            ))
        parts = _filter(parts, since)
        _emit(emit, "STMicroelectronics", f"Found {len(parts)} EOL parts")
        return parts
    except Exception as e:
        _emit(emit, "STMicroelectronics", f"Failed: {e}")
        return []


# ── Analog Devices ────────────────────────────────────────────────────────────

def fetch_adi(session, since=None, emit=None):
    _emit(emit, "Analog Devices", "Downloading ADI obsolete parts list...")
    url = "https://www.analog.com/media/en/manufacturing-supply-chain/obsolescence/ADI_Obsolete_Parts.xlsx"
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        rows = _read_xlsx(r.content)
        parts = []
        for row in rows:
            pn = _v(row, ["part_number", "model_number", "mpn", "device"])
            if not pn:
                continue
            parts.append(EOLPart(
                part_number=pn.upper(), supplier="Analog Devices",
                description=_v(row, ["description", "product_description"]),
                eol_date=_parse_date(_v(row, ["eol_date", "discontinuance_date", "obsolete_date"])),
                last_buy_date=_parse_date(_v(row, ["last_time_buy_date", "ltb_date"])),
                replacement_pn=_v(row, ["suggested_replacement", "replacement_part"]),
                package=_v(row, ["package", "pkg"]),
                source_url=url, source_doc="ADI_Obsolete_Parts.xlsx",
            ))
        parts = _filter(parts, since)
        _emit(emit, "Analog Devices", f"Found {len(parts)} EOL parts")
        return parts
    except Exception as e:
        _emit(emit, "Analog Devices", f"Failed: {e}")
        return []


# ── NXP ───────────────────────────────────────────────────────────────────────

def fetch_nxp(session, since=None, emit=None):
    _emit(emit, "NXP", "Downloading NXP Last-Time-Buy list...")
    url = "https://www.nxp.com/docs/en/supporting-information/NXP_PCN_Last_Time_Buy.xlsx"
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        rows = _read_xlsx(r.content)
        parts = []
        for row in rows:
            pn = _v(row, ["part_number", "device", "mpn", "nxp_part_number"])
            if not pn:
                continue
            parts.append(EOLPart(
                part_number=pn.upper(), supplier="NXP",
                description=_v(row, ["description", "product_name"]),
                eol_date=_parse_date(_v(row, ["eol_date", "discontinuance_date"])),
                last_buy_date=_parse_date(_v(row, ["last_time_buy_date", "ltb_date"])),
                replacement_pn=_v(row, ["suggested_replacement", "replacement_part"]),
                package=_v(row, ["package", "pkg"]),
                source_url=url, source_doc="NXP_PCN_Last_Time_Buy.xlsx",
            ))
        parts = _filter(parts, since)
        _emit(emit, "NXP", f"Found {len(parts)} EOL parts")
        return parts
    except Exception as e:
        _emit(emit, "NXP", f"Failed: {e}")
        return []


# ── Infineon ──────────────────────────────────────────────────────────────────

def fetch_infineon(session, since=None, emit=None):
    _emit(emit, "Infineon", "Checking Infineon PCN portal...")
    page_url = "https://www.infineon.com/cms/en/product/promopages/product-change-notification/"
    parts = []
    try:
        r = session.get(page_url, timeout=20)
        r.raise_for_status()
        soup = _soup(r.text)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(ext in href.lower() for ext in [".xlsx", ".xls"]) and \
               any(kw in href.lower() for kw in ["eol", "obsolete", "discontinu", "pdn", "ltb"]):
                url = href if href.startswith("http") else "https://www.infineon.com" + href
                polite_delay(1, 2)
                try:
                    r2 = session.get(url, timeout=30)
                    r2.raise_for_status()
                    rows = _read_xlsx(r2.content)
                    for row in rows:
                        pn = _v(row, ["part_number", "device", "mpn", "ordering_code"])
                        if pn:
                            parts.append(EOLPart(
                                part_number=pn.upper(), supplier="Infineon",
                                description=_v(row, ["description", "product_name"]),
                                eol_date=_parse_date(_v(row, ["eol_date", "discontinuance_date"])),
                                last_buy_date=_parse_date(_v(row, ["last_time_buy_date", "ltb_date"])),
                                replacement_pn=_v(row, ["replacement", "suggested_replacement"]),
                                source_url=url,
                            ))
                except Exception:
                    pass
    except Exception as e:
        _emit(emit, "Infineon", f"Failed: {e}")
    parts = _filter(parts, since)
    _emit(emit, "Infineon", f"Found {len(parts)} EOL parts")
    return parts


# ── Nexperia ──────────────────────────────────────────────────────────────────

def fetch_nexperia(session, since=None, emit=None):
    _emit(emit, "Nexperia", "Checking Nexperia PCN page...")
    page_url = "https://www.nexperia.com/support/quality-and-sustainability/product-change-notices.html"
    parts = []
    try:
        r = session.get(page_url, timeout=20)
        r.raise_for_status()
        soup = _soup(r.text)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(ext in href.lower() for ext in [".xlsx", ".xls"]):
                url = href if href.startswith("http") else "https://www.nexperia.com" + href
                polite_delay(1, 2)
                try:
                    r2 = session.get(url, timeout=30)
                    r2.raise_for_status()
                    rows = _read_xlsx(r2.content)
                    for row in rows:
                        pn = _v(row, ["part_number", "type_number", "device", "mpn"])
                        if not pn:
                            continue
                        notice = _v(row, ["pcn_type", "type", "change_type"]).lower()
                        if notice and not any(kw in notice for kw in
                                              ["discontinu", "eol", "ltb", "obsolete"]):
                            continue
                        parts.append(EOLPart(
                            part_number=pn.upper(), supplier="Nexperia",
                            description=_v(row, ["description", "product_name"]),
                            eol_date=_parse_date(_v(row, ["eol_date", "notification_date"])),
                            last_buy_date=_parse_date(_v(row, ["last_time_buy_date", "ltb_date"])),
                            replacement_pn=_v(row, ["replacement", "alternate"]),
                            source_url=url,
                        ))
                except Exception:
                    pass
    except Exception as e:
        _emit(emit, "Nexperia", f"Failed: {e}")
    parts = _filter(parts, since)
    _emit(emit, "Nexperia", f"Found {len(parts)} EOL parts")
    return parts


# ── Power Integrations ────────────────────────────────────────────────────────

def fetch_pi(session, since=None, emit=None):
    _emit(emit, "Power Integrations", "Scraping Power Integrations PCN page...")
    url = "https://www.power.com/resources/pcn-product-change-notices"
    parts = []
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
        soup = _soup(r.text)
        for row in soup.find_all(["tr", "li"]):
            text = row.get_text(" ", strip=True)
            if not any(kw in text.lower() for kw in
                       ["discontinu", "eol", "last time buy", "ltb", "obsolete"]):
                continue
            for pn in re.findall(r'\b[A-Z]{2,5}\d{3}[A-Z0-9]*\b', text):
                parts.append(EOLPart(part_number=pn, supplier="Power Integrations",
                                     description=text[:120], source_url=url))
        seen = set()
        parts = [p for p in parts if not (p.part_number in seen or seen.add(p.part_number))]
    except Exception as e:
        _emit(emit, "Power Integrations", f"Failed: {e}")
    parts = _filter(parts, since)
    _emit(emit, "Power Integrations", f"Found {len(parts)} EOL parts")
    return parts


# ── Dispatcher ────────────────────────────────────────────────────────────────

SUPPLIER_MAP = {
    "Texas Instruments":  fetch_ti,
    "STMicroelectronics": fetch_stm,
    "Analog Devices":     fetch_adi,
    "NXP":                fetch_nxp,
    "Infineon":           fetch_infineon,
    "Nexperia":           fetch_nexperia,
    "Power Integrations": fetch_pi,
}


def scrape_suppliers(suppliers: list, since: date, emit=None) -> list:
    session = get_session()
    all_parts = []
    for name in suppliers:
        fn = SUPPLIER_MAP.get(name)
        if not fn:
            _emit(emit, name, "No scraper available — skipping")
            continue
        polite_delay(1, 2)
        try:
            parts = fn(session=session, since=since, emit=emit)
            all_parts.extend(parts)
        except Exception as e:
            _emit(emit, name, f"Error: {e}")
    seen = set()
    unique = []
    for p in all_parts:
        key = f"{p.supplier}::{p.part_number}"
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique
