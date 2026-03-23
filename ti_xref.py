import logging
from typing import Optional
import requests
from bs4 import BeautifulSoup
from models import EOLPart, TIAlternative, CrossRefResult
from session import polite_delay

log = logging.getLogger(__name__)

TI_XREF_URL   = "https://www.ti.com/cross-reference/search/en/p/cross-reference"
TI_PARAMS_BASE = "https://www.ti.com/product/"

MATCH_LABELS = {
    "drop-in":       "Drop-in Replacement",
    "drop in":       "Drop-in Replacement",
    "pin-to-pin":    "Pin-to-Pin",
    "pin to pin":    "Pin-to-Pin",
    "similar":       "Similar",
    "same function": "Same Functionality",
    "functional":    "Same Functionality",
}


def _normalise_match(raw: str) -> str:
    if not raw:
        return ""
    lower = raw.lower().strip()
    for key, label in MATCH_LABELS.items():
        if key in lower:
            return label
    return raw.strip().title()


def _soup(html):
    return BeautifulSoup(html, "html.parser")


def cross_reference(eol_part: EOLPart, session: requests.Session, max_results=3) -> CrossRefResult:
    pn = eol_part.part_number
    alternatives = []

    try:
        alternatives = _call_api(pn, session, max_results)
    except Exception as e:
        log.debug(f"TI xref API failed for {pn}: {e}")

    if not alternatives:
        try:
            alternatives = _call_html(pn, session, max_results)
        except Exception as e:
            log.debug(f"TI xref HTML failed for {pn}: {e}")

    for alt in alternatives:
        try:
            polite_delay(1.0, 2.5)
            alt.params = _fetch_params(alt.ti_part_number, session)
        except Exception:
            pass

    return CrossRefResult(eol_part=eol_part, ti_alternatives=alternatives)


def _call_api(pn, session, max_results):
    headers = {**session.headers,
               "Accept": "application/json, */*",
               "X-Requested-With": "XMLHttpRequest",
               "Referer": TI_XREF_URL}
    r = session.get(TI_XREF_URL,
                    params={"competitorPart": pn, "rows": max_results, "start": 0},
                    headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    results = (data.get("results") or data.get("products") or
               data.get("response", {}).get("docs", []) or [])
    alts = []
    for item in results[:max_results]:
        ti_pn = (item.get("tiPn") or item.get("partNumber") or item.get("mpn") or "").strip()
        if not ti_pn:
            continue
        alts.append(TIAlternative(
            ti_part_number=ti_pn.upper(),
            ti_description=item.get("description") or item.get("productName") or "",
            match_type=_normalise_match(item.get("match") or item.get("matchType") or ""),
            ti_product_url=f"{TI_PARAMS_BASE}{ti_pn.upper()}",
            lifecycle_status=item.get("lifecycleStatus") or "Active",
        ))
    return alts


def _call_html(pn, session, max_results):
    r = session.get(TI_XREF_URL, params={"competitorPart": pn}, timeout=20)
    r.raise_for_status()
    soup = _soup(r.text)
    alts = []
    table = soup.find("table")
    if not table:
        return []
    headers = [th.get_text(strip=True).lower().replace(" ", "_")
               for th in table.find_all("th")]
    for tr in table.find_all("tr")[1:max_results + 1]:
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 2:
            continue
        row = dict(zip(headers, cells))
        ti_pn = _v(row, ["ti_part_number", "ti_pn", "part_number", "device"])
        if not ti_pn:
            continue
        alts.append(TIAlternative(
            ti_part_number=ti_pn.upper(),
            ti_description=_v(row, ["description", "product_name"]),
            match_type=_normalise_match(_v(row, ["match", "match_type", "compatibility"])),
            ti_product_url=f"{TI_PARAMS_BASE}{ti_pn.upper()}",
        ))
    return alts


def _fetch_params(ti_pn, session):
    import json, re
    url = f"{TI_PARAMS_BASE}{ti_pn.upper()}"
    r = session.get(url, timeout=20)
    r.raise_for_status()
    soup = _soup(r.text)
    params = {}
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) >= 2:
                k = cells[0].get_text(strip=True)
                v = cells[1].get_text(strip=True)
                if k and v:
                    params[k] = v
    m = re.search(r'window\.__STATE__\s*=\s*(\{.+?\});', r.text, re.DOTALL)
    if m:
        try:
            state = json.loads(m.group(1))
            specs = (state.get("productDetails", {}).get("specs") or
                     state.get("product", {}).get("parameters") or {})
            if isinstance(specs, list):
                for item in specs:
                    if isinstance(item, dict):
                        params[item.get("name", "")] = str(item.get("value", ""))
        except Exception:
            pass
    return params


def _v(d, keys):
    for k in keys:
        v = d.get(k, "")
        if v and str(v).strip() not in ("", "nan", "None"):
            return str(v).strip()
    return ""


def run_crossref_batch(parts, session, emit=None):
    results = []
    for i, part in enumerate(parts, 1):
        if emit:
            emit("TI Cross-Reference",
                 f"({i}/{len(parts)}) {part.part_number} ({part.supplier})")
        polite_delay(2.0, 4.5)
        results.append(cross_reference(part, session))
    return results
