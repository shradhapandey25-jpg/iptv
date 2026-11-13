#!/usr/bin/env python3
"""Stream health checker for IPTV Dekh.

Fetches the iptv-org index playlist, probes every stream, and writes
recommended.json — a curated list of channels whose streams were verified
playable (manifest fetch + ffprobe). The site renders these in a
"Recommended — verified working" row.

Tier 1 (all streams): HTTP GET of the manifest URL. "working" means
HTTP 200 and the body parses as a playlist (#EXTM3U for HLS, <MPD for
DASH). 403/451/429 or an empty 200 body is "uncertain" (possibly
geo-blocked — NOT dead). Anything else is "dead".

Tier 2 (candidates only): ffprobe must find at least one video or audio
stream. A manifest that passes Tier 1 but fails ffprobe never lands in
Recommended.

The same run also refreshes featured-live.json — the Featured Live hero
and row (running Movies + Music channels, HTTPS + logo required, so the
showcase never shows a dead stream).

IMPORTANT: a failed check never removes or hides a channel from the site.
It only decides Recommended membership — streams flap, and geo-blocking
varies by viewer location.

Usage:
    python3 tools/check-streams.py [--out recommended.json]
    python3 tools/check-streams.py --limit 200          # smoke test
    python3 tools/check-streams.py --tier1-in tier1.json # reuse Tier 1
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

DEFAULT_PLAYLIST = "https://iptv-org.github.io/iptv/index.m3u"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")
NSFW = re.compile(r"\b(xxx|porn|adult|sex|erotic|nsfw|playboy|brazzers)\b", re.I)

COUNTRY_NAMES = {
    "AD": "Andorra",
    "AE": "United Arab Emirates",
    "AF": "Afghanistan",
    "AG": "Antigua and Barbuda",
    "AI": "Anguilla",
    "AL": "Albania",
    "AM": "Armenia",
    "AO": "Angola",
    "AQ": "Antarctica",
    "AR": "Argentina",
    "AS": "American Samoa",
    "AT": "Austria",
    "AU": "Australia",
    "AW": "Aruba",
    "AX": "Åland Islands",
    "AZ": "Azerbaijan",
    "BA": "Bosnia and Herzegovina",
    "BB": "Barbados",
    "BD": "Bangladesh",
    "BE": "Belgium",
    "BF": "Burkina Faso",
    "BG": "Bulgaria",
    "BH": "Bahrain",
    "BI": "Burundi",
    "BJ": "Benin",
    "BL": "Saint Barthélemy",
    "BM": "Bermuda",
    "BN": "Brunei",
    "BO": "Bolivia",
    "BQ": "Caribbean Netherlands",
    "BR": "Brazil",
    "BS": "Bahamas",
    "BT": "Bhutan",
    "BV": "Bouvet Island",
    "BW": "Botswana",
    "BY": "Belarus",
    "BZ": "Belize",
    "CA": "Canada",
    "CC": "Cocos (Keeling) Islands",
    "CD": "DR Congo",
    "CF": "Central African Republic",
    "CG": "Republic of the Congo",
    "CH": "Switzerland",
    "CI": "Côte d'Ivoire (Ivory Coast)",
    "CK": "Cook Islands",
    "CL": "Chile",
    "CM": "Cameroon",
    "CN": "China",
    "CO": "Colombia",
    "CR": "Costa Rica",
    "CU": "Cuba",
    "CV": "Cape Verde",
    "CW": "Curaçao",
    "CX": "Christmas Island",
    "CY": "Cyprus",
    "CZ": "Czechia",
    "DE": "Germany",
    "DJ": "Djibouti",
    "DK": "Denmark",
    "DM": "Dominica",
    "DO": "Dominican Republic",
    "DZ": "Algeria",
    "EC": "Ecuador",
    "EE": "Estonia",
    "EG": "Egypt",
    "EH": "Western Sahara",
    "ER": "Eritrea",
    "ES": "Spain",
    "ET": "Ethiopia",
    "EU": "European Union",
    "FI": "Finland",
    "FJ": "Fiji",
    "FK": "Falkland Islands",
    "FM": "Micronesia",
    "FO": "Faroe Islands",
    "FR": "France",
    "GA": "Gabon",
    "GB": "United Kingdom",
    "GB-ENG": "England",
    "GB-NIR": "Northern Ireland",
    "GB-SCT": "Scotland",
    "GB-WLS": "Wales",
    "GD": "Grenada",
    "GE": "Georgia",
    "GF": "French Guiana",
    "GG": "Guernsey",
    "GH": "Ghana",
    "GI": "Gibraltar",
    "GL": "Greenland",
    "GM": "Gambia",
    "GN": "Guinea",
    "GP": "Guadeloupe",
    "GQ": "Equatorial Guinea",
    "GR": "Greece",
    "GS": "South Georgia",
    "GT": "Guatemala",
    "GU": "Guam",
    "GW": "Guinea-Bissau",
    "GY": "Guyana",
    "HK": "Hong Kong",
    "HM": "Heard Island and McDonald Islands",
    "HN": "Honduras",
    "HR": "Croatia",
    "HT": "Haiti",
    "HU": "Hungary",
    "ID": "Indonesia",
    "IE": "Ireland",
    "IL": "Israel",
    "IM": "Isle of Man",
    "IN": "India",
    "IO": "British Indian Ocean Territory",
    "IQ": "Iraq",
    "IR": "Iran",
    "IS": "Iceland",
    "IT": "Italy",
    "JE": "Jersey",
    "JM": "Jamaica",
    "JO": "Jordan",
    "JP": "Japan",
    "KE": "Kenya",
    "KG": "Kyrgyzstan",
    "KH": "Cambodia",
    "KI": "Kiribati",
    "KM": "Comoros",
    "KN": "Saint Kitts and Nevis",
    "KP": "North Korea",
    "KR": "South Korea",
    "KW": "Kuwait",
    "KY": "Cayman Islands",
    "KZ": "Kazakhstan",
    "LA": "Laos",
    "LB": "Lebanon",
    "LC": "Saint Lucia",
    "LI": "Liechtenstein",
    "LK": "Sri Lanka",
    "LR": "Liberia",
    "LS": "Lesotho",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "LV": "Latvia",
    "LY": "Libya",
    "MA": "Morocco",
    "MC": "Monaco",
    "MD": "Moldova",
    "ME": "Montenegro",
    "MF": "Saint Martin",
    "MG": "Madagascar",
    "MH": "Marshall Islands",
    "MK": "North Macedonia",
    "ML": "Mali",
    "MM": "Myanmar",
    "MN": "Mongolia",
    "MO": "Macau",
    "MP": "Northern Mariana Islands",
    "MQ": "Martinique",
    "MR": "Mauritania",
    "MS": "Montserrat",
    "MT": "Malta",
    "MU": "Mauritius",
    "MV": "Maldives",
    "MW": "Malawi",
    "MX": "Mexico",
    "MY": "Malaysia",
    "MZ": "Mozambique",
    "NA": "Namibia",
    "NC": "New Caledonia",
    "NE": "Niger",
    "NF": "Norfolk Island",
    "NG": "Nigeria",
    "NI": "Nicaragua",
    "NL": "Netherlands",
    "NO": "Norway",
    "NP": "Nepal",
    "NR": "Nauru",
    "NU": "Niue",
    "NZ": "New Zealand",
    "OM": "Oman",
    "PA": "Panama",
    "PE": "Peru",
    "PF": "French Polynesia",
    "PG": "Papua New Guinea",
    "PH": "Philippines",
    "PK": "Pakistan",
    "PL": "Poland",
    "PM": "Saint Pierre and Miquelon",
    "PN": "Pitcairn Islands",
    "PR": "Puerto Rico",
    "PS": "Palestine",
    "PT": "Portugal",
    "PW": "Palau",
    "PY": "Paraguay",
    "QA": "Qatar",
    "RE": "Réunion",
    "RO": "Romania",
    "RS": "Serbia",
    "RU": "Russia",
    "RW": "Rwanda",
    "SA": "Saudi Arabia",
    "SB": "Solomon Islands",
    "SC": "Seychelles",
    "SD": "Sudan",
    "SE": "Sweden",
    "SG": "Singapore",
    "SH": "Saint Helena, Ascension and Tristan da Cunha",
    "SI": "Slovenia",
    "SJ": "Svalbard and Jan Mayen",
    "SK": "Slovakia",
    "SL": "Sierra Leone",
    "SM": "San Marino",
    "SN": "Senegal",
    "SO": "Somalia",
    "SR": "Suriname",
    "SS": "South Sudan",
    "ST": "São Tomé and Príncipe",
    "SV": "El Salvador",
    "SX": "Sint Maarten",
    "SY": "Syria",
    "SZ": "Eswatini (Swaziland)",
    "TC": "Turks and Caicos Islands",
    "TD": "Chad",
    "TF": "French Southern and Antarctic Lands",
    "TG": "Togo",
    "TH": "Thailand",
    "TJ": "Tajikistan",
    "TK": "Tokelau",
    "TL": "Timor-Leste",
    "TM": "Turkmenistan",
    "TN": "Tunisia",
    "TO": "Tonga",
    "TR": "Turkey",
    "TT": "Trinidad and Tobago",
    "TV": "Tuvalu",
    "TW": "Taiwan",
    "TZ": "Tanzania",
    "UA": "Ukraine",
    "UG": "Uganda",
    "UM": "United States Minor Outlying Islands",
    "UN": "United Nations",
    "US": "United States",
    "US-AK": "Alaska",
    "US-AL": "Alabama",
    "US-AR": "Arkansas",
    "US-AZ": "Arizona",
    "US-CA": "California",
    "US-CO": "Colorado",
    "US-CT": "Connecticut",
    "US-DE": "Delaware",
    "US-FL": "Florida",
    "US-GA": "Georgia",
    "US-HI": "Hawaii",
    "US-IA": "Iowa",
    "US-ID": "Idaho",
    "US-IL": "Illinois",
    "US-IN": "Indiana",
    "US-KS": "Kansas",
    "US-KY": "Kentucky",
    "US-LA": "Louisiana",
    "US-MA": "Massachusetts",
    "US-MD": "Maryland",
    "US-ME": "Maine",
    "US-MI": "Michigan",
    "US-MN": "Minnesota",
    "US-MO": "Missouri",
    "US-MS": "Mississippi",
    "US-MT": "Montana",
    "US-NC": "North Carolina",
    "US-ND": "North Dakota",
    "US-NE": "Nebraska",
    "US-NH": "New Hampshire",
    "US-NJ": "New Jersey",
    "US-NM": "New Mexico",
    "US-NV": "Nevada",
    "US-NY": "New York",
    "US-OH": "Ohio",
    "US-OK": "Oklahoma",
    "US-OR": "Oregon",
    "US-PA": "Pennsylvania",
    "US-RI": "Rhode Island",
    "US-SC": "South Carolina",
    "US-SD": "South Dakota",
    "US-TN": "Tennessee",
    "US-TX": "Texas",
    "US-UT": "Utah",
    "US-VA": "Virginia",
    "US-VT": "Vermont",
    "US-WA": "Washington",
    "US-WI": "Wisconsin",
    "US-WV": "West Virginia",
    "US-WY": "Wyoming",
    "UY": "Uruguay",
    "UZ": "Uzbekistan",
    "VA": "Vatican City (Holy See)",
    "VC": "Saint Vincent and the Grenadines",
    "VE": "Venezuela",
    "VG": "British Virgin Islands",
    "VI": "United States Virgin Islands",
    "VN": "Vietnam",
    "VU": "Vanuatu",
    "WF": "Wallis and Futuna",
    "WS": "Samoa",
    "XK": "Kosovo",
    "YE": "Yemen",
    "YT": "Mayotte",
    "ZA": "South Africa",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
    "UK": "United Kingdom",
    "IND": "India", "NPL": "Nepal", "USA": "United States",
}


def parse_m3u(text):
    """Parse an M3U playlist into channel dicts.

    Name parsing is attribute-aware: the name starts after the comma that
    follows the last key="value" attribute, so commas inside quoted values
    (e.g. http-user-agent strings) do not corrupt the name.
    """
    channels = []
    cur = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXTINF:"):
            m = re.match(r"^#EXTINF:-?\d+((?:\s+[A-Za-z0-9-]+=\"[^\"]*\")*),(.*)$", line)
            attrs = m.group(1) if m else ""
            name = m.group(2).strip() if m else ""
            g = re.search(r'group-title="([^"]*)"', attrs)
            t = re.search(r'tvg-id="([^"]*)"', attrs)
            l = re.search(r'tvg-logo="([^"]*)"', attrs)
            group = g.group(1).strip() if g else ""
            if not group or group.lower() == "undefined":
                group = "General"
            cur = {
                "name": name or "Channel",
                "group": group,
                "category": group.split(";")[0].strip() or "General",
                "tvg_id": t.group(1) if t else "",
                "logo": l.group(1) if l else "",
                "country": "International",
                "url": "",
            }
            # Country: Nepal/India name keywords first (catches e.g.
            # "Nepal 1", tvg-id Nepal1.in), then the tvg-id country code.
            hay = (cur["name"] + " " + group).lower()
            if re.search(r"\bnepal\b|\bnepali\b", hay):
                cur["country"] = "Nepal"
            elif re.search(r"\bindia\b|\bhindi\b", hay):
                cur["country"] = "India"
            elif cur["tvg_id"]:
                cm = re.search(r"\.([A-Za-z]{2})(?:@|$)", cur["tvg_id"])
                if cm:
                    code = cm.group(1).upper()
                    cur["country"] = COUNTRY_NAMES.get(code, code)
        elif not line.startswith("#") and cur is not None:
            cur["url"] = line
            channels.append(cur)
            cur = None
    return channels


def probe_manifest(url, timeout=10, max_bytes=65536):
    """Tier 1: fetch a stream manifest and classify it.

    Returns (status, reason) with status in working/uncertain/dead/skipped.
    """
    if not url.startswith(("http://", "https://")):
        return ("skipped", "unsupported scheme")
    start = time.monotonic()
    try:
        with requests.get(url, headers={"User-Agent": UA, "Accept": "*/*"},
                          timeout=timeout, stream=True) as resp:
            if resp.status_code in (403, 451, 429):
                return ("uncertain", f"http {resp.status_code}")
            if resp.status_code != 200:
                return ("dead", f"http {resp.status_code}")
            chunks = []
            read = 0
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    chunks.append(chunk)
                    read += len(chunk)
                if read >= max_bytes or time.monotonic() - start > timeout:
                    break
            body = b"".join(chunks)
            if not body:
                return ("uncertain", "empty body")
            text = body[:max_bytes].decode("utf-8", errors="ignore")
            if "#EXTM3U" in text or "<MPD" in text:
                return ("working", "playlist ok")
            return ("dead", "invalid body")
    except requests.exceptions.Timeout:
        return ("dead", "timeout")
    except requests.exceptions.ConnectionError as e:
        return ("dead", f"connection error: {type(e).__name__}")
    except requests.exceptions.RequestException as e:
        return ("dead", f"request error: {type(e).__name__}")


def tier1(channels, workers=30, timeout=10, limit=None, prior=None,
          checkpoint_path=None):
    """Probe every channel manifest concurrently.

    prior: previously probed results (url -> {status, reason}) to skip.
    checkpoint_path: Tier 1 results are checkpointed here every 500 probes
    so a killed run can resume with --tier1-in instead of starting over.
    """
    todo = channels if limit is None else channels[:limit]
    results = dict(prior or {})
    todo = [c for c in todo if c["url"] not in results]
    done = 0
    total = len(todo)
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(probe_manifest, c["url"], timeout): c for c in todo}
        for fut in as_completed(futs):
            ch = futs[fut]
            try:
                status, reason = fut.result()
            except Exception as e:  # pragma: no cover - defensive
                status, reason = "dead", f"probe error: {e}"
            results[ch["url"]] = {"status": status, "reason": reason}
            done += 1
            if done % 500 == 0 or done == total:
                el = time.monotonic() - t0
                print(f"  tier1: {done}/{total} "
                      f"({done / el:.1f}/s, {el:.0f}s elapsed)", flush=True)
                if checkpoint_path:
                    atomic_write(checkpoint_path, {
                        "checked_at": datetime.now(timezone.utc).isoformat(),
                        "partial": True,
                        "results": results})
    return results


def ffprobe_ok(url, timeout=25):
    """Tier 2: confirm ffprobe finds a playable audio/video stream."""
    cmd = ["ffprobe", "-v", "error", "-user_agent", UA,
           "-analyzeduration", "5000000", "-probesize", "5000000",
           "-show_entries", "stream=codec_type",
           "-of", "json", url]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False
    if proc.returncode != 0:
        return False
    try:
        data = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError:
        return False
    kinds = {s.get("codec_type") for s in data.get("streams", [])}
    return bool(kinds & {"video", "audio"})


def tier2(candidates, workers=8, timeout=25):
    """ffprobe-confirm candidates concurrently. Returns the confirmed list."""
    confirmed = []
    done = 0
    total = len(candidates)
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(ffprobe_ok, c["url"], timeout): c for c in candidates}
        for fut in as_completed(futs):
            ch = futs[fut]
            try:
                ok = fut.result()
            except Exception:
                ok = False
            if ok:
                confirmed.append(ch)
            done += 1
            if done % 25 == 0 or done == total:
                print(f"  tier2: {done}/{total} confirmed={len(confirmed)} "
                      f"({time.monotonic() - t0:.0f}s)", flush=True)
    return confirmed


def select_recommended(confirmed, target=36, max_candidates=120):
    """Pick the Recommended row: prioritise Nepal, India and US
    news/sports/movies, then spread across other categories/countries."""
    def bucket(ch):
        return (ch["country"], ch["category"])

    picked, seen = [], set()

    def take(pred, n):
        for ch in confirmed:
            if len(picked) >= target:
                return
            if ch["url"] in seen:
                continue
            if pred(ch):
                picked.append(ch)
                seen.add(ch["url"])
                n -= 1
                if n <= 0:
                    return

    # 1. Nepal first (any category) — up to 8
    take(lambda c: c["country"] == "Nepal", 8)
    # 2. India news/sports/movies — up to 10
    take(lambda c: c["country"] == "India"
         and c["category"] in ("News", "Sports", "Movies"), 10)
    # 3. US news/sports/movies — up to 8
    take(lambda c: c["country"] == "United States"
         and c["category"] in ("News", "Sports", "Movies"), 8)
    # 4. Spread: round-robin over remaining categories, preferring
    #    countries not yet represented.
    cats = ["News", "Sports", "Movies", "Entertainment", "Kids", "Music",
            "Documentary", "Series", "Religious", "Business", "Education",
            "Comedy", "Culture", "Animation", "Lifestyle", "General"]
    have_countries = {c["country"] for c in picked}
    for cat in cats:
        if len(picked) >= target:
            break
        cands = [c for c in confirmed
                 if c["url"] not in seen and c["category"] == cat]
        cands.sort(key=lambda c: (c["country"] in have_countries, c["name"]))
        for ch in cands[:2]:
            if len(picked) >= target:
                break
            picked.append(ch)
            seen.add(ch["url"])
            have_countries.add(ch["country"])
    # 5. Fill any remaining slots with whatever is left, by name.
    if len(picked) < target:
        rest = sorted((c for c in confirmed if c["url"] not in seen),
                      key=lambda c: c["name"])
        for ch in rest:
            if len(picked) >= target:
                break
            picked.append(ch)
            seen.add(ch["url"])
    order = {"Nepal": 0, "India": 1, "United States": 2}
    picked.sort(key=lambda c: (order.get(c["country"], 3),
                               c["category"], c["name"]))
    return picked


def select_featured(working, confirmed, movies_n=8, music_n=4):
    """Pick the Featured Live row: running Movies + Music channels.

    Prefers ffprobe-confirmed streams, falling back to Tier-1 working
    ones when there are not enough confirmed. HTTPS + logo required:
    HTTP streams are blocked as mixed content on the HTTPS site, and
    the hero/row need artwork. Output interleaves movies and music
    (two movies, one music) for the hero rotation.
    """
    def pool_for(cat, n, source):
        cands = [c for c in source
                 if c["category"] == cat
                 and c["url"].startswith("https://") and c["logo"]]
        # Spread across countries: one per country first, then fill.
        picked, seen_countries = [], set()
        for ch in cands:
            if len(picked) >= n:
                break
            if ch["country"] not in seen_countries:
                picked.append(ch)
                seen_countries.add(ch["country"])
        for ch in cands:
            if len(picked) >= n:
                break
            if ch["url"] not in {p["url"] for p in picked}:
                picked.append(ch)
        return picked

    movies = pool_for("Movies", movies_n, confirmed)
    if len(movies) < movies_n:
        movies = pool_for("Movies", movies_n, working)
    music = pool_for("Music", music_n, confirmed)
    if len(music) < music_n:
        music = pool_for("Music", music_n, working)
    featured = []
    mi = ui = 0
    while mi < len(movies) or ui < len(music):
        for _ in range(2):
            if mi < len(movies):
                featured.append(movies[mi])
                mi += 1
        if ui < len(music):
            featured.append(music[ui])
            ui += 1
    return featured


def candidate_pool(working, max_candidates=120):
    """Rank Tier-1 working channels for ffprobe: priority buckets first,
    then one per category/country round-robin so Tier 2 covers the spread."""
    pool, seen = [], set()

    def take(pred, n):
        for ch in working:
            if len(pool) >= max_candidates or n <= 0:
                return
            if ch["url"] in seen:
                continue
            if pred(ch):
                pool.append(ch)
                seen.add(ch["url"])
                n -= 1

    take(lambda c: c["country"] == "Nepal", 20)
    take(lambda c: c["country"] == "India"
         and c["category"] in ("News", "Sports", "Movies"), 30)
    take(lambda c: c["country"] == "United States"
         and c["category"] in ("News", "Sports", "Movies"), 25)
    cats = ["News", "Sports", "Movies", "Entertainment", "Kids", "Music",
            "Documentary", "Series", "Religious", "Business", "Education",
            "Comedy", "Culture", "Animation", "Lifestyle"]
    for cat in cats:
        take(lambda c, cat=cat: c["category"] == cat, 3)
    if len(pool) < max_candidates:
        for ch in working:
            if len(pool) >= max_candidates:
                break
            if ch["url"] not in seen:
                pool.append(ch)
                seen.add(ch["url"])
    return pool


def atomic_write(path, payload):
    """Write JSON atomically: temp file in the same dir, then rename."""
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
            f.write("\n")
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def main():
    ap = argparse.ArgumentParser(description="Check IPTV stream health.")
    ap.add_argument("--playlist", default=DEFAULT_PLAYLIST)
    ap.add_argument("--out", default="recommended.json")
    ap.add_argument("--featured-out", default="featured-live.json",
                    help="also write the Featured Live list here "
                    "(empty string disables it)")
    ap.add_argument("--tier1-out", default=None,
                    help="also save full Tier 1 results here")
    ap.add_argument("--tier1-in", default=None,
                    help="reuse Tier 1 results instead of probing")
    ap.add_argument("--workers", type=int, default=30)
    ap.add_argument("--probe-workers", type=int, default=8)
    ap.add_argument("--timeout", type=int, default=10)
    ap.add_argument("--limit", type=int, default=None,
                    help="only check the first N channels (smoke test)")
    ap.add_argument("--candidates", type=int, default=120)
    ap.add_argument("--target", type=int, default=36)
    args = ap.parse_args()

    print(f"fetching {args.playlist}", flush=True)
    resp = requests.get(args.playlist, headers={"User-Agent": UA}, timeout=60)
    resp.raise_for_status()
    channels = parse_m3u(resp.text)
    # Never recommend NSFW, and never probe entries with no URL.
    channels = [c for c in channels
                if c["url"] and not NSFW.search(c["name"])
                and not NSFW.search(c["group"])]
    if args.limit:
        channels = channels[:args.limit]
    print(f"channels to check: {len(channels)}", flush=True)

    prior = None
    if args.tier1_in:
        with open(args.tier1_in, encoding="utf-8") as f:
            prior = json.load(f)["results"]
        have = sum(1 for c in channels if c["url"] in prior)
        print(f"loaded {len(prior)} prior Tier 1 results "
              f"({have}/{len(channels)} channels covered)", flush=True)
    if prior is not None and all(c["url"] in prior for c in channels):
        results = prior
        print("all channels already probed — skipping Tier 1", flush=True)
    else:
        print("tier 1: probing manifests ...", flush=True)
        results = tier1(channels, workers=args.workers, timeout=args.timeout,
                        prior=prior, checkpoint_path=args.tier1_out)
        if args.tier1_out:
            atomic_write(args.tier1_out, {
                "checked_at": datetime.now(timezone.utc).isoformat(),
                "partial": False,
                "results": results})
            print(f"tier 1 results saved to {args.tier1_out}", flush=True)

    counts = {"working": 0, "dead": 0, "uncertain": 0, "skipped": 0}
    for r in results.values():
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print(f"tier 1: {counts}", flush=True)

    working = [c for c in channels
               if results.get(c["url"], {}).get("status") == "working"]
    print(f"tier 1 working: {len(working)}", flush=True)

    pool = candidate_pool(working, max_candidates=args.candidates)
    print(f"tier 2: ffprobe-confirming {len(pool)} candidates ...", flush=True)
    confirmed = tier2(pool, workers=args.probe_workers)
    print(f"tier 2 confirmed: {len(confirmed)}", flush=True)

    picked = select_recommended(confirmed, target=args.target)
    print(f"recommended row: {len(picked)} channels", flush=True)
    for ch in picked:
        print(f"  {ch['country']} | {ch['category']:13s} | {ch['name']}", flush=True)

    payload = {
        "last_checked": datetime.now(timezone.utc).isoformat(),
        "source": args.playlist,
        "total_tested": len(channels),
        "working": counts.get("working", 0),
        "dead": counts.get("dead", 0),
        "uncertain": counts.get("uncertain", 0),
        "skipped": counts.get("skipped", 0),
        "ffprobe_confirmed": len(confirmed),
        "channels": [{
            "tvg_id": c["tvg_id"],
            "name": c["name"],
            "url": c["url"],
            "category": c["category"],
            "country": c["country"],
            "logo": c["logo"],
        } for c in picked],
    }
    atomic_write(args.out, payload)
    print(f"wrote {args.out}", flush=True)

    if args.featured_out:
        featured = select_featured(working, confirmed)
        if len(featured) >= 6:
            feat_payload = {
                "last_checked": datetime.now(timezone.utc).isoformat(),
                "source": args.playlist,
                "probe": ("tier1 manifest fetch (HTTP 200 + #EXTM3U), "
                          "https + logo required; ffprobe-confirmed preferred"),
                "total_tested": len(channels),
                "movies": sum(1 for c in featured
                              if c["category"] == "Movies"),
                "music": sum(1 for c in featured
                             if c["category"] == "Music"),
                "channels": [{
                    "tvg_id": c["tvg_id"],
                    "name": c["name"],
                    "url": c["url"],
                    "category": c["category"],
                    "country": c["country"],
                    "logo": c["logo"],
                } for c in featured],
            }
            atomic_write(args.featured_out, feat_payload)
            print(f"featured live: {len(featured)} channels "
                  f"wrote {args.featured_out}", flush=True)
        else:
            print(f"featured live: only {len(featured)} verified "
                  f"movies/music channels — leaving {args.featured_out} "
                  f"unchanged", flush=True)


if __name__ == "__main__":
    main()
