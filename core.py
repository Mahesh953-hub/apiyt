"""Browserless apiyt engine: search, resolve, stream, download.

Reproduces window.ttt locally (no browser / no JS):
  mss  = "get" + ytid + "now"
  pwx  = "jXcFfyfv67i6vi3MAuyPEgvRewxI9wLw"   # hardcoded in apiyt's client JS
  key  = PBKDF2-SHA1(pwx, salt=32B random, 100 iters, 256-bit)
  ttt  = base64( salt(32B) + iv(16B) + AES-256-CBC-Pkcs7(mss, key, iv) )
"""
from __future__ import annotations

import base64
import hashlib
import os
import re
import time
from urllib.parse import parse_qs, urlparse

import requests
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

PWX = "jXcFfyfv67i6vi3MAuyPEgvRewxI9wLw"
PREFIX, SUFFIX = "get", "now"
UA = (
    "Mozilla/5.0 (Linux; Android 16; realme 5s Build/BP4A.251205.006; wv) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/150.0.7871.181 Mobile Safari/537.36"
)
CHUNK = 1 << 16  # 64 KiB


class ResolutionError(RuntimeError):
    pass


def make_ttt(vid: str) -> str:
    """Recompute window.ttt for a video id entirely client-side."""
    mss = (PREFIX + vid + SUFFIX).encode()
    salt, iv = os.urandom(32), os.urandom(16)
    key = hashlib.pbkdf2_hmac("sha1", PWX.encode(), salt, 100, dklen=32)
    padder = padding.PKCS7(128).padder()
    pt = padder.update(mss) + padder.finalize()
    enc = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
    ct = enc.update(pt) + enc.finalize()
    return base64.b64encode(salt + iv + ct).decode()


def extract_vid(text: str) -> str:
    """Accept a raw 11-char id, watch URL, or youtu.be short URL."""
    t = text.strip()
    if "://" in t:
        u = urlparse(t)
        if u.netloc.endswith("youtu.be"):
            return u.path.lstrip("/").split("/")[0]
        q = parse_qs(u.query)
        if "v" in q:
            return q["v"][0]
    return re.sub(r"[^a-zA-Z0-9_-]", "", t)


def _search_page(query: str, page_token: str = None):
    params = {"q": query}
    if page_token:
        params["pageToken"] = page_token
    r = requests.get(
        "https://yt-meta.convert1s.com/search",
        params=params,
        headers={
            "User-Agent": UA,
            "Accept": "application/json",
            "Origin": "https://media.ytmp3.gg",
            "Referer": "https://media.ytmp3.gg/",
            "X-Requested-With": "mark.via.gp",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def search(query: str, limit: int = 15):
    """Search and yield one result dict at a time (background-friendly)."""
    data = _search_page(query)
    seen, emitted = set(), 0
    for it in data.get("items", []):
        if it.get("type") != "stream":
            continue
        raw_id = it.get("id") or ""
        if "watch?v=" not in raw_id:
            continue
        vid = extract_vid(raw_id)
        if not vid or vid in seen:
            continue
        seen.add(vid)
        yield {
            "id": vid,
            "title": it.get("title") or "",
            "channel": it.get("uploaderName") or "",
            "duration": it.get("duration"),
            "views": it.get("viewCount"),
        }
        emitted += 1
        if emitted >= limit:
            return


def resolve(vid: str, session: requests.Session = None) -> dict:
    """GET the iframe, read sid, POST token -> direct download metadata."""
    vid = extract_vid(vid)
    s = session or requests.Session()
    s.headers.update({"User-Agent": UA})

    html = s.get(f"https://apiyt.com/iframe/?vid={vid}", timeout=30).text
    m = re.search(r"'sid':\s*'([^']+)'", html)
    if not m:
        return {"vid": vid, "status": "error", "note": "sid not found in iframe"}

    sid = m.group(1)
    r = s.post(
        "https://apiyt.com/downloader.v2.php",
        data={"sid": sid, "token": make_ttt(vid), "_": str(int(time.time() * 1000))},
        timeout=60,
    )
    try:
        d = r.json()
    except Exception:
        d = {"status": "error"}

    out = {
        "vid": vid,
        "status": d.get("status"),
        "engine": d.get("engine"),
        "durl": d.get("durl"),
        "_session": s,
    }
    if d.get("status") != "ok":
        out["note"] = r.text[:300]
    elif not out["durl"]:
        out["note"] = "no direct MP3 (likely an official-music / VEVO video)"
    return out


def open_audio(vid: str, session: requests.Session = None):
    """Resolve and return (info, streaming response) for the mp3."""
    info = resolve(vid, session=session)
    if info.get("status") != "ok" or not info.get("durl"):
        raise ResolutionError(f"{info['vid']}: {info.get('note') or 'no download available'}")
    s = info["_session"]
    resp = s.get(info["durl"], stream=True, timeout=120)
    if resp.status_code != 200:
        raise ResolutionError(f"{info['vid']}: download endpoint HTTP {resp.status_code}")
    ct = (resp.headers.get("content-type") or "").lower()
    if ct and "audio" not in ct and "octet-stream" not in ct:
        preview = resp.content[:120].decode("utf-8", "replace")
        raise ResolutionError(f"{info['vid']}: server returned '{ct}' ({preview!r}) — not convertible")
    return info, resp


def download(vid: str, dest_path: str, progress=None, session: requests.Session = None) -> str:
    """Download the mp3 to dest_path. progress(done:int, total:int|None)."""
    _info, resp = open_audio(vid, session=session)
    total = resp.headers.get("content-length")
    total = int(total) if total and total.isdigit() else None
    done = 0
    with open(dest_path, "wb") as fh:
        for chunk in resp.iter_content(CHUNK):
            fh.write(chunk)
            done += len(chunk)
            if progress:
                progress(done, total)
    return dest_path


def iter_audio(vid: str, session: requests.Session = None):
    """Yield mp3 bytes (for streaming to stdout / a player)."""
    _info, resp = open_audio(vid, session=session)
    for chunk in resp.iter_content(CHUNK):
        yield chunk
