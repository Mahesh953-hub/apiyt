"""apiyt — browserless YouTube→MP3 CLI with a rich terminal UI.

Search and downloads run in background threads; the foreground shows a live
dashboard (queued / downloading / done / failed, bytes, speed, progress bars).
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import shlex
import subprocess
import sys
import threading
import time
from collections import OrderedDict
from pathlib import Path

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

import core

err = Console(stderr=True)          # status/progress -> stderr so stdout stays clean
out = Console()                      # regular output

QUEUE_FILE = Path.home() / ".apiyt_queue.json"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024
    return f"{n:.1f}GB"


def human_count(n) -> str:
    if n is None:
        return "—"
    n = int(n)
    for unit in ("", "K", "M", "B"):
        if n < 1000 or unit == "B":
            return f"{n}{unit}" if unit else str(n)
        n /= 1000
    return f"{n:.1f}B"


def human_duration(sec) -> str:
    if sec is None:
        return "—"
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def load_queue() -> list:
    if QUEUE_FILE.exists():
        try:
            return json.loads(QUEUE_FILE.read_text())
        except Exception:
            return []
    return []


def save_queue(items: list):
    QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    QUEUE_FILE.write_text(json.dumps(items, indent=2))


# --------------------------------------------------------------------------- #
# background download engine + live dashboard
# --------------------------------------------------------------------------- #
def _build_dashboard(states, order, panel_title="apiyt") -> Panel:
    t = Table(box=box.MINIMAL_HEAVY_HEAD, expand=True, pad_edge=False)
    t.add_column("Video ID", style="cyan", no_wrap=True)
    t.add_column("State")
    t.add_column("Progress", min_width=22)
    t.add_column("Size", justify="right")
    t.add_column("Speed", justify="right")
    t.add_column("Result", overflow="fold")

    for vid in order:
        s = states[vid]
        st = s["state"]
        if st == "queued":
            row = [vid, "[dim]queued[/]", Text("… pending", style="dim"), "—", "—", ""]
        elif st == "downloading":
            total, done = s["total"], s["done"]
            bar = ProgressBar(total=(total or None), completed=done, width=22)
            if total:
                pct = min(100, int(done * 100 / total))
                size = f"{human(done)} / {human(total)} ({pct}%)"
            else:
                size = f"{human(done)}"
            speed = f"{human(s['speed'])}/s" if s["speed"] else "…"
            row = [vid, "[yellow]downloading[/]", bar, size, speed, ""]
        elif st == "ok":
            row = [vid, "[green]done[/]", Text("✓", style="green"),
                   human(s["done"]), "", s["path"]]
        else:  # error
            row = [vid, "[red]error[/]", Text("✗", style="red"), "—", "",
                   s.get("err") or ""]
        t.add_row(*row)

    ok = sum(1 for v in order if states[v]["state"] == "ok")
    fails = sum(1 for v in order if states[v]["state"] == "error")
    active = sum(1 for v in order if states[v]["state"] == "downloading")
    footer = Text(
        f"{ok}/{len(order)} done · {active} active · "
        f"{sum(1 for v in order if states[v]['state']=='queued')} queued · {fails} failed",
        style="bold",
    )
    return Panel(Group(t, footer), title=panel_title, border_style="blue")


def run_downloads(ids, out_dir=".", workers=3, panel_title="apiyt") -> dict:
    os.makedirs(out_dir, exist_ok=True)
    ids = [core.extract_vid(i) for i in ids]
    ids = list(dict.fromkeys(ids))  # dedupe, keep order
    states = OrderedDict(
        (vid, {"state": "queued", "total": None, "done": 0, "speed": 0.0, "err": "", "path": ""})
        for vid in ids
    )
    lock = threading.RLock()
    q = queue.Queue()
    for vid in ids:
        q.put(vid)

    def worker():
        while True:
            try:
                vid = q.get_nowait()
            except queue.Empty:
                return
            try:
                with lock:
                    states[vid]["state"] = "downloading"
                _info, resp = core.open_audio(vid)
                total = resp.headers.get("content-length")
                total = int(total) if total and total.isdigit() else None
                fn = os.path.join(out_dir, f"{vid}.mp3")
                done, last_t, last_d = 0, time.monotonic(), 0
                with open(fn, "wb") as fh:
                    for chunk in resp.iter_content(core.CHUNK):
                        fh.write(chunk)
                        done += len(chunk)
                        now = time.monotonic()
                        dt = now - last_t
                        if dt >= 0.3:
                            speed = (done - last_d) / dt
                            last_d, last_t = done, now
                            with lock:
                                states[vid]["done"] = done
                                states[vid]["speed"] = speed
                                states[vid]["total"] = total
                with lock:
                    states[vid].update(state="ok", done=done, speed=0.0, total=total, path=fn)
            except Exception as e:  # noqa: BLE001
                with lock:
                    states[vid].update(state="error", err=str(e))
            finally:
                q.task_done()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(max(1, workers))]
    for t in threads:
        t.start()

    def done_all():
        return all(states[v]["state"] in ("ok", "error") for v in ids)

    with Live(_build_dashboard(states, ids, panel_title), console=err,
              refresh_per_second=8, screen=False) as live:
        while not done_all():
            live.update(_build_dashboard(states, ids, panel_title))
            time.sleep(0.15)
        live.update(_build_dashboard(states, ids, panel_title), refresh=True)

    for t in threads:
        t.join()

    return {v: dict(states[v]) for v in ids}


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #
def cmd_search(args):
    items, lock = [], threading.Lock()
    ev_done = threading.Event()
    failed = []

    def fetch():
        try:
            for it in core.search(args.query, args.limit):
                with lock:
                    items.append(it)
        except Exception as e:  # noqa: BLE001
            failed.append(str(e))
        finally:
            ev_done.set()

    threading.Thread(target=fetch, daemon=True).start()

    def build() -> Table:
        table = Table(box=box.SIMPLE_HEAVY, expand=True, title=f"Search: [bold]{args.query}[/]")
        table.add_column("#", style="dim")
        table.add_column("Title", style="bold cyan", ratio=3, overflow="fold")
        table.add_column("Channel", style="magenta", no_wrap=True)
        table.add_column("Duration", justify="right")
        table.add_column("Views", justify="right")
        table.add_column("ID", style="green", no_wrap=True)
        with lock:
            snap = list(items)
        if not snap and not ev_done.is_set():
            table.add_row("…", "[dim]searching…[/]", "", "", "", "")
            return table
        for i, it in enumerate(snap, 1):
            views = human_count(it["views"])
            table.add_row(str(i), it["title"], it["channel"],
                          human_duration(it["duration"]), views, it["id"])
        return table

    with Live(console=out, refresh_per_second=12, get_renderable=build) as live:
        while not ev_done.is_set():
            live.refresh()
            time.sleep(0.08)
        live.refresh()

    if failed:
        err.print(f"[red]search error:[/] {failed[0]}")


def cmd_download(args):
    run_downloads(args.ids, out_dir=args.out_dir, workers=args.workers, panel_title="apiyt downloads")


def cmd_stream(args):
    vid = core.extract_vid(args.id)
    sink = None
    proc = None
    if args.player:
        proc = subprocess.Popen(shlex.split(args.player), stdin=subprocess.PIPE)
        sink = proc.stdin
    else:
        sink = getattr(sys.stdout, "buffer", sys.stdout)

    _info, resp = core.open_audio(vid)
    total = resp.headers.get("content-length")
    total = int(total) if total and total.isdigit() else None

    with Progress(
        SpinnerColumn(), TextColumn("[bold cyan]{task.description}"),
        BarColumn(), DownloadColumn(), TransferSpeedColumn(), TimeElapsedColumn(),
        console=err, transient=False, disable=args.no_progress,
    ) as progress:
        task = progress.add_task(vid, total=total)
        for chunk in resp.iter_content(core.CHUNK):
            sink.write(chunk)
            sink.flush()
            progress.update(task, advance=len(chunk))

    if proc:
        proc.stdin.close()
        proc.wait()
    if not args.player:
        err.print(f"[green]streamed[/] {vid}")


def cmd_queue(args):
    action = args.action or "list"
    items = load_queue()

    if action == "add":
        added = 0
        for i in args.items:
            v = core.extract_vid(i)
            if v and v not in items:
                items.append(v)
                added += 1
        save_queue(items)
        out.print(f"[green]{added}[/] added · queue now holds {len(items)}")
        return

    if action == "remove":
        target = {core.extract_vid(i) for i in args.items}
        new = [i for i in items if i not in target]
        removed = len(items) - len(new)
        save_queue(new)
        out.print(f"[green]{removed}[/] removed · queue now holds {len(new)}")
        return

    if action == "clear":
        n = len(items)
        save_queue([])
        out.print(f"[green]cleared[/] {n} queued items")
        return

    if action == "run":
        if not items:
            out.print("[dim]queue is empty[/]")
            return
        out.print(f"[bold]draining[/] {len(items)} item(s)")
        results = run_downloads(items, out_dir=args.out_dir, workers=args.workers, panel_title="apiyt queue")
        ok = [v for v, r in results.items() if r["state"] == "ok"]
        failed = {v for v, r in results.items() if r["state"] != "ok"}
        save_queue([v for v in items if v in failed])
        out.print(f"[green]{len(ok)} downloaded[/] · [red]{len(failed)} kept in queue[/]")
        return

    # default: list
    if not items:
        out.print("[dim]queue is empty — add with: apiyt queue add <id>…[/]")
        return
    t = Table(box=box.SIMPLE_HEAVY, title="Download queue")
    t.add_column("#", style="dim")
    t.add_column("Video ID", style="cyan")
    for i, v in enumerate(items, 1):
        t.add_row(str(i), v)
    out.print(t)
    out.print(f"[bold]{len(items)}[/] queued — run with: [green]apiyt queue run[/]")


# --------------------------------------------------------------------------- #
# arg parsing
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="apiyt",
        description="Browserless YouTube→MP3 (search, stream, download, queue).",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("search", help="search videos (background)")
    s.add_argument("query")
    s.add_argument("--limit", type=int, default=15)
    s.set_defaults(func=cmd_search)

    d = sub.add_parser("download", help="download one or more videos (queued, background)")
    d.add_argument("ids", nargs="+")
    d.add_argument("--out", dest="out_dir", default=".")
    d.add_argument("--workers", type=int, default=3)
    d.set_defaults(func=cmd_download)

    st = sub.add_parser("stream", help="stream mp3 bytes to stdout (pipe to a player)")
    st.add_argument("id")
    st.add_argument("--player", default=None, help='player command, e.g. --player "mpv -"')
    st.add_argument("--no-progress", action="store_true")
    st.set_defaults(func=cmd_stream)

    q = sub.add_parser("queue", help="manage the download queue")
    q.add_argument("action", nargs="?", default="list",
                   choices=["list", "add", "remove", "clear", "run"])
    q.add_argument("items", nargs="*")
    q.add_argument("--out", dest="out_dir", default=".")
    q.add_argument("--workers", type=int, default=3)
    q.set_defaults(func=cmd_queue)

    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
