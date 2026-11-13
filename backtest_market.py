"""Read Binance's public USD-M archives for the copy-trading replay."""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timedelta, timezone
from io import BytesIO, TextIOWrapper
from urllib.error import HTTPError
from urllib.request import urlopen
from zipfile import ZipFile


ARCHIVE = "https://data.binance.vision/data/futures/um/"
MINUTE = 60_000


def utc_ms(day):
    return int(datetime.fromisoformat(str(day)).replace(tzinfo=timezone.utc).timestamp() * 1000)


def utc_day(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).date()


def archive_names(first_day, end):
    day = first_day.replace(day=1)
    while day < end:
        next_month = (day + timedelta(days=32)).replace(day=1)
        if next_month <= end:
            yield "monthly", day.strftime("%Y-%m")
        else:
            for n in range((end - max(day, first_day)).days):
                yield "daily", str(max(day, first_day) + timedelta(days=n))
        day = next_month


def archive_path(symbol, kind, period, stamp):
    if kind == "fundingRate":
        return f"{period}/{kind}/{symbol}/{symbol}-{kind}-{stamp}.zip"
    return f"{period}/{kind}/{symbol}/1m/{symbol}-1m-{stamp}.zip"


def load_archive(path, cache):
    file = cache / path
    if file.exists():
        return file.read_bytes()
    try:
        with urlopen(ARCHIVE + path, timeout=20) as response:
            content = response.read()
    except HTTPError as error:
        if error.code == 404:
            return None
        raise
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_bytes(content)
    return content


def rows_from_zip(content):
    with ZipFile(BytesIO(content)) as archive:
        with archive.open(archive.namelist()[0]) as member:
            for row in csv.reader(TextIOWrapper(member, encoding="utf-8-sig")):
                if row and row[0].isdigit():
                    yield row


def fetch_market(first_by_symbol, end, cache, progress=False):
    """Return minute trades, minute marks, actual funding and archive coverage."""
    jobs = []
    for symbol, first_day in sorted(first_by_symbol.items()):
        for period, stamp in archive_names(first_day, end):
            for kind in ("klines", "markPriceKlines", "fundingRate"):
                if period == "daily" and kind == "fundingRate":
                    continue
                jobs.append((symbol, kind, period, stamp))

    def read(job):
        symbol, kind, period, stamp = job
        path = archive_path(symbol, kind, period, stamp)
        content = load_archive(path, cache)
        return job, content

    trades, marks, funding = defaultdict(dict), defaultdict(dict), defaultdict(list)
    funding_months = set()
    unavailable = []
    end_ms = utc_ms(end)
    first_ms = {symbol: utc_ms(day) for symbol, day in first_by_symbol.items()}
    archived = 0
    with ThreadPoolExecutor(max_workers=6) as executor:
        for index, (job, content) in enumerate(executor.map(read, jobs), 1):
            symbol, kind, period, stamp = job
            if content is None:
                unavailable.append(archive_path(*job))
                continue
            archived += 1
            if kind == "fundingRate":
                funding_months.add((symbol, stamp))
            for row in rows_from_zip(content):
                t = int(row[0])
                if not first_ms[symbol] <= t < end_ms:
                    continue
                if kind == "fundingRate":
                    funding[t // MINUTE].append((symbol, float(row[2])))
                elif kind == "klines":
                    trades[symbol][t // MINUTE] = (float(row[1]), float(row[4]))
                else:
                    marks[symbol][t // MINUTE] = (float(row[1]), float(row[2]), float(row[3]), float(row[4]))
            if progress and (index % 40 == 0 or index == len(jobs)):
                print(f"Market archives {index}/{len(jobs)}", flush=True)
    return trades, marks, funding, funding_months, {
        "archive_count": archived, "missing_archives": unavailable,
    }
