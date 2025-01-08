from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import NamedTuple

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, HttpUrl
from rich.console import Console
from rich.logging import RichHandler
from yarl import URL


class ForumThreadTuple(NamedTuple):
    host: str
    name: str
    id_: int
    page: int | None
    post: int | None
    path_qs: str


app = FastAPI()

logger = logging.getLogger(__name__)

all_urls = set()


def setup_logger() -> None:
    logger.setLevel(10)
    rich_file_handler = RichHandler(
        console=Console(file=Path("fast_api.log").open("w", encoding="utf8"), width=180),
        level=10,
    )

    rich_handler = RichHandler(
        show_time=False,
        level=10,
    )

    logger.addHandler(rich_file_handler)
    logger.addHandler(rich_handler)


class DatabaseEntry(BaseModel):
    origin: HttpUrl
    urls: list[str]


THREAD_PARTS = "threads", "topic"


@dataclass(slots=True, frozen=True)
class ForumThread:
    host: str
    name: str
    id_: int
    page: int = field(default=1)
    thread_path: str = field(default="/")
    post_number: int | None = field(default=None, compare=False, hash=False)
    path_qs: str = field(default="", compare=False, hash=False)

    @property
    def as_tuple(self) -> ForumThreadTuple:
        return ForumThreadTuple(self.host, self.name, self.id_, self.page, self.post_number, self.path_qs)  # type: ignore

    @property
    def url(self) -> URL:
        url = URL("https://" + self.host + self.path_qs)
        if self.post_number:
            url = url.with_fragment(f"post-{self.post_number}")
        return url

    @classmethod
    def from_row(cls, row: dict) -> ForumThread:
        url = "https://" + row["host"] + row["path_qs"]
        return cls.from_url(url)

    @classmethod
    def from_url(cls, origin: HttpUrl) -> ForumThread:
        url = URL(str(origin))
        msg = f"Invalid forum thread URL: {url}"
        if not url.host or not any(part in url.parts for part in THREAD_PARTS):
            raise ValueError(msg)

        host = url.host
        found_part = next(part for part in THREAD_PARTS if part in url.parts)
        name_index = url.parts.index(found_part) + 1
        name = url.parts[name_index]
        if "." not in name:
            raise ValueError(msg)

        post_sections = {url.fragment, *url.parts}
        post_string = next((sec for sec in post_sections if "post-" in sec), None)
        post_number = None
        if post_string:
            post_number = int(post_string.replace("post-", "").strip())

        page = 1
        if len(url.parts) > name_index + 1 and "page-" in url.parts[name_index + 1]:
            page = int(url.parts[name_index + 1].replace("page-", "").strip())

        name, id_ = name.rsplit(".")
        name = name.strip()
        id_ = int(id_.strip())
        thread_path = "/" + "/".join(url.parts[1 : name_index + 1])

        return cls(
            host=host,
            name=name,
            post_number=post_number,
            page=page,
            id_=id_,
            thread_path=thread_path,
            path_qs=url.path_qs,
        )


def save_data(data: DatabaseEntry) -> int:
    try:
        forum_thread = ForumThread.from_url(data.origin)
        return _save_forum_thread_urls(forum_thread, data.urls)
    except ValueError:
        return _save_other_urls(data.origin, data.urls)


def _save_forum_thread_urls(forum_thread: ForumThread, urls: list[str]) -> int:
    global all_urls
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    date_received = datetime.now(UTC).isoformat()
    before = len(all_urls)
    query = "INSERT OR IGNORE INTO forum_threads (host, name, id, page, post, path_qs, url, date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    for url in sorted(urls):
        if (forum_thread, url) in all_urls:
            logger.info(f"Skipped: {forum_thread.url} with {url}")
            continue
        cursor.execute(query, (*forum_thread.as_tuple, url, date_received))
        all_urls.add((forum_thread, url))
    new_urls = len(all_urls) - before
    logger.info(f"Added {new_urls} url(s)")
    conn.commit()
    conn.close()
    return new_urls


def _save_other_urls(origin: HttpUrl, urls: list[str]) -> int:
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    date_received = datetime.now(UTC).isoformat()
    query = "INSERT OR IGNORE INTO other_urls (origin, url, date) VALUES (?, ?, ?)"
    before = len(all_urls)
    for url in sorted(urls):
        cursor.execute(query, (str(origin), url, date_received))
    new_urls = len(all_urls) - before
    conn.commit()
    conn.close()
    return new_urls


def get_urls_by_origin(origin: HttpUrl, page: int | None = None) -> list[str]:
    try:
        forum_thread = ForumThread.from_url(origin)
        return _get_forum_thread_urls(forum_thread, page)
    except ValueError:
        return _get_other_urls(origin)


def _get_forum_thread_urls(forum_thread: ForumThread, page: int | None = None) -> list[str]:
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    data = forum_thread.as_tuple[0:3]
    query = "SELECT url FROM forum_threads WHERE host = ? AND path = ? AND name = ?"
    if page:
        query += " AND page = ?"
        data = forum_thread.as_tuple

    cursor.execute(query, (*data,))
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]


def _get_other_urls(origin: HttpUrl) -> list[str]:
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    query = "SELECT url FROM other_urls WHERE origin = ?"
    cursor.execute(query, (str(origin),))
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]


def get_urls() -> set[tuple[ForumThread | str, str]]:
    conn = sqlite3.connect("data.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query = "SELECT * FROM forum_threads"
    cursor.execute(query)
    rows = cursor.fetchall()
    forum_urls = {(ForumThread.from_row(row), row["url"]) for row in rows}
    query = "SELECT * FROM other_urls"
    cursor.execute(query)
    rows = cursor.fetchall()
    other_urls = {tuple(row) for row in rows}
    conn.close()
    return forum_urls | other_urls  # type: ignore


@app.post("/submit")
async def submit_data(data: DatabaseEntry):
    try:
        new_urls = save_data(data)
        return {"status:": "OK", "message": f"Added {new_urls} new urls"}
    except Exception as e:
        logger.exception(f"{e}")
        details = f"status: ERROR - message: {e}"
        raise HTTPException(status_code=500, detail=details) from None


@app.get("/retrieve")
async def retrieve_data(origin: HttpUrl, page: int | None = Query(1, ge=1)):
    try:
        urls = get_urls_by_origin(origin, page)
        return {"urls": urls}
    except Exception as e:
        logger.exception(f"{e}")
        raise HTTPException(status_code=500, detail=str(e)) from None


def init_db():
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS forum_threads (
            host TEXT,
            name TEXT,
            id NUMBER,
            page NUMBER,
            post NUMBER,
            url TEXT,
            date TEXT,
            path_qs TEXT,
            UNIQUE(host, name, page, url)
        )
    """)
    conn.commit()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS other_urls (
            origin TEXT,
            url TEXT,
            date TEXT,
            UNIQUE(origin, url)
        )
    """)
    conn.commit()
    conn.close()


def main():
    global all_urls
    setup_logger()
    init_db()
    all_urls = get_urls()
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
