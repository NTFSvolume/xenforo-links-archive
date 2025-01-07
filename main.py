import sqlite3
from dataclasses import InitVar, dataclass, field
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, HttpUrl
from yarl import URL

app = FastAPI()

class DatabaseEntry(BaseModel):
    origin: str
    urls: list[str]


@dataclass(slots=True)
class ForumThread:
    origin: InitVar[HttpUrl]
    url: URL = field(init=False)
    name: str = field(init=False)
    page: int = field(default=1,init=False)

    def __post_init__(self, origin: HttpUrl) -> None:
        self.url = URL(str(origin))
        if not self.url.host or not any (part in self.url.parts for part in ("threads","topic")):
            raise ValueError("Invalid forum thread URL")
        self.name = self.url.name

    @property
    def as_tuple(self) -> tuple [str, str, str, int]:
        return self.url.host, self.url.path, self.name, self.page # type: ignore

def save_data(data: DatabaseEntry) -> None:
    forum_thread = ForumThread(data.origin)  # type: ignore
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    date_received = datetime.now(UTC).isoformat()
    query = "INSERT OR IGNORE INTO forum_threads (host, path, name, page, url, date) VALUES (?, ?, ?, ?, ?, ?)"
    for url in data.urls:
        cursor.execute(query,(*forum_thread.as_tuple, url, date_received))
    conn.commit()
    conn.close()


def get_urls_by_origin(origin: HttpUrl, page: int | None = None):
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    forum_thread = ForumThread(origin)
    data = forum_thread.as_tuple[0:3]
    query = "SELECT url FROM forum_threads WHERE host = ? AND path = ? AND name = ?"
    if page:
        query += " AND page = ?"
        data = forum_thread.as_tuple

    cursor.execute(query, (*data,))
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]


@app.post("/submit")
async def submit_data(data: DatabaseEntry):
    try:
        save_data(data)
        return {"message": "Data received and saved successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from None

@app.get("/retrieve")
async def retrieve_data(origin: HttpUrl, page: int | None = Query(1, ge=1)):
    try:
        urls = get_urls_by_origin(origin, page)
        return {"urls": urls}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from None


def init_db():
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS forum_threads (
            host TEXT,
            path TEXT,
            name TEXT,
            page NUMBER,
            url TEXT,
            date TEXT,
            UNIQUE(host, path, url)
        )
    """)
    conn.commit()
    conn.close()


init_db()
