import sqlite3
from dataclasses import InitVar, dataclass, field
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, HttpUrl
from yarl import URL

app = FastAPI()

class DatabaseEntry(BaseModel):
    origin: HttpUrl
    urls: list[str]

THREAD_PARTS = "threads","topic"
@dataclass(slots=True)
class ForumThread:
    origin: InitVar[HttpUrl]
    url: URL = field(init=False)
    name: str = field(init=False)
    page: int = field(default=1,init=False)
    thread_path: str = field(default="/", init=False)
    _id: int= field(init=False)
    post_number: int| None = field(default=None,init=False)

    def __post_init__(self, origin: HttpUrl) -> None:
        self.url = URL(str(origin))
        if not self.url.host or not any (part in self.url.parts for part in THREAD_PARTS):
            raise ValueError("Invalid forum thread URL")

        found_part = next(part for part in THREAD_PARTS if part in self.url.parts)
        name_index = self.url.parts.index(found_part) + 1
        name = self.url.parts[name_index]
        if "." not in name:
            raise ValueError("Invalid forum thread URL")

        post_sections = {self.url.fragment, *self.url.parts}
        post_string = next((sec for sec in post_sections if "post-" in sec), None)
        if post_string:
            self.post_number = int(post_string.replace("post-","").strip())

        if len(self.url.parts) > name_index + 1 and "page-" in self.url.parts[name_index+1]:
            self.page = int(self.url.parts[name_index+1].replace("page-","").strip())

        self.name, _id = name.rsplit(".")
        self.name = self.name.strip()
        self._id = int(_id.strip())
        self.thread_path = "/" + "/".join(self.url.parts[1:name_index+1])

    @property
    def as_tuple(self) -> tuple [str, str, str, int]:
        return self.url.host, self.url.path, self.name, self.page # type: ignore

def save_data(data: DatabaseEntry) -> None:
    forum_thread = ForumThread(data.origin)
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
            post NUMBER,
            url TEXT,
            date TEXT,
            UNIQUE(host, path, url)
        )
    """)
    conn.commit()
    conn.close()


init_db()
