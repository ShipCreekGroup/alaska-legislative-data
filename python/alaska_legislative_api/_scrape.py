import json
from collections.abc import Iterable
from pathlib import Path
from typing import Literal, TypeAlias

from alaska_legislative_api import _low

Cache: TypeAlias = bool | Literal["previous-sessions"]


def scrape(directory: str | Path, *, cache: Cache = "previous-sessions") -> None:
    """
    Scrape the Alaska Legislative API data and save it to the specified directory.

    Args:
        directory (str): The directory where the scraped data will be saved.
    """
    d = Path(directory)
    sessions_path = d / "sessions.json"
    if not cache or not sessions_path.exists():
        sessions = scrape_sessions()
        sessions_path.parent.mkdir(parents=True, exist_ok=True)
        sessions_path.write_text(_low.json.dumps(sessions, indent=4))
    else:
        sessions = json.loads(sessions_path.read_text())

    session_numbers = [int(s["Number"]) for s in sessions]

    members_path = d / "members"
    scrape_members(session_numbers, members_path, cache)


def scrape_sessions() -> list[dict]:
    """
    Get the list of available sessions.

    Returns:
        list[int]: A list of available session numbers.
    """
    session_number = 0
    entered_range = False
    sessions = []
    while True:
        session_number += 1
        try:
            s = _low.sessions(
                queries=[
                    # "laws",
                    # "journals",  # this makes each response too large
                    # "sponsors",
                    "locations",
                    "subjects",
                    "requestors",
                    "stats",
                    "housestatus",
                    "senatestatus",
                ],
                session=session_number,
            )
        except _low.DataUnimplementedError:
            if not entered_range:
                continue
            else:
                break
        except ValueError as e:
            if "Invalid Session Number" in str(e):
                break
        s = {**s, "Number": int(s["Number"])}
        sessions.append(s)
        entered_range = True
    return sessions


def scrape_members(sessions: list[int], d: Path, cache) -> list[dict]:
    result = []
    for session_number in sessions:
        p = d / f"members_{session_number}.json"
        if _need_refresh(session_number, sessions, cache, p):
            members = scrape_members_of_session(session_number)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(_low.json.dumps(members, indent=4))
        else:
            members = json.loads(p.read_text())
        result.extend(members)
    return result


def scrape_members_of_session(session_number: int) -> list[dict]:
    try:
        m = _low.members(session=session_number)
    except _low.DataUnimplementedError:
        return []
    return [{**member, "Session": session_number} for member in m]


def _is_latest(session, sessions) -> bool:
    return int(session) == max(int(s) for s in sessions)


def _need_refresh(session: int, sessions: Iterable[int], cache: Cache, path: Path):
    if not cache:
        return True
    if not path.exists():
        return True
    if cache is True:
        return False
    assert cache == "previous-sessions"
    return _is_latest(session, sessions)
