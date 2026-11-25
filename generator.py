"""Генератор синтетических событий: реалистичные сессии просмотра.

Сессия: [SEARCHED] -> VIEW_STARTED -> (VIEW_PAUSED -> VIEW_RESUMED)* -> [VIEW_FINISHED -> [LIKED]]
progress_seconds растёт от события к событию, VIEW_FINISHED всегда идёт после VIEW_STARTED.

Два режима:
  * backfill: один раз при первом запуске создаёт историю за GENERATOR_BACKFILL_DAYS дней
    (нужна, чтобы retention и когорты были непустыми);
  * live: непрерывно эмулирует сессии с текущими временными метками.
"""
import logging
import os
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from models import DeviceType, EventType as ET, MovieEvent

log = logging.getLogger("generator")

N_MOVIES = 40
N_USERS = int(os.getenv("GENERATOR_USERS", "300"))
BACKFILL_DAYS = int(os.getenv("GENERATOR_BACKFILL_DAYS", "9"))
EVENTS_PER_SECOND = float(os.getenv("GENERATOR_EVENTS_PER_SECOND", "2"))
MARKER = os.getenv("GENERATOR_MARKER", "/data/backfill.done")
SEED = 42
DEVICES = list(DeviceType)
MOVIE_WEIGHTS = [1 / (i + 1) ** 0.8 for i in range(N_MOVIES)]  # часть фильмов популярнее


def movie_id(i: int) -> str:
    return f"movie-{i:03d}"


def movie_duration(i: int) -> int:
    return 5400 + (i * 97) % 3600  # 90..150 минут


def session_steps(rng: random.Random, finish_prob=0.7, search_prob=0.5):
    """Генерирует шаги одной сессии: (задержка_с, тип, movie_id, progress_seconds)."""
    idx = rng.choices(range(N_MOVIES), weights=MOVIE_WEIGHTS)[0]
    mid, dur = movie_id(idx), movie_duration(idx)
    delay = 0
    if rng.random() < search_prob:
        yield (0, ET.SEARCHED, "", 0)
        delay = rng.randint(5, 40)
    p = 0
    yield (delay, ET.VIEW_STARTED, mid, 0)
    for _ in range(rng.choice([0, 0, 1, 1, 2, 3])):
        step = rng.randint(120, 1500)
        if p + step >= dur - 60:
            break
        p += step
        yield (step, ET.VIEW_PAUSED, mid, p)
        yield (rng.randint(10, 300), ET.VIEW_RESUMED, mid, p)
    if rng.random() < finish_prob:
        yield (dur - p, ET.VIEW_FINISHED, mid, dur)
        if rng.random() < 0.4:
            yield (rng.randint(2, 20), ET.LIKED, mid, dur)
    # иначе зритель бросил просмотр: VIEW_FINISHED не будет


class SimUser:
    def __init__(self, n: int, rng: random.Random, days: int):
        self.id = f"user-{n:04d}"
        self.device = rng.choice(DEVICES)
        self.signup_ago = rng.randint(0, days)  # сколько дней назад пользователь появился


def build_users(rng: random.Random):
    return [SimUser(i, rng, BACKFILL_DAYS) for i in range(N_USERS)]


def is_active(user: SimUser, days_ago: int, rng: random.Random) -> bool:
    """Активен ли пользователь в этот день: в день регистрации всегда, потом всё реже."""
    age = user.signup_ago - days_ago
    if age < 0:
        return False
    if age == 0:
        return True
    return rng.random() < 0.05 + 0.5 * 0.8 ** (age - 1)  # D1 ~55%, D7 ~22%


def make_event(rng, user, session_id, et, mid, progress, ts) -> MovieEvent:
    return MovieEvent(
        event_id=uuid.UUID(int=rng.getrandbits(128), version=4),
        user_id=user.id, movie_id=mid, event_type=et, timestamp=ts,
        device_type=user.device, session_id=session_id, progress_seconds=progress,
    )


def backfill(publish) -> int:
    rng = random.Random(SEED)
    users = build_users(rng)
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    total = 0
    for days_ago in range(BACKFILL_DAYS, 0, -1):
        day = today - timedelta(days=days_ago)
        for u in users:
            if not is_active(u, days_ago, rng):
                continue
            for _ in range(rng.choice([1, 1, 2])):
                sid = f"s-{rng.getrandbits(48):012x}"
                t = day + timedelta(hours=8, seconds=rng.randint(0, 12 * 3600))
                for delay, et, mid, prog in session_steps(rng):
                    t += timedelta(seconds=delay)
                    publish(make_event(rng, u, sid, et, mid, prog, t))
                    total += 1
    return total


def live_loop(publish, stop: threading.Event):
    rng = random.Random()
    users = build_users(random.Random(SEED))
    active = [u for u in users if is_active(u, 0, rng)] or users
    sessions = {}  # user.id -> (user, session_id, итератор шагов)
    tick = 1.0 / EVENTS_PER_SECOND
    while not stop.is_set():
        if len(sessions) < 10:
            free = [u for u in active if u.id not in sessions]
            if free:
                u = rng.choice(free)
                sessions[u.id] = (u, f"s-{uuid.uuid4().hex[:12]}", session_steps(rng))
        if sessions:
            uid = rng.choice(list(sessions))
            u, sid, steps = sessions[uid]
            try:
                _, et, mid, prog = next(steps)
            except StopIteration:
                del sessions[uid]
            else:
                publish(make_event(rng, u, sid, et, mid, prog, datetime.now(timezone.utc)))
        stop.wait(tick)


def run(publisher, stop: threading.Event):
    def publish(ev):
        try:
            publisher.publish(ev)
        except Exception as e:
            log.error("generator publish failed: %s", e)

    if BACKFILL_DAYS > 0 and not os.path.exists(MARKER):
        t0 = time.time()
        log.info("backfill started: days=%d users=%d", BACKFILL_DAYS, N_USERS)
        n = backfill(publish)
        try:
            os.makedirs(os.path.dirname(MARKER), exist_ok=True)
            open(MARKER, "w").write(datetime.now(timezone.utc).isoformat())
        except OSError as e:
            log.warning("cannot write marker: %s", e)
        log.info("backfill finished: events=%d in %.1fs", n, time.time() - t0)
    log.info("live generator started: %.1f events/s", EVENTS_PER_SECOND)
    live_loop(publish, stop)
