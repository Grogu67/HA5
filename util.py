import logging
import time

log = logging.getLogger("util")


def with_retry(fn, what: str, attempts: int = 3, base_delay: float = 1.0):
    """Повторяет вызов с exponential backoff (1 с, 2 с, 4 с...). После последней попытки пробрасывает ошибку."""
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            log.warning("%s failed (attempt %d/%d): %s", what, i, attempts, e)
            if i == attempts:
                raise
            time.sleep(base_delay * 2 ** (i - 1))
