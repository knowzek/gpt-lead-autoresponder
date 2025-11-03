import time, logging
from elasticsearch import ApiError
log = logging.getLogger("es.resilient")

RETRYABLE = {429, 500, 502, 503, 504}

def _should_retry(err):
    try:
        return getattr(err, "status", None) in RETRYABLE
    except Exception:
        return False

def es_index_with_retry(es, *, index, id, document, max_retries=6, base_sleep=0.5):
    sleep = base_sleep
    for attempt in range(max_retries):
        try:
            return es.index(index=index, id=id, document=document)
        except ApiError as e:
            if _should_retry(e):
                log.warning("ES index  retryable=%s status=%s attempt=%s id=%s",
                            True, getattr(e, "status", None), attempt+1, id)
                time.sleep(sleep); sleep *= 2
                continue
            raise
        except Exception as e:
            # network hiccup → retry a few times
            log.warning("ES index exception=%s attempt=%s id=%s", type(e).__name__, attempt+1, id)
            time.sleep(sleep); sleep *= 2
    # last try
    return es.index(index=index, id=id, document=document)

def es_update_with_retry(es, *, index, id, doc, max_retries=6, base_sleep=0.5):
    sleep = base_sleep
    for attempt in range(max_retries):
        try:
            return es.update(index=index, id=id, doc=doc)
        except ApiError as e:
            if _should_retry(e):
                log.warning("ES update retry status=%s attempt=%s id=%s",
                            getattr(e, "status", None), attempt+1, id)
                time.sleep(sleep); sleep *= 2
                continue
            raise
        except Exception as e:
            log.warning("ES update exception=%s attempt=%s id=%s", type(e).__name__, attempt+1, id)
            time.sleep(sleep); sleep *= 2
    return es.update(index=index, id=id, doc=doc)

def es_head_exists_with_retry(es, *, index, id, max_retries=4, base_sleep=0.3, default=False):
    sleep = base_sleep
    for attempt in range(max_retries):
        try:
            # HEAD under the hood
            return es.exists(index=index, id=id)
        except ApiError as e:
            if _should_retry(e):
                time.sleep(sleep); sleep *= 2
                continue
            # for non-retryables, return a safe default so the pipeline keeps moving
            return default
        except Exception:
            time.sleep(sleep); sleep *= 2
    return default
