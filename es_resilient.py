from elasticsearch import ApiError
log = logging.getLogger("es.resilient")

RETRYABLE = {429, 500, 502, 503, 504}
import os, json, time, logging

log = logging.getLogger("es.resilient")
RETRYABLE = {429, 500, 502, 503, 504}

def _should_retry(err):
    try:
        return getattr(err, "status", None) in RETRYABLE
    except Exception:
        return False

def _buffer_to_disk(index: str, id: str, doc: dict):
    try:
        path = f"/mnt/data/es-buffer/{index}"
        os.makedirs(path, exist_ok=True)
        with open(f"{path}/{id}.json", "w") as f:
            json.dump(doc, f)
        log.warning("Buffered ES doc index=%s id=%s to %s", index, id, path)
    except Exception as e2:
        log.error("Failed to buffer ES doc index=%s id=%s err=%s", index, id, e2)

def es_upsert_with_retry(es, *, index, id, document, max_retries=6, base_sleep=0.5):
    """
    Upsert via Update API so we can avoid HEAD/exists and PUT/index split.
    On repeated 5xx, buffer to disk and return None (do not raise).
    Returns the ES response dict when successful, else None.
    """
    sleep = base_sleep
    for attempt in range(max_retries):
        try:
            # ES 8.x client: update(..., doc=..., doc_as_upsert=True)
            return es.update(index=index, id=id, doc=document, doc_as_upsert=True)
        except ApiError as e:
            if _should_retry(e):
                log.warning("ES upsert retry status=%s attempt=%s id=%s",
                            getattr(e, "status", None), attempt+1, id)
                time.sleep(sleep); sleep = min(sleep * 2, 8.0)
                continue
            # non-retryable → buffer and stop
            log.error("ES upsert non-retryable status=%s id=%s", getattr(e, "status", None), id)
            _buffer_to_disk(index, id, document)
            return None
        except Exception as e:
            log.warning("ES upsert exception=%s attempt=%s id=%s", type(e).__name__, attempt+1, id)
            time.sleep(sleep); sleep = min(sleep * 2, 8.0)
    # exhausted retries → buffer and continue without raising
    _buffer_to_disk(index, id, document)
    return None


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
