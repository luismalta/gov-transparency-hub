
import json
import re
import time
import logging
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from urllib.parse import urlencode

TIMEOUT = 30

# Some report endpoints prepend HTML comments and a <style> block to the JSON body.
HTML_PREFIX = re.compile(r"^\s*(?:<!--.*?-->\s*|<style>.*?</style>\s*)+", re.IGNORECASE | re.DOTALL)
MAX_RETRIES = 3
# Gateway/proxy errors (incl. Cloudflare 52x) mean the portal is overloaded or
# slow; they are transient, so retry them with a longer backoff.
GATEWAY_ERRORS = (502, 503, 504, 520, 522, 524)
GATEWAY_BACKOFF_SECONDS = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

def _decode(data: bytes) -> str:
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")

def _strip_html_prefix(text: str) -> str:
    return HTML_PREFIX.sub("", text, count=1)

def fetch(url: str, params: dict | None = None, timeout: int = TIMEOUT) -> dict:
    if params:
        url = f"{url}?{urlencode(params)}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = Request(url, headers={"Accept": "application/json", "User-Agent": "data-hub/1.0"})
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(_strip_html_prefix(_decode(resp.read())))
        except HTTPError as exc:
            # erros permanentes: não adianta tentar de novo
            if exc.code in (403, 422):
                raise
            if attempt == MAX_RETRIES:
                raise
            if exc.code in GATEWAY_ERRORS:
                wait = GATEWAY_BACKOFF_SECONDS * 2 ** (attempt - 1)
            else:
                wait = 2 ** attempt
            log.warning("  attempt %d/%d failed (%s), retrying in %ds", attempt, MAX_RETRIES, exc, wait)
            time.sleep(wait)
        except (URLError, TimeoutError) as exc:
            if attempt == MAX_RETRIES:
                raise
            wait = 2 ** attempt
            log.warning("  attempt %d/%d failed (%s), retrying in %ds", attempt, MAX_RETRIES, exc, wait)
            time.sleep(wait)
        except json.JSONDecodeError as exc:
            # body vazio ou HTML de erro — não adianta tentar de novo
            raise