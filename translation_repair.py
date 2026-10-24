"""Bounded, cached Google translation behind the store's existing CMS API.

No background work, proxy rotation, credentials, or database migration is used.
The previous implementations remain available in ``service.original``.
"""
from collections import OrderedDict
import hashlib
import html
import json
import re
import threading
import time

from bs4 import BeautifulSoup


class TranslationUnavailable(RuntimeError):
    pass


class TranslationService:
    CACHE_LIMIT = 256
    CACHE_BYTES = 512 * 1024
    RESPONSE_LIMIT = 256 * 1024
    COOLDOWN = 30

    def __init__(self, namespace):
        self.g = namespace
        self.original = {name: namespace[name] for name in
                         ('GoogleTranslator', '_translate_text_raw', '_translate_single_line',
                          '_translate_product_description', '_BAD_TR_MARKERS')}
        self.lock = threading.Lock()
        self.slots = threading.BoundedSemaphore(2)
        self.cache = OrderedDict()
        self.cache_bytes = 0
        self.inflight = {}
        self.retry_at = 0
        self.last_failure = None

    def _cached(self, key):
        entry = self.cache.get(key)
        if entry is None:
            return None
        if entry[0] <= time.monotonic():
            self.cache_bytes -= self.cache.pop(key)[2]
            return None
        self.cache.move_to_end(key)
        return entry[1]

    def _remember(self, key, value):
        size = len(value.encode('utf-8'))
        if size > self.CACHE_BYTES:
            return
        previous = self.cache.pop(key, None)
        if previous:
            self.cache_bytes -= previous[2]
        self.cache[key] = (time.monotonic() + 3600, value, size)
        self.cache_bytes += size
        while len(self.cache) > self.CACHE_LIMIT or self.cache_bytes > self.CACHE_BYTES:
            self.cache_bytes -= self.cache.popitem(last=False)[1][2]

    def _fetch(self, url, params):
        deadline = time.monotonic() + 10
        response = self.g['requests'].get(url, params=params,
                    timeout=(2.5, 4), stream=True)
        try:
            response.raise_for_status()
            parts, size = [], 0
            for part in response.iter_content(chunk_size=8192):
                if time.monotonic() > deadline:
                    raise TranslationUnavailable('response_deadline')
                if not part:
                    continue
                size += len(part)
                if size > self.RESPONSE_LIMIT:
                    raise TranslationUnavailable('response_too_large')
                parts.append(part)
            return b''.join(parts).decode('utf-8')
        finally:
            response.close()

    def _accepted(self, source, value, target):
        return (isinstance(value, str) and not self.g['_is_bad_translation'](source, value)
                and not (target == 'en' and self.g['_has_arabic_letters'](source)
                         and self.g['_has_arabic_letters'](value)))

    def _produce(self, text, target, source):
        # The mobile page is the original library's Google transport, now with
        # explicit connect/read timeouts. A structured response is the fallback.
        providers = (
            ('https://translate.google.com/m', {'sl': source, 'tl': target, 'q': text}),
            ('https://translate.googleapis.com/translate_a/single',
             {'client': 'gtx', 'sl': source, 'tl': target, 'dt': 't', 'q': text}),
        )
        failure = 'no_translation'
        for index, (url, params) in enumerate(providers):
            try:
                body = self._fetch(url, params)
                if index == 0:
                    page = BeautifulSoup(body, 'html.parser')
                    element = (page.find('div', {'class': 'result-container'})
                               or page.find('div', {'class': 't0'}))
                    value = element.get_text() if element else None
                else:
                    payload = json.loads(body)
                    value = ''.join(row[0] for row in payload[0]
                                    if isinstance(row, list) and row and isinstance(row[0], str))
                if self._accepted(text, value, target):
                    return value.strip()
                failure = 'invalid_translation'
            except Exception as exc:
                status = getattr(getattr(exc, 'response', None), 'status_code', None)
                failure = 'http_' + str(status) if status else type(exc).__name__
        with self.lock:
            self.retry_at = time.monotonic() + self.COOLDOWN
            self.last_failure = failure
        # No source text, URL query, user data or credentials enter diagnostics.
        self.g['logger'].warning('Translation temporarily unavailable: cause=%s target=%s; retry in %ss',
                                 failure, target, self.COOLDOWN)
        return None

    def plain(self, text, target_lang='en', tries=1, *, source='auto'):
        if not isinstance(text, str) or not text.strip():
            return None
        text = text.strip()
        if len(text) > 20000:
            return None
        if source == target_lang or not re.search(r'[^\W\d_]', text):
            return text
        if len(text) > 3500:
            # Preserve every separator when joining bounded translation chunks.
            chunks, remaining = [], text
            while remaining:
                end = min(3000, len(remaining))
                if end < len(remaining):
                    boundary = max(remaining.rfind(' ', 1500, end), remaining.rfind('\n', 1500, end))
                    if boundary >= 1500:
                        end = boundary + 1
                chunk, remaining = remaining[:end], remaining[end:]
                lead = chunk[:len(chunk) - len(chunk.lstrip())]
                tail = chunk[len(chunk.rstrip()):]
                value = self.plain(chunk, target_lang, source=source)
                if value is None:
                    return None
                chunks.append(lead + value + tail)
            return ''.join(chunks)
        key = hashlib.sha256((source + '\0' + target_lang + '\0' + text).encode()).hexdigest()
        with self.lock:
            cached = self._cached(key)
            if cached is not None:
                return cached
            flight = self.inflight.get(key)
            if flight is None:
                if time.monotonic() < self.retry_at:
                    return None
                flight = {'done': threading.Event(), 'value': None}
                self.inflight[key] = flight
                leader = True
            else:
                leader = False
        if not leader:
            flight['done'].wait(14)
            return flight['value']
        acquired = self.slots.acquire(blocking=False)
        try:
            if not acquired:
                return None
            value = self._produce(text, target_lang, source)
            if value is not None:
                with self.lock:
                    self._remember(key, value)
                    self.last_failure = None
                flight['value'] = value
            return value
        finally:
            if acquired:
                self.slots.release()
            with self.lock:
                self.inflight.pop(key, None)
                flight['done'].set()

    def markup(self, text, target='en'):
        if not text or not str(text).strip():
            return text
        lines = []
        preserve = self.g['_PRESERVE_RE']
        source_lines = [preserve.split(line) for line in str(text).split('\n')]
        fragments = [html.unescape(part.strip()) for parts in source_lines for part in parts
                     if part and not preserve.fullmatch(part)
                     and re.search(r'[^\W\d_]', html.unescape(part))]
        joined = '\n'.join(fragments)
        batch = None
        if 1 < len(fragments) <= 100 and len(joined) <= 3500:
            value = self.plain(joined, target)
            if value is None:
                return text
            translated = value.split('\n')
            if (len(translated) == len(fragments)
                    and all(self._accepted(src, out, target) for src, out in zip(fragments, translated))):
                batch = iter(translated)
                with self.lock:
                    for src, out in zip(fragments, translated):
                        key = hashlib.sha256(('auto\0' + target + '\0' + src).encode()).hexdigest()
                        self._remember(key, out.strip())
        for parts in source_lines:
            output = []
            for part in parts:
                if not part:
                    continue
                if preserve.fullmatch(part) or not re.search(r'[^\W\d_]', html.unescape(part)):
                    output.append(part)
                    continue
                lead = part[:len(part) - len(part.lstrip())]
                tail = part[len(part.rstrip()):]
                value = next(batch).strip() if batch is not None else self.plain(html.unescape(part.strip()), target)
                if value is None:
                    return text
                output.append(lead + html.escape(value, quote=False) + tail)
            lines.append(''.join(output))
        return '\n'.join(lines)

    def description(self, text):
        if not text or not self.g['_cms_arabic_count'](text):
            return text
        translated = self.markup(text, 'en')
        # Protected Arabic examples, emoji fallbacks and code stay unchanged.
        return translated if not self.g['_cms_arabic_count'](translated) else None


def install(namespace):
    if namespace.get('_translation_service'):
        return namespace['_translation_service']
    service = TranslationService(namespace)
    original_class = service.original['GoogleTranslator']

    class BoundedGoogleTranslator(original_class):
        def translate(self, text, **kwargs):
            translated = service.plain(text, self._target, source=self._source)
            if translated is None:
                raise TranslationUnavailable('Translation temporarily unavailable')
            return translated

    # Common UI phrases are valid translations. HTTP status, result structure,
    # error-page signatures, markup and placeholder guards remain enforced.
    ambiguous = {'server error', 'service unavailable', 'please try again later', 'bad gateway'}
    namespace['_BAD_TR_MARKERS'] = tuple(value for value in service.original['_BAD_TR_MARKERS']
                                         if value not in ambiguous)
    namespace['GoogleTranslator'] = BoundedGoogleTranslator
    namespace['_translate_text_raw'] = service.plain
    namespace['_translate_single_line'] = service.markup
    namespace['_translate_product_description'] = service.description
    namespace['_translation_service'] = service
    return service
