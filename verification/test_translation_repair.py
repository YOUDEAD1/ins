"""Real CMS/product flows with fake HTTP responses; no translation service is contacted."""
from concurrent.futures import ThreadPoolExecutor
import html
import json
import threading
from unittest.mock import Mock

import pytest
import requests

from harness import MESSAGES
from test_integration import mod, isolate
from test_client_diagnostics import message


class Response:
    def __init__(self, text='', status=200):
        self.body = text.encode('utf-8')
        self.status_code = status
        self.closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError('Fixture HTTP failure', response=self)

    def iter_content(self, chunk_size):
        for index in range(0, len(self.body), chunk_size):
            yield self.body[index:index + chunk_size]

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def clear_translator(mod, isolate):
    engine = mod._translation_service
    def reset():
        with engine.lock:
            engine.cache.clear()
            engine.cache_bytes = 0
            engine.retry_at = 0
            engine.last_failure = None
        assert not engine.inflight
    reset()
    yield engine
    reset()


def google(mod, monkeypatch, mapping):
    responses = []
    def get(url, *, params, timeout, stream):
        assert timeout == (2.5, 4) and stream is True
        source = params['q']
        translated = '\n'.join(mapping.get(line, line) for line in source.split('\n'))
        response = Response('<div class="result-container">' + html.escape(translated) + '</div>')
        responses.append(response)
        return response
    mock = Mock(side_effect=get)
    monkeypatch.setattr(mod.requests, 'get', mock)
    return mock, responses


@pytest.mark.parametrize('ar,en', [
    ('حاول لاحقًا', 'Please try again later'),
    ('الخدمة غير متاحة', 'Service unavailable'),
    ('خطأ في الخادم', 'Server error'),
])
def test_legitimate_error_messages_translate_and_save_in_the_existing_cms(mod, monkeypatch, ar, en):
    transport, responses = google(mod, monkeypatch, {ar: en})
    mod.ad_save_custom_text(message(mod, ar), 'shop_search_prompt')
    assert mod.db.custom_texts.find_one({'lang': 'ar', 'key': 'shop_search_prompt'})['value'] == ar
    assert mod.db.custom_texts.find_one({'lang': 'en', 'key': 'shop_search_prompt'})['value'] == en
    assert not any('تعذّرت الترجمة' in text for _, text, _ in MESSAGES)
    assert transport.call_count == 1 and all(r.closed for r in responses)


def test_cms_keeps_premium_emojis_placeholders_links_and_blank_lines(mod, monkeypatch):
    source = ('<tg-emoji emoji-id="5271619747891388291">✨</tg-emoji> <b>ابحث عن المنتج</b>\n\n'
              'الرصيد {0} <a href="https://example.test/store?q=1&amp;x=2">المتجر</a> '
              '<code>مثال عربي</code>')
    transport, _ = google(mod, monkeypatch, {'ابحث عن المنتج': 'Search products',
                                            'الرصيد': 'Balance', 'المتجر': 'Store'})
    translated = mod.safe_translate_for_cms(source, 'en')
    assert translated == source.replace('ابحث عن المنتج', 'Search products').replace('الرصيد', 'Balance').replace('>المتجر<', '>Store<')
    assert '<code>مثال عربي</code>' in translated and '{0}' in translated
    assert mod._cms_arabic_count(translated) == 0
    assert transport.call_count == 2  # one request per nonempty line, not per HTML fragment


def test_rich_product_description_uses_one_batch_without_translating_protected_tokens(mod, monkeypatch):
    source = '<b>وصف المنتج</b> {name}\n<tg-emoji emoji-id="123">ب</tg-emoji> <i>متوفر الآن</i> <code>مثال</code>'
    transport, _ = google(mod, monkeypatch, {'وصف المنتج': 'Product description', 'متوفر الآن': 'Available now'})
    translated = mod._translate_product_description(source)
    assert translated == source.replace('وصف المنتج', 'Product description').replace('متوفر الآن', 'Available now')
    assert transport.call_count == 1
    sent = transport.call_args.kwargs['params']['q']
    assert '{name}' not in sent and '<tg-emoji' not in sent and 'مثال' not in sent


def test_library_compatibility_routes_button_translation_through_the_bounded_client(mod, monkeypatch):
    transport, _ = google(mod, monkeypatch, {'اشتر الآن': 'Buy now'})
    assert mod.GoogleTranslator(source='auto', target='en').translate('اشتر الآن') == 'Buy now'
    assert mod._translate_text_raw('اشتر الآن', 'en') == 'Buy now'
    assert 'en' in mod.GoogleTranslator().get_supported_languages(as_dict=True).values()
    assert transport.call_count == 1


def test_structured_fallback_recovers_without_curl_or_background_threads(mod, monkeypatch):
    denied = Response('Too many requests', 429)
    success = Response(json.dumps([[['Welcome', 'مرحبا', None, None]]]))
    transport = Mock(side_effect=[denied, success])
    monkeypatch.setattr(mod.requests, 'get', transport)
    assert mod._translate_text_raw('مرحبا', 'en') == 'Welcome'
    assert transport.call_count == 2
    assert denied.closed and success.closed
    assert mod._translation_service.last_failure is None


def test_outage_does_not_multiply_requests_and_recovery_is_possible(mod, monkeypatch):
    transport = Mock(side_effect=lambda *a, **kw: Response('Unavailable', 503))
    monkeypatch.setattr(mod.requests, 'get', transport)
    assert mod._translate_text_raw('مرحبا', 'en') is None
    for value in ('مرحبا', 'نص آخر', 'المنتجات', 'ابحث هنا'):
        assert mod._translate_text_raw(value, 'en') is None
    assert transport.call_count == 2
    assert mod._translation_service.last_failure == 'http_503'
    mod._translation_service.retry_at -= 60
    recovered, _ = google(mod, monkeypatch, {'مرحبا': 'Welcome'})
    assert mod._translate_text_raw('مرحبا', 'en') == 'Welcome'
    assert recovered.call_count == 1


def test_cached_translation_stays_available_during_an_outage(mod, monkeypatch):
    google(mod, monkeypatch, {'مرحبا': 'Welcome'})
    assert mod._translate_text_raw('مرحبا', 'en') == 'Welcome'
    failed = Mock(side_effect=lambda *a, **kw: Response('Unavailable', 503))
    monkeypatch.setattr(mod.requests, 'get', failed)
    assert mod._translate_text_raw('نص جديد', 'en') is None
    assert mod._translate_text_raw('مرحبا', 'en') == 'Welcome'
    assert failed.call_count == 2


def test_concurrent_identical_requests_share_one_http_call(mod, monkeypatch):
    started, release = threading.Event(), threading.Event()
    def get(*args, **kwargs):
        started.set()
        assert release.wait(3)
        return Response('<div class="result-container">Welcome</div>')
    transport = Mock(side_effect=get)
    monkeypatch.setattr(mod.requests, 'get', transport)
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(mod._translate_text_raw, 'مرحبا', 'en') for _ in range(8)]
        assert started.wait(3)
        release.set()
        assert [f.result(timeout=3) for f in futures] == ['Welcome'] * 8
    assert transport.call_count == 1


@pytest.mark.parametrize('body', ['<html><title>Error 500</title></html>',
                                 '<!doctype html><div>captcha required</div>',
                                 '{"error":"unavailable"}', ''])
def test_invalid_provider_responses_never_replace_saved_english(mod, monkeypatch, body):
    mod.db.custom_texts.insert_one({'lang': 'en', 'key': 'shop_search_prompt', 'value': 'Previous English'})
    transport = Mock(side_effect=lambda *a, **kw: Response(body))
    monkeypatch.setattr(mod.requests, 'get', transport)
    mod.ad_save_custom_text(message(mod, 'ابحث عن المنتجات'), 'shop_search_prompt')
    assert mod.db.custom_texts.find_one({'lang': 'en', 'key': 'shop_search_prompt'})['value'] == 'Previous English'
    assert transport.call_count == 2


def test_untranslated_arabic_is_not_accepted_as_english(mod, monkeypatch):
    transport, _ = google(mod, monkeypatch, {})
    assert mod._translate_text_raw('لم يترجم هذا النص', 'en') is None
    assert transport.call_count == 2


def test_oversized_provider_response_is_bounded_and_closed(mod, monkeypatch):
    responses = []
    def get(*a, **kw):
        r = Response('x' * (mod._translation_service.RESPONSE_LIMIT + 8192))
        responses.append(r)
        return r
    monkeypatch.setattr(mod.requests, 'get', get)
    assert mod._translate_text_raw('مرحبا', 'en') is None
    assert len(responses) == 2 and all(r.closed for r in responses)
    assert mod._translation_service.last_failure == 'TranslationUnavailable'


def test_product_translation_outage_keeps_the_existing_english(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, 'get', lambda *a, **kw: Response('Unavailable', 503))
    assert mod._translate_product_description('<b>الوصف العربي</b> {0}') is None
    assert mod._translate_product_description('<b>English</b> <code>مثال</code>') == '<b>English</b> <code>مثال</code>'


def test_cache_has_fixed_entry_and_byte_limits(mod, monkeypatch):
    google(mod, monkeypatch, {})
    service = mod._translation_service
    for index in range(service.CACHE_LIMIT + 10):
        assert service.plain('English fixture ' + str(index), 'en')
    assert len(service.cache) == service.CACHE_LIMIT
    assert service.cache_bytes <= service.CACHE_BYTES


def test_original_functions_and_verification_alert_fix_remain_available(mod):
    service = mod._translation_service
    assert callable(service.original['_translate_text_raw'])
    assert callable(service.original['_translate_single_line'])
    assert callable(service.original['_translate_product_description'])
    assert 'please try again later' in service.original['_BAD_TR_MARKERS']


def test_html_entities_and_protected_only_text_do_not_break_translation(mod, monkeypatch):
    transport, _ = google(mod, monkeypatch, {'مرحبا': 'Welcome'})
    source = '&nbsp;<b>مرحبا</b>&amp;<code>مثال</code>'
    assert mod.safe_translate_for_cms(source, 'en') == source.replace('مرحبا', 'Welcome')
    assert transport.call_count == 1


def test_english_to_arabic_uses_the_same_service(mod, monkeypatch):
    transport, _ = google(mod, monkeypatch, {'Search products': 'ابحث عن المنتجات'})
    assert mod.safe_translate_for_cms('<b>Search products</b> {0}', 'ar') == '<b>ابحث عن المنتجات</b> {0}'
    assert transport.call_count == 1


def test_batch_line_mismatch_falls_back_without_moving_text_between_tags(mod, monkeypatch):
    mapping = {'مرحبا\nالمتجر': 'Combined translation', 'مرحبا': 'Welcome', 'المتجر': 'Store'}
    def get(*args, **kwargs):
        value = mapping[kwargs['params']['q']]
        return Response('<div class="result-container">' + value + '</div>')
    transport = Mock(side_effect=get)
    monkeypatch.setattr(mod.requests, 'get', transport)
    assert mod._translate_single_line('<b>مرحبا</b> <i>المتجر</i>', 'en') == '<b>Welcome</b> <i>Store</i>'
    assert transport.call_count == 3
