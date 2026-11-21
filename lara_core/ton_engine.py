"""Multi-Provider TON / LTC / On-Chain Deposit & Admin Check Engine.

Fixes:
1. Canonical TON address comparison (`EQ...` == `UQ...` == `0:hex...`).
2. Accepts 64-char hex, 44-char base64 (`+`/`/`/`=`), base64url, AND Tonviewer/Tonscan URLs.
3. Multi-provider TON queries (TonAPI v2 + TONCenter v3 + TONCenter v2) with aborted/bounced filtering.
4. Exact crypto-unit (`wa_pay_amount`) pending-deposit matching immune to USD price drift.
5. `[CRIT-01]` Litecoin 0-confirmation mempool/RBF protection & self-change input filtering.
6. `[MED-03]` Safe `crediting` -> `completed` state transition in `auto_credit_from_pending`.
7. Instant WebApp `/webapp/deposit/check` support for TON, LTC, USDT TRC20 & BEP20.
8. Fast, non-hanging Admin Check (`ad_check_tx`) + 1-click Wallet Diagnostics (`ad_wallet_diag` & `/ton_status`).
"""
import base64
import datetime
import functools
import html
import math
import os
import re
import struct
import threading
import time


_TON_PRICE_CACHE = {'price': None, 'exp': 0.0}
_TON_PRICE_LOCK = threading.Lock()
_HTTP_HEADERS = {'User-Agent': 'LaraStock-Verify/2.0', 'Accept': 'application/json'}


def ton_canon_addr(addr):
    """Convert any TON address (`EQ..`, `UQ..`, `kQ..`, `0Q..`, or `wc:64hex`) to `wc:64hex`."""
    s = str(addr or '').strip()
    if not s or s == 'Not Set':
        return ''
    m_raw = re.fullmatch(r'(-?\d+):([0-9a-fA-F]{64})', s)
    if m_raw:
        return f"{int(m_raw.group(1))}:{m_raw.group(2).lower()}"
    if re.fullmatch(r'[A-Za-z0-9+/_-]{48}', s):
        try:
            b64 = s.replace('-', '+').replace('_', '/')
            raw = base64.b64decode(b64)
            if len(raw) == 36:
                wc = struct.unpack('b', raw[1:2])[0]
                return f"{wc}:{raw[2:34].hex()}"
        except Exception:
            return ''
    return ''


def ton_extract_hash(raw_input):
    """Extract canonical 64-char lowercase hex hash from raw hash, base64 (incl. `/` & `+`), or Explorer URL."""
    s = str(raw_input or '').strip()
    if not s:
        return ''
    # Strip Tonviewer / Tonscan / Tonspace URL wrappers without breaking `/` inside standard Base64 hashes!
    s_low = s.lower()
    if '://' in s or any(dom in s_low for dom in ('tonviewer.', 'tonscan.', 'tonspace.', 'tonapi.', 'ton.cx')):
        s = s.split('?', 1)[0].split('#', 1)[0].strip()
        for marker in ('/transaction/', '/transactions/', '/tx/'):
            idx = s.lower().find(marker)
            if idx >= 0:
                after = s[idx + len(marker):]
                m_hex = re.match(r'^((?:0[xX])?[0-9a-fA-F]{64})(?:/|$)', after)
                if m_hex:
                    s = m_hex.group(1)
                elif re.fullmatch(r'[A-Za-z0-9+/_-]{43,44}={0,2}', after):
                    s = after
                else:
                    s = after.strip('/')
                break
        else:
            s = s.rstrip('/').rsplit('/', 1)[-1]
    s = re.sub(r'\s+', '', s)
    if re.fullmatch(r'(?:0[xX])?[0-9a-fA-F]{64}', s):
        return s[-64:].lower()
    if re.fullmatch(r'[A-Za-z0-9+/_-]{42,44}={0,2}', s):
        try:
            b = s.replace('-', '+').replace('_', '/')
            b += '=' * (-len(b) % 4)
            raw = base64.b64decode(b)
            if len(raw) == 32:
                return raw.hex()
        except Exception:
            return ''
    return ''


def _alt_hashes(*vals):
    """Other hashes the customer may paste for the same transfer: the message hash, or the
    sender-side trace/event hash that Tonkeeper and Tonviewer show."""
    out = set()
    for v in vals:
        h = ton_extract_hash(v) if v else ''
        if h:
            out.add(h)
    return out


class TonAndCheckEngine:
    def __init__(self, ns):
        self.ns = ns
        self.db = ns['db']
        self.bot = ns['bot']
        self.logger = ns['logger']

    def _toncenter_headers(self):
        hdrs = dict(_HTTP_HEADERS)
        key = (str(self.ns['get_setting']('toncenter_api_key', '') or '').strip()
               or os.getenv('TONCENTER_API_KEY', '').strip())
        if key and key != 'Not Set':
            hdrs['X-API-Key'] = key
        return hdrs

    def get_ton_price_usd(self):
        now = time.monotonic()
        with _TON_PRICE_LOCK:
            if _TON_PRICE_CACHE['price'] and _TON_PRICE_CACHE['exp'] > now:
                return _TON_PRICE_CACHE['price']

        req = self.ns['requests']
        sources = (
            ('https://data-api.binance.vision/api/v3/ticker/price?symbol=TONUSDT',
             lambda j: float(j.get('price', 0))),
            ('https://api.bybit.com/v5/market/tickers?category=spot&symbol=TONUSDT',
             lambda j: float(((j.get('result') or {}).get('list') or [{}])[0].get('lastPrice', 0))),
            ('https://www.okx.com/api/v5/market/ticker?instId=TON-USDT',
             lambda j: float((j.get('data') or [{}])[0].get('last', 0))),
            ('https://api.coinbase.com/v2/prices/TON-USD/spot',
             lambda j: float((j.get('data') or {}).get('amount', 0))),
            ('https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd',
             lambda j: float((j.get('the-open-network') or {}).get('usd', 0))),
        )
        for url, extractor in sources:
            try:
                r = req.get(url, headers=_HTTP_HEADERS, timeout=6)
                if r.status_code == 200:
                    val = extractor(r.json())
                    if math.isfinite(val) and 0.2 <= val <= 250.0:
                        with _TON_PRICE_LOCK:
                            _TON_PRICE_CACHE.update(price=val, exp=time.monotonic() + 40.0)
                        return val
            except Exception:
                continue
        return None

    def _parse_tonapi_tx(self, tx, wallet_canon):
        """Parse TonAPI v2 transaction object safely with bounce/abort rejection."""
        if not isinstance(tx, dict):
            return None
        tx_hash = ton_extract_hash(tx.get('hash', ''))
        if not tx_hash:
            return None
        if tx.get('aborted') is True:
            return None
        comp = tx.get('compute_phase') or {}
        if isinstance(comp, dict) and comp.get('skipped') is not True and comp.get('success') is False:
            return None
        in_msg = tx.get('in_msg') or {}
        if not in_msg or in_msg.get('bounced') is True:
            return None
        out_msgs = tx.get('out_msgs') or []
        if any(isinstance(m, dict) and m.get('bounced') is True for m in out_msgs):
            return None
        src_obj = in_msg.get('source') or {}
        src_raw = src_obj.get('address', '') if isinstance(src_obj, dict) else str(src_obj or '')
        dst_obj = in_msg.get('destination') or {}
        dst_raw = dst_obj.get('address', '') if isinstance(dst_obj, dict) else str(dst_obj or '')
        src_canon = ton_canon_addr(src_raw)
        dst_canon = ton_canon_addr(dst_raw)
        if wallet_canon:
            if src_canon and src_canon == wallet_canon:
                return {'is_sender': True, 'hash': tx_hash, 'utime': int(tx.get('utime') or 0)}
            if dst_canon and dst_canon != wallet_canon:
                return None
        val_nano = int(in_msg.get('value') or 0)
        out_nano = sum(int(m.get('value') or 0) for m in out_msgs if isinstance(m, dict))
        net_nano = val_nano - out_nano if out_nano > 0 else val_nano
        if net_nano <= 0:
            return None
        comment = str(in_msg.get('decoded_body', {}).get('text', '') if isinstance(in_msg.get('decoded_body'), dict) else '')
        return {
            'hash': tx_hash,
            'received_ton': net_nano / 1_000_000_000,
            'utime': int(tx.get('utime') or 0),
            'source_addr': src_raw,
            'comment': comment,
            'provider': 'tonapi_v2',
            'alt': _alt_hashes(in_msg.get('hash'), tx.get('trace_id')),
        }

    def _parse_toncenter_v3_tx(self, tx, wallet_canon):
        if not isinstance(tx, dict):
            return None
        tx_hash = ton_extract_hash(tx.get('hash', ''))
        if not tx_hash:
            return None
        desc = tx.get('description') or {}
        if desc.get('aborted') is True:
            return None
        cph = desc.get('compute_ph') or {}
        if isinstance(cph, dict) and cph.get('skipped') is not True and cph.get('success') is False:
            return None
        in_msg = tx.get('in_msg') or {}
        if not in_msg or in_msg.get('bounced') is True:
            return None
        out_msgs = tx.get('out_msgs') or []
        if any(isinstance(m, dict) and m.get('bounced') is True for m in out_msgs):
            return None
        src_raw = str(in_msg.get('source') or '')
        dst_raw = str(in_msg.get('destination') or '')
        src_canon = ton_canon_addr(src_raw)
        dst_canon = ton_canon_addr(dst_raw)
        if wallet_canon:
            if src_canon and src_canon == wallet_canon:
                return {'is_sender': True, 'hash': tx_hash, 'utime': int(tx.get('now') or 0)}
            if dst_canon and dst_canon != wallet_canon:
                return None
        val_nano = int(in_msg.get('value') or 0)
        out_nano = sum(int(m.get('value') or 0) for m in out_msgs if isinstance(m, dict))
        net_nano = val_nano - out_nano if out_nano > 0 else val_nano
        if net_nano <= 0:
            return None
        return {
            'hash': tx_hash,
            'received_ton': net_nano / 1_000_000_000,
            'utime': int(tx.get('now') or 0),
            'source_addr': src_raw,
            'comment': '',
            'provider': 'toncenter_v3',
            'alt': _alt_hashes(in_msg.get('hash'), in_msg.get('hash_norm'), tx.get('trace_id')),
        }

    def _parse_toncenter_v2_tx(self, tx, wallet_canon):
        if not isinstance(tx, dict):
            return None
        tx_hash = ton_extract_hash((tx.get('transaction_id') or {}).get('hash', ''))
        if not tx_hash:
            return None
        in_msg = tx.get('in_msg') or {}
        if not in_msg:
            return None
        out_msgs = tx.get('out_msgs') or []
        # If out_msgs bounced value back to sender, reject
        src_raw = str(in_msg.get('source') or '')
        dst_raw = str(in_msg.get('destination') or '')
        src_canon = ton_canon_addr(src_raw)
        dst_canon = ton_canon_addr(dst_raw)
        if wallet_canon:
            if src_canon and src_canon == wallet_canon:
                return {'is_sender': True, 'hash': tx_hash, 'utime': int(tx.get('utime') or 0)}
            if dst_canon and dst_canon != wallet_canon:
                return None
        val_nano = int(in_msg.get('value') or 0)
        out_nano = sum(int(m.get('value') or 0) for m in out_msgs if isinstance(m, dict))
        net_nano = val_nano - out_nano if out_nano > 0 else val_nano
        if net_nano <= 0:
            return None
        return {
            'hash': tx_hash,
            'received_ton': net_nano / 1_000_000_000,
            'utime': int(tx.get('utime') or 0),
            'source_addr': src_raw,
            'comment': str(in_msg.get('message') or ''),
            'provider': 'toncenter_v2',
            'alt': _alt_hashes(in_msg.get('hash'), in_msg.get('msg_hash')),
        }

    def fetch_wallet_txs(self, wallet_address, limit=40):
        """Fetch incoming TON txs across TonAPI v2 -> TONCenter v3 -> TONCenter v2."""
        wallet_canon = ton_canon_addr(wallet_address)
        if not wallet_canon:
            return []
        req = self.ns['requests']
        hdrs = self._toncenter_headers()

        # 1) TonAPI v2 (Fast, reliable, rich bounce/abort metadata)
        try:
            r = req.get(
                f"https://tonapi.io/v2/blockchain/accounts/{wallet_address}/transactions",
                params={'limit': min(100, max(10, int(limit)))},
                headers=_HTTP_HEADERS,
                timeout=10,
            )
            if r.status_code == 200:
                rows = []
                for tx in r.json().get('transactions', []):
                    parsed = self._parse_tonapi_tx(tx, wallet_canon)
                    if parsed and not parsed.get('is_sender'):
                        rows.append(parsed)
                return rows
        except Exception as exc:
            self.logger.debug('TonAPI wallet list fallback: %s', exc)

        # 2) TONCenter v3
        try:
            r = req.get(
                'https://toncenter.com/api/v3/transactions',
                params={'account': wallet_address, 'limit': min(100, max(10, int(limit))), 'sort': 'desc'},
                headers=hdrs,
                timeout=10,
            )
            if r.status_code == 200:
                rows = []
                for tx in r.json().get('transactions', []):
                    parsed = self._parse_toncenter_v3_tx(tx, wallet_canon)
                    if parsed and not parsed.get('is_sender'):
                        rows.append(parsed)
                return rows
        except Exception as exc:
            self.logger.debug('TONCenter v3 wallet list fallback: %s', exc)

        # 3) TONCenter v2
        try:
            r = req.get(
                'https://toncenter.com/api/v2/getTransactions',
                params={'address': wallet_address, 'limit': min(60, max(10, int(limit)))},
                headers=hdrs,
                timeout=10,
            )
            if r.status_code == 200:
                body = r.json()
                if body.get('ok'):
                    rows = []
                    for tx in body.get('result', []):
                        parsed = self._parse_toncenter_v2_tx(tx, wallet_canon)
                        if parsed and not parsed.get('is_sender'):
                            rows.append(parsed)
                    return rows
        except Exception as exc:
            self.logger.debug('TONCenter v2 wallet list error: %s', exc)

        return []

    def lookup_tx_by_hash(self, query_or_hash, wallet_address):
        """Find a specific TON transaction by hash/base64/URL across direct & account endpoints."""
        wallet_canon = ton_canon_addr(wallet_address)
        if not wallet_canon:
            return None
        canon_hex = ton_extract_hash(query_or_hash)
        req = self.ns['requests']
        hdrs = self._toncenter_headers()

        # 1) Direct hash lookup via TonAPI v2 (works regardless of wallet tx depth!)
        if canon_hex:
            try:
                r = req.get(
                    f"https://tonapi.io/v2/blockchain/transactions/{canon_hex}",
                    headers=_HTTP_HEADERS,
                    timeout=10,
                )
                if r.status_code == 200:
                    parsed = self._parse_tonapi_tx(r.json(), wallet_canon)
                    if parsed:
                        return parsed
            except Exception as exc:
                self.logger.debug('TonAPI direct tx lookup fallback: %s', exc)

            # 2) Direct hash lookup via TONCenter v3
            try:
                r = req.get(
                    'https://toncenter.com/api/v3/transactions',
                    params={'hash': canon_hex, 'limit': 5},
                    headers=hdrs,
                    timeout=10,
                )
                if r.status_code == 200:
                    for tx in r.json().get('transactions', []):
                        if ton_extract_hash(tx.get('hash', '')) == canon_hex:
                            parsed = self._parse_toncenter_v3_tx(tx, wallet_canon)
                            if parsed:
                                return parsed
            except Exception as exc:
                self.logger.debug('TONCenter v3 direct tx lookup fallback: %s', exc)

            # 3) The customer pasted the SENDER-side hash (Tonkeeper / Tonviewer show the sender's
            #    transaction or event) or the message hash: follow the trace to OUR incoming transaction.
            try:
                r = req.get(f"https://tonapi.io/v2/traces/{canon_hex}", headers=_HTTP_HEADERS, timeout=10)
                if r.status_code == 200:
                    stack = [r.json() or {}]
                    while stack:
                        node = stack.pop()
                        if not isinstance(node, dict):
                            continue
                        tx = node.get('transaction') or {}
                        acct = (tx.get('account') or {}).get('address', '') if isinstance(tx.get('account'), dict) else ''
                        if acct and ton_canon_addr(acct) == wallet_canon:
                            parsed = self._parse_tonapi_tx(tx, wallet_canon)
                            if parsed:
                                return parsed
                        stack.extend(node.get('children') or [])
            except Exception as exc:
                self.logger.debug('TonAPI trace lookup fallback: %s', exc)
            try:
                r = req.get('https://toncenter.com/api/v3/transactionsByMessage',
                            params={'msg_hash': canon_hex, 'limit': 10}, headers=hdrs, timeout=10)
                if r.status_code == 200:
                    for tx in r.json().get('transactions', []) or []:
                        if ton_canon_addr(tx.get('account', '')) == wallet_canon:
                            parsed = self._parse_toncenter_v3_tx(tx, wallet_canon)
                            if parsed:
                                return parsed
            except Exception as exc:
                self.logger.debug('TONCenter v3 message lookup fallback: %s', exc)

        # 4) Account recent history scan: our tx hash, its message hash, or the sender trace id
        q_clean = str(query_or_hash or '').strip().lower()
        for kind, provider_fn in (
            ('tonapi', lambda: req.get(
                f"https://tonapi.io/v2/blockchain/accounts/{wallet_address}/transactions",
                params={'limit': 100}, headers=_HTTP_HEADERS, timeout=12
            )),
            ('v3', lambda: req.get(
                'https://toncenter.com/api/v3/transactions',
                params={'account': wallet_address, 'limit': 100, 'sort': 'desc'}, headers=hdrs, timeout=12
            )),
            ('v2', lambda: req.get(
                'https://toncenter.com/api/v2/getTransactions',
                params={'address': wallet_address, 'limit': 60}, headers=hdrs, timeout=12
            )),
        ):
            try:
                r = provider_fn()
                if r.status_code != 200:
                    continue
                body = r.json()
                tx_list = body.get('transactions') if 'transactions' in body else (body.get('result') or [])
                for tx in tx_list or []:
                    parsed = (self._parse_toncenter_v2_tx(tx, wallet_canon) if kind == 'v2' else
                              self._parse_toncenter_v3_tx(tx, wallet_canon) if kind == 'v3' else
                              self._parse_tonapi_tx(tx, wallet_canon))
                    if not parsed:
                        continue
                    h = parsed['hash']
                    if (canon_hex and (h == canon_hex or canon_hex in (parsed.get('alt') or ()))) or \
                            (not canon_hex and len(q_clean) >= 12 and q_clean in h):
                        return parsed
            except Exception:
                continue
        return None

    def match_pending_crypto(self, received_crypto, usd_amount, coin, *, crypto_tol):
        """Match pending TON/LTC deposit by exact quoted crypto quantity first, then USD fallback."""
        now = int(time.time())
        records = list(self.db.pending_deposits.find({
            'coin': coin,
            'status': 'pending',
            'expires_at': {'$gt': now},
        }))
        if not records:
            return None

        # 1) Exact match against quoted `wa_pay_amount` (immune to TON/LTC market price ticks!)
        candidates = []
        for rec in records:
            raw_pay = rec.get('wa_pay_amount') or rec.get('expected_crypto')
            try:
                expected = float(raw_pay) if raw_pay is not None else 0.0
            except (TypeError, ValueError):
                expected = 0.0
            if expected > 0 and math.isfinite(expected):
                diff = abs(expected - float(received_crypto))
                if diff <= crypto_tol:
                    candidates.append((diff, rec))

        if len(candidates) == 1:
            return candidates[0][1]
        if len(candidates) > 1:
            candidates.sort(key=lambda item: (item[0], item[1].get('created_at', 0)))
            if (candidates[1][0] - candidates[0][0]) >= (crypto_tol * 0.4):
                return candidates[0][1]
            self.ns['_alert_ambiguous_deposit'](coin, usd_amount, [c[1] for c in candidates])
            return None

        # 2) Fallback to USD amount matching only for legacy records lacking wa_pay_amount
        return self.ns['find_pending_deposit_for_amount'](usd_amount, coin, tolerance=0.015)

    def safe_auto_credit(self, pending, tx_id_for_record, method_label, actual_usd=None):
        """Fix [MED-03]: Claim into `crediting` and mark `completed` only if `credit_user` succeeds."""
        try:
            uid = pending['user_id']
            requested_amount = float(pending.get('base_amount_usd', 0))
            pending_id = pending.get('pending_id', '')
            if actual_usd is not None and float(actual_usd) > 0:
                base_amount = round(float(actual_usd), 2)
            else:
                base_amount = requested_amount
            shortfall = (actual_usd is not None and round(float(actual_usd), 2) < requested_amount - 0.01)

            claimed = self.db.pending_deposits.find_one_and_update(
                {'pending_id': pending_id, 'status': 'pending'},
                {'$set': {
                    'status': 'crediting',
                    'crediting_at': int(time.time()),
                    'tx_id_detected': tx_id_for_record,
                    'credited_usd': base_amount,
                }},
            )
            if not claimed:
                return False

            self.ns['unlock_deposit'](uid)
            try:
                self.ns['PENDING_DEPOSIT_USERS'].pop(uid, None)
            except Exception:
                pass

            try:
                shortfall_note = self.ns['_notification_text'](uid, 'deposit_shortfall_dm', requested_amount) if shortfall else ''
                msg = self.ns['_notification_text'](
                    uid, 'deposit_detected_dm', base_amount,
                    html.escape(str(method_label)), html.escape(str(tx_id_for_record)), shortfall_note
                )
                self.bot.send_message(uid, msg, parse_mode='HTML')
            except Exception:
                pass

            credited = self.ns['credit_user'](
                uid, base_amount, tx_id_for_record, self.ns['get_lang'](uid), method_label, trusted_txid=True
            )
            used_ok = bool(credited) or bool(self.db.used_transactions.find_one({'transaction_id': tx_id_for_record}, {'_id': 1}))
            self.db.pending_deposits.update_one(
                {'pending_id': pending_id},
                {'$set': {
                    'status': 'completed' if used_ok else 'needs_review',
                    'completed_at': int(time.time()),
                    'credited_amount_usd': base_amount if used_ok else 0.0,
                }}
            )
            return used_ok
        except Exception as exc:
            self.logger.error('safe_auto_credit error: %s', exc)
            return False

    def check_ton_blockchain_auto(self):
        """Multi-provider automatic TON deposit detector."""
        try:
            wallet_address = self.ns['get_setting']('ton_address')
            if not wallet_address or wallet_address == 'Not Set':
                return
            now = int(time.time())
            pending_list = list(self.db.pending_deposits.find({
                'coin': 'TON',
                'status': 'pending',
                'expires_at': {'$gt': now},
            }))
            if not pending_list:
                return

            earliest_created = min(int(p.get('created_at') or now) for p in pending_list)
            cutoff_time = min(now - 45 * 60, earliest_created - 120)

            txs = self.fetch_wallet_txs(wallet_address, limit=40)
            if not txs:
                return

            ton_price = self.get_ton_price_usd()
            if not ton_price or ton_price < 0.2:
                return

            for tx in txs:
                try:
                    canon_hex = tx['hash']
                    tx_time = int(tx.get('utime') or 0)
                    if tx_time <= 0 or tx_time < cutoff_time:
                        continue
                    tx_id_normalized = 'ton' + canon_hex
                    all_keys = list(self.ns['_ton_tx_keys'](canon_hex))
                    if self.db.used_transactions.find_one({'transaction_id': {'$in': all_keys}}, {'_id': 1}):
                        continue

                    received_ton = float(tx['received_ton'])
                    if received_ton <= 0:
                        continue
                    usd_amount = round(received_ton * ton_price, 6)

                    pending = self.match_pending_crypto(received_ton, usd_amount, 'TON', crypto_tol=0.00025)
                    if pending:
                        if int(tx_time) < int(pending.get('created_at', 0)) - 120:
                            continue
                        if self.safe_auto_credit(pending, tx_id_normalized, 'Toncoin (TON) Auto'):
                            self.logger.info('✅ AUTO-CREDITED TON: user %s -> $%.2f (%s)',
                                             pending['user_id'], pending['base_amount_usd'], canon_hex[:16])
                except Exception as tx_err:
                    self.logger.debug('Skip TON auto tx: %s', tx_err)
        except Exception as exc:
            self.logger.error('check_ton_blockchain_auto error: %s', exc)

    def check_ltc_blockchain_auto(self):
        """Hardened LTC auto-detector: enforces >=1 confirmation ([CRIT-01]) & rejects self-change."""
        try:
            wallet_address = self.ns['get_setting']('ltc_address')
            if not wallet_address or wallet_address == 'Not Set':
                return
            now = int(time.time())
            pending_list = list(self.db.pending_deposits.find({
                'coin': 'LTC',
                'status': 'pending',
                'expires_at': {'$gt': now},
            }))
            if not pending_list:
                return

            url = f"https://litecoinspace.org/api/address/{wallet_address}/txs"
            try:
                res = self.ns['requests'].get(url, headers=_HTTP_HEADERS, timeout=10)
                if res.status_code != 200:
                    return
                txs = res.json()
            except Exception:
                return

            ltc_price = self.ns['get_ltc_price_usd']()
            if not ltc_price or ltc_price < 5:
                return

            earliest_created = min(int(p.get('created_at') or now) for p in pending_list)
            cutoff_sec = min(now - 60 * 60, earliest_created - 120)

            for tx in txs[:30]:
                try:
                    tx_id = str(tx.get('txid') or '').strip()
                    if not tx_id:
                        continue
                    status = tx.get('status') or {}
                    # CRITICAL SECURITY FIX [CRIT-01]: Never credit unconfirmed mempool/RBF transactions!
                    if status.get('confirmed') is not True:
                        continue
                    block_time = int(status.get('block_time') or 0)
                    if block_time <= 0 or block_time < cutoff_sec:
                        continue

                    # Reject outgoing spends from our own wallet returning change
                    is_self_spend = False
                    for vin in tx.get('vin', []):
                        prev_addr = ((vin or {}).get('prevout') or {}).get('scriptpubkey_address')
                        if prev_addr == wallet_address:
                            is_self_spend = True
                            break
                    if is_self_spend:
                        continue

                    tx_id_normalized = self.ns['normalize_tx_id'](tx_id)
                    if self.db.used_transactions.find_one({'transaction_id': tx_id_normalized}, {'_id': 1}):
                        continue

                    received_ltc = 0.0
                    for vout in tx.get('vout', []):
                        if vout.get('scriptpubkey_address') == wallet_address:
                            received_ltc += int(vout.get('value', 0) or 0) / 100_000_000
                    if received_ltc <= 0:
                        continue

                    usd_amount = round(received_ltc * ltc_price, 6)
                    pending = self.match_pending_crypto(received_ltc, usd_amount, 'LTC', crypto_tol=0.00001)
                    if pending:
                        if block_time < int(pending.get('created_at', 0)) - 300:
                            continue
                        if self.safe_auto_credit(pending, tx_id_normalized, 'Litecoin (LTC) Auto'):
                            self.logger.info('✅ AUTO-CREDITED LTC: user %s -> $%.2f (%s)',
                                             pending['user_id'], pending['base_amount_usd'], tx_id[:16])
                except Exception as tx_err:
                    self.logger.debug('Skip LTC auto tx: %s', tx_err)
        except Exception as exc:
            self.logger.error('check_ltc_blockchain_auto error: %s', exc)

    def verify_ton_public_blockchain(self, message, lang, wallet_address):
        """Customer manual TON TxID / URL verification (supports hex, base64, and Tonviewer URLs)."""
        ns = self.ns
        uid = message.from_user.id
        if ns['is_user_banned'](uid):
            return
        if not message.text:
            self.bot.send_message(uid, ns['get_text'](uid, 'dep_fail'), parse_mode='HTML')
            return
        raw_text = message.text.strip()
        if raw_text.startswith('/') or raw_text.lower() in ('الغاء', 'cancel', 'إلغاء', 'اغلاق'):
            return
        if not wallet_address or wallet_address == 'Not Set' or not ton_canon_addr(wallet_address):
            self.bot.send_message(
                uid,
                ns['bil'](uid, '❌ <b>خطأ:</b> عنوان محفظة TON غير مضبوط في الإعدادات.',
                               '❌ <b>Error:</b> TON wallet address is not configured.'),
                parse_mode='HTML'
            )
            return

        canon_hex = ton_extract_hash(raw_text)
        if not canon_hex:
            self.bot.send_message(
                uid,
                ns['bil'](
                    uid,
                    '❌ <b>هاش معاملة TON غير صالح.</b>\n\n'
                    'أرسل هاش المعاملة الكامل (Hex أو Base64) أو رابط العملية مباشرة من Tonviewer / Tonscan.',
                    '❌ <b>Invalid TON transaction hash.</b>\n\n'
                    'Send the full Transaction Hash (Hex or Base64) or paste the Tonviewer / Tonscan link directly.'
                ),
                parse_mode='HTML'
            )
            return

        tx_track_key = 'ton' + canon_hex
        ton_all_keys = list(ns['_ton_tx_keys'](canon_hex))

        with ns['tx_lock']:
            if self.db.used_transactions.find_one({'transaction_id': {'$in': ton_all_keys}}, {'_id': 1}):
                self.bot.reply_to(message, ns['get_text'](uid, 'tx_used'))
                return
            if tx_track_key in ns['PROCESSING_TXS']:
                self.bot.send_message(
                    uid,
                    ns['bil'](uid, '⏳ <b>يتم فحص هذه العملية حالياً، يرجى الانتظار.</b>',
                                   '⏳ <b>This transaction is already being verified, please wait.</b>'),
                    parse_mode='HTML'
                )
                return
            ns['PROCESSING_TXS'].add(tx_track_key)

        claim_status = ns['claim_tx_hash'](tx_track_key, uid)
        if claim_status == 'stolen_attempt':
            ns['PROCESSING_TXS'].discard(tx_track_key)
            self.bot.send_message(
                uid,
                ns['bil'](uid, '❌ <b>هذه الحوالة مسجّلة لمستخدم آخر!</b>',
                               '❌ <b>This transfer belongs to another user!</b>'),
                parse_mode='HTML'
            )
            return
        if claim_status == 'already_used':
            ns['PROCESSING_TXS'].discard(tx_track_key)
            self.bot.reply_to(message, ns['get_text'](uid, 'tx_used'))
            return

        try:
            self.bot.send_message(uid, ns['get_text'](uid, 'crypto_checking'), parse_mode='HTML')
            found = self.lookup_tx_by_hash(canon_hex, wallet_address)
            if not found:
                self.db.claimed_hashes.delete_one({'transaction_id': tx_track_key, 'user_id': uid})
                self.bot.send_message(
                    uid,
                    ns['bil'](
                        uid,
                        '❌ <b>لم يتم العثور على حوالة TON مؤكدة لهذا الهاش على محفظة المتجر.</b>\n'
                        'تأكد من اكتمال التأكيد على الشبكة ثم أعد المحاولة بعد دقيقة.',
                        '❌ <b>No confirmed TON transfer to the store wallet was found for this hash.</b>\n'
                        'Wait 1 minute for blockchain confirmation and try again.'
                    ),
                    parse_mode='HTML'
                )
                return
            if found.get('is_sender'):
                self.db.claimed_hashes.delete_one({'transaction_id': tx_track_key, 'user_id': uid})
                self.bot.send_message(uid, '❌ <b>مرفوض:</b> هذه معاملة صادرة وليست إيداعاً.', parse_mode='HTML')
                return
            our_hex = found.get('hash') or canon_hex
            if our_hex != canon_hex:
                # Found through the sender/message hash: record the transfer under OUR transaction hash,
                # so the same transfer can never be credited twice through a different hash.
                new_key = 'ton' + our_hex
                with ns['tx_lock']:
                    if self.db.used_transactions.find_one({'transaction_id': {'$in': list(ns['_ton_tx_keys'](our_hex))}}, {'_id': 1}) \
                            or new_key in ns['PROCESSING_TXS']:
                        self.db.claimed_hashes.delete_one({'transaction_id': tx_track_key, 'user_id': uid})
                        self.bot.reply_to(message, ns['get_text'](uid, 'tx_used'))
                        return
                    ns['PROCESSING_TXS'].add(new_key)
                st2 = ns['claim_tx_hash'](new_key, uid)
                if st2 in ('stolen_attempt', 'already_used'):
                    ns['PROCESSING_TXS'].discard(new_key)
                    self.db.claimed_hashes.delete_one({'transaction_id': tx_track_key, 'user_id': uid})
                    self.bot.reply_to(message, ns['get_text'](uid, 'tx_used'))
                    return
                self.db.claimed_hashes.delete_one({'transaction_id': tx_track_key, 'user_id': uid})
                ns['PROCESSING_TXS'].discard(tx_track_key)
                tx_track_key = new_key
            utime = int(found.get('utime') or 0)
            if utime > 0 and (int(time.time()) - utime) > 24 * 3600:
                self.db.claimed_hashes.delete_one({'transaction_id': tx_track_key, 'user_id': uid})
                self.bot.send_message(uid, '❌ <b>مرفوض:</b> الحوالة قديمة (أكثر من 24 ساعة). تواصل مع الدعم.', parse_mode='HTML')
                return

            ton_price = self.get_ton_price_usd()
            if not ton_price:
                self.db.claimed_hashes.delete_one({'transaction_id': tx_track_key, 'user_id': uid})
                self.bot.send_message(uid, '⚠️ تعذّر جلب سعر TON حالياً. يرجى المحاولة بعد دقيقة.', parse_mode='HTML')
                return

            received_ton = float(found['received_ton'])
            usd_amount = round(received_ton * ton_price, 2)
            # If user had a matching pending deposit, settle it cleanly
            pending = self.db.pending_deposits.find_one({
                'user_id': uid, 'coin': 'TON', 'status': 'pending', 'expires_at': {'$gt': int(time.time())}
            }, sort=[('created_at', -1)])
            if pending:
                self.safe_auto_credit(pending, tx_track_key, 'Toncoin (TON)', actual_usd=usd_amount)
            else:
                ns['credit_user'](uid, usd_amount, tx_track_key, ns['get_lang'](uid), 'Toncoin (TON)', trusted_txid=True)
        finally:
            ns['PROCESSING_TXS'].discard(tx_track_key)

    def admin_search_ton_tx(self, query):
        wallet_address = self.ns['get_setting']('ton_address')
        if not wallet_address or wallet_address == 'Not Set':
            return None
        return self.lookup_tx_by_hash(query, wallet_address)

    def upgraded_wa_deposit_check(self, identity, body):
        """Enable instant check button & optional TxID submission for TON, LTC, USDT, Binance & Bybit."""
        ns = self.ns
        uid = identity['id']
        if ns['is_user_banned'](uid):
            return 403, {'error': 'banned'}
        rec = self.db.pending_deposits.find_one({'user_id': uid, 'status': 'pending'}, sort=[('created_at', -1)])
        if not rec:
            return 200, {'ok': True, 'status': None, 'balance': ns['_wa_balance'](uid)}
        if not ns['_wa_rate_ok'](f"depchk:{uid}", 4, 30):
            return 200, {'ok': True, 'status': 'pending', 'throttled': True}

        coin = str(rec.get('coin', ''))
        supplied = str((body or {}).get('transaction_id') or '').strip()
        supplied = re.sub(r'[\u200b-\u200f\u202a-\u202e\u2060-\u2069\ufeff]', '', supplied)
        supplied = supplied.translate(str.maketrans('٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹', '01234567890123456789'))

        target = None
        if coin == 'BINANCE':
            if not supplied and rec.get('last_manual_check') == 'search_pending':
                supplied = str((rec.get('binance_manual_search') or {}).get('supplied') or '').strip()
            if supplied:
                if not re.fullmatch(r'[A-Za-z0-9_-]{8,128}', supplied, re.ASCII):
                    return 400, {'error': 'bad_request'}
                self.db.pending_deposits.update_one({'pending_id': rec['pending_id'], 'user_id': uid},
                                                    {'$set': {'last_manual_check': 'checking'}})
                target = lambda: ns['verify_binance_pay'](ns['_WAMessage'](uid, supplied), ns['get_lang'](uid))
            else:
                target = lambda: ns['binance_check_payment'](ns['_WACall'](uid, f"binance_check_{uid}"))
        elif coin.startswith('BYBIT'):
            target = lambda: ns['bybit_check_payment'](ns['_WACall'](uid, f"bybit_check_{uid}"))
        elif coin == 'TON':
            if supplied:
                target = lambda: self.verify_ton_public_blockchain(
                    ns['_WAMessage'](uid, supplied), ns['get_lang'](uid), ns['get_setting']('ton_address', 'Not Set')
                )
            else:
                target = self.check_ton_blockchain_auto
        elif coin == 'LTC':
            if supplied and ns['_ltc_txid'](supplied):
                target = lambda: ns['verify_ltc_public_blockchain'](
                    ns['_WAMessage'](uid, supplied), ns['get_lang'](uid), ns['get_setting']('ltc_address', 'Not Set')
                )
            else:
                target = self.check_ltc_blockchain_auto
        elif coin in ('USDT', 'USDT_BEP20'):
            target = ns['check_usdt_blockchain_auto']

        if target:
            threading.Thread(target=target, daemon=True, name='webapp-dep-check').start()
        return 200, {'ok': True, 'status': 'pending', 'manual': bool(target)}

    def fast_ad_check_tx_handle(self, message):
        """Fast, non-hanging Admin Transaction Check across DB, Binance (if configured), Bybit, TON, LTC & USDT."""
        ns = self.ns
        uid = message.from_user.id
        if not ns['_is_admin_check'](uid):
            return
        text = (message.text or '').strip()
        if not text or text.lower() in ('/cancel', 'الغاء', 'إلغاء', 'cancel'):
            self.bot.send_message(uid, '❌ تم الإلغاء.')
            return

        query = text.strip()
        ton_hex = ton_extract_hash(query)
        query_norm = ns['normalize_tx_id'](ton_hex or query)
        self.bot.send_message(uid, f"🔍 <b>جاري الفحص السريع:</b> <code>{html.escape(query[:80])}</code>", parse_mode='HTML')

        # 1. Search Database (used_transactions + balance_logs + TON canonical keys)
        found = []
        seen_ids = set()
        try:
            or_clauses = [{'transaction_id': query_norm}, {'display_transaction_id': query}, {'transaction_id': query}]
            if ton_hex:
                or_clauses.append({'transaction_id': {'$in': list(ns['_ton_tx_keys'](ton_hex))}})
            for r in self.db.used_transactions.find({'$or': or_clauses}).limit(5):
                if r['_id'] not in seen_ids:
                    found.append(r)
                    seen_ids.add(r['_id'])
            if len(found) < 5:
                safe = re.escape((ton_hex or query)[-20:] if len(ton_hex or query) > 20 else (ton_hex or query))
                for r in self.db.used_transactions.find({'$or': [
                    {'transaction_id': {'$regex': safe, '$options': 'i'}},
                    {'remark': {'$regex': safe, '$options': 'i'}},
                ]}).limit(5):
                    if r['_id'] not in seen_ids:
                        found.append(r)
                        seen_ids.add(r['_id'])
            safe2 = re.escape(query[-20:] if len(query) > 20 else query)
            for r in self.db.balance_logs.find({'$or': [{'note': query}, {'note': {'$regex': safe2, '$options': 'i'}}]}).limit(5):
                if r['_id'] not in seen_ids:
                    found.append({
                        '_id': r.get('_id'),
                        'transaction_id': f"[Manual Note] {str(r.get('note', ''))[:50]}",
                        'user_id': r.get('user_id'),
                        'amount': r.get('amount', 0),
                        'method': f"Manual Gift ({r.get('type', '?')})",
                        'used_at': r.get('date', ''),
                        'remark': r.get('note', ''),
                    })
                    seen_ids.add(r['_id'])
        except Exception as exc:
            self.bot.send_message(uid, f"❌ خطأ في فحص قاعدة البيانات: {html.escape(str(exc))}", parse_mode='HTML')
            return

        if found:
            self.bot.send_message(uid, f"✅ <b>مسجّلة في قاعدة البيانات ({len(found)} نتيجة):</b>", parse_mode='HTML')
            for i, r in enumerate(found, 1):
                u_id = r.get('user_id', '?')
                u_doc = self.db.users.find_one({'user_id': u_id}) if isinstance(u_id, int) else None
                uname = (u_doc or {}).get('username') or ''
                fname = (u_doc or {}).get('first_name') or (u_doc or {}).get('name') or str(u_id)
                u_bal = float((u_doc or {}).get('balance', 0) or 0)
                detail = (
                    f"━━━ <b>#{i}</b> ━━━\n"
                    f"🆔 <b>TX:</b> <code>{html.escape(str(r.get('transaction_id', '?')))}</code>\n"
                    f"👤 <b>المستخدم:</b> <a href=\"tg://user?id={u_id}\">{html.escape(fname)}</a> (<code>{u_id}</code>)"
                    + (f" · @{html.escape(uname)}" if uname else '') + "\n"
                    f"💰 <b>المبلغ:</b> <b>${float(r.get('amount') or 0):.4f}</b> · 💼 الرصيد: <b>${u_bal:.2f}</b>\n"
                    f"🔄 <b>الطريقة:</b> {html.escape(str(r.get('method', '?')))}\n"
                    f"📅 <b>الوقت:</b> {html.escape(str(r.get('used_at', ''))[:19])}"
                )
                if r.get('remark'):
                    detail += f"\n📝 <b>ملاحظة:</b> <code>{html.escape(str(r['remark'])[:200])}</code>"
                self.bot.send_message(uid, detail, parse_mode='HTML', disable_web_page_preview=True)
            return

        # 2. Check TON first if input looks like a TON URL/base64/64-hex and TON wallet is configured!
        onchain_match = None
        onchain_is_sender = None

        ton_wallet = ns['get_setting']('ton_address')
        if ton_wallet and ton_wallet != 'Not Set' and (ton_hex or 'ton' in query.lower()):
            ton_r = self.admin_search_ton_tx(query)
            if ton_r:
                if ton_r.get('is_sender'):
                    onchain_is_sender = ('TON', ton_r.get('hash', query))
                else:
                    ton_price = self.get_ton_price_usd()
                    if not ton_price:
                        self.bot.send_message(
                            uid,
                            f"⚠️ <b>تم العثور على حوالة TON ({ton_r['received_ton']:.6f} TON)</b> لكن تعذّر جلب سعر الصرف اللحظي.\n"
                            f"🆔 <code>{html.escape(ton_r['hash'])}</code>",
                            parse_mode='HTML'
                        )
                        return
                    onchain_match = {
                        'coin': 'TON', 'label': f"💎 Toncoin ({ton_r.get('provider', 'TON')})",
                        'amount_usd': round(ton_r['received_ton'] * ton_price, 4),
                        'crypto_amount': ton_r['received_ton'],
                        'hash': ton_r['hash'],
                        'when': ton_r.get('utime', 0),
                        'source_addr': ton_r.get('source_addr', ''),
                    }

        # 3. Check Binance ONLY if API credentials are actually configured (prevents 3-minute proxy hang!)
        binance_configured = bool(str(ns.get('BINANCE_API_KEY') or '').strip() and str(ns.get('BINANCE_API_SECRET') or '').strip())
        if not onchain_match and not onchain_is_sender and binance_configured:
            try:
                safe_b = re.escape(query[-15:] if len(query) > 15 else query)
                pattern = re.compile(safe_b, re.IGNORECASE)
                q_hex = query.lower()[2:] if query.lower().startswith('0x') else query.lower()
                is_onchain = len(q_hex) == 64 and all(c in '0123456789abcdef' for c in q_hex)
                binance_match, binance_source = None, None
                if not is_onchain:
                    txs = ns['_binance_pay_request']()
                    for tx in txs or []:
                        fields = (str(tx.get('orderId') or ''), str(tx.get('transactionId') or ''), str(tx.get('bizOrderNo') or ''))
                        if any(pattern.search(f) for f in fields):
                            binance_match, binance_source = tx, 'pay'
                            break
                if not binance_match:
                    now_ms = int(time.time() * 1000)
                    start_ms = now_ms - (90 * 86400 * 1000) + 120000
                    deps = ns['execute_binance_call'](
                        lambda c: c.get_deposit_history(offset=0, limit=1000, startTime=start_ms),
                        max_retries=2, fast_mode=True, total_timeout=8
                    ) or []
                    for d in deps:
                        tx_h = str(d.get('txId') or '')
                        if tx_h and (pattern.search(tx_h) or ns['_tx_hash_matches'](query, tx_h)):
                            d['_coin'] = d.get('coin', 'CRYPTO')
                            binance_match, binance_source = d, 'deposit'
                            break
                if binance_match:
                    if binance_source == 'pay':
                        oid = str(binance_match.get('orderId') or binance_match.get('transactionId') or '?')
                        cur = ns['_binance_tx_currency'](binance_match) or 'USDT'
                        usd_v, _ = ns['binance_amount_to_usd'](binance_match.get('amount') or 0, cur)
                        onchain_match = {
                            'coin': cur, 'label': '🟡 Binance Pay',
                            'amount_usd': float(usd_v or 0),
                            'crypto_amount': abs(float(binance_match.get('amount') or 0)),
                            'hash': oid,
                            'when': int(binance_match.get('transactionTime') or 0) // 1000,
                            'source_addr': str(binance_match.get('note') or ''),
                        }
                    else:
                        oid = str(binance_match.get('txId') or '?')
                        cur = binance_match.get('_coin', 'CRYPTO')
                        usd_v, _ = ns['binance_amount_to_usd'](binance_match.get('amount') or 0, cur)
                        onchain_match = {
                            'coin': cur, 'label': f'🟡 Binance Deposit ({cur})',
                            'amount_usd': float(usd_v or 0),
                            'crypto_amount': abs(float(binance_match.get('amount') or 0)),
                            'hash': oid,
                            'when': int(binance_match.get('insertTime') or 0) // 1000,
                            'source_addr': str(binance_match.get('address') or ''),
                        }
            except Exception as be:
                self.logger.debug('Admin fast Binance check skipped: %s', be)

        # 4. Check Bybit if configured
        if not onchain_match and not onchain_is_sender and ns['bybit_is_configured']():
            try:
                by_r = ns['admin_search_bybit_tx'](query)
                if by_r and by_r.get('amount_usd'):
                    onchain_match = {
                        'coin': by_r.get('coin', 'USDT'),
                        'label': by_r.get('label', '🟠 Bybit'),
                        'amount_usd': by_r['amount_usd'],
                        'crypto_amount': by_r.get('crypto_amount', 0),
                        'hash': by_r.get('hash', query),
                        'when': int(by_r.get('when', 0) or 0) // 1000,
                        'source_addr': by_r.get('note', '') or '',
                    }
            except Exception as exc:
                self.logger.debug('Admin Bybit search err: %s', exc)

        # 5. Check TON (if not already checked above), LTC, and USDT (TRC20 + BEP20)
        if not onchain_match and not onchain_is_sender and ton_wallet and ton_wallet != 'Not Set' and not (ton_hex or 'ton' in query.lower()):
            ton_r = self.admin_search_ton_tx(query)
            if ton_r:
                if ton_r.get('is_sender'):
                    onchain_is_sender = ('TON', ton_r.get('hash', query))
                else:
                    tp = self.get_ton_price_usd() or 0
                    onchain_match = {
                        'coin': 'TON', 'label': '💎 Toncoin (TON)',
                        'amount_usd': round(ton_r['received_ton'] * tp, 4),
                        'crypto_amount': ton_r['received_ton'],
                        'hash': ton_r['hash'], 'when': ton_r.get('utime', 0),
                        'source_addr': ton_r.get('source_addr', ''),
                    }

        if not onchain_match and not onchain_is_sender:
            ltc_r = ns['_admin_search_ltc_tx'](query)
            if ltc_r:
                if ltc_r.get('is_sender'):
                    onchain_is_sender = ('LTC', ltc_r.get('hash', query))
                else:
                    lp = ns['get_ltc_price_usd']() or 0
                    onchain_match = {
                        'coin': 'LTC', 'label': '🔵 Litecoin (LTC)',
                        'amount_usd': round(ltc_r['received_ltc'] * lp, 4),
                        'crypto_amount': ltc_r['received_ltc'],
                        'hash': ltc_r.get('hash', query), 'when': ltc_r.get('block_time', 0),
                        'source_addr': '',
                    }

        if not onchain_match and not onchain_is_sender:
            usdt_r = ns['_admin_search_usdt_onchain'](query)
            if isinstance(usdt_r, dict) and not usdt_r.get('_conn_fail'):
                onchain_match = usdt_r

        if onchain_is_sender:
            _c, _h = onchain_is_sender
            self.bot.send_message(
                uid,
                f"⚠️ <b>هذه حوالة صادرة من محفظة المتجر ({_c}) وليست إيداعاً.</b>\n🆔 <code>{html.escape(str(_h)[:70])}</code>",
                parse_mode='HTML'
            )
            return

        if onchain_match:
            amt_usd = float(onchain_match.get('amount_usd') or 0)
            _hash = str(onchain_match['hash'])
            _coin = str(onchain_match['coin'])
            tx_norm = ('ton' + _hash) if _coin == 'TON' and len(_hash) == 64 else ns['normalize_tx_id'](_hash)
            pending_credit_id = f"pc_{int(time.time())}_{_hash[-10:]}"
            self.db.pending_admin_credits.insert_one({
                '_id': pending_credit_id,
                'tx_id': tx_norm,
                'amount': amt_usd,
                'crypto_amount': float(onchain_match.get('crypto_amount') or 0),
                'crypto_currency': _coin,
                'note': f"{_coin} ({onchain_match.get('crypto_amount', 0):.6f} {_coin})",
                'payer_name': str(onchain_match.get('source_addr') or ''),
                'source': f"onchain_{_coin.lower()}",
                'created_at': datetime.datetime.utcnow(),
                'admin_id': uid,
            })
            when_str = 'غير متوفر'
            if onchain_match.get('when'):
                try:
                    when_str = datetime.datetime.fromtimestamp(int(onchain_match['when']), datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                except Exception:
                    pass
            src = str(onchain_match.get('source_addr') or '')
            src_line = f"👤 <b>المُرسِل / الملاحظة:</b> <code>{html.escape(src[:60])}</code>\n" if src else ''
            msg_txt = (
                f"✅ <b>تم العثور على المعاملة (غير مضافة بعد)</b>\n\n"
                f"📡 <b>المصدر:</b> {onchain_match['label']}\n"
                f"💰 <b>القيمة:</b> <b>${amt_usd:.4f}</b> (<code>{onchain_match.get('crypto_amount', 0):.6f} {_coin}</code>)\n"
                f"📅 <b>التوقيت:</b> {when_str}\n"
                f"{src_line}"
                f"🆔 <b>المعرّف:</b>\n<code>{html.escape(_hash[:80])}</code>"
            )
            mk = ns['InlineKeyboardMarkup']()
            lbl = f"💰 إضافة ${amt_usd:.2f} لمستخدم" if amt_usd > 0 else "💵 إضافة لمستخدم"
            mk.add(ns['InlineKeyboardButton'](lbl, callback_data=f"credit_pending_{pending_credit_id}"))
            self.bot.send_message(uid, msg_txt, parse_mode='HTML', reply_markup=mk)
            return

        # Clear, honest summary showing which networks are configured
        checked = ['قاعدة البيانات']
        if ton_wallet and ton_wallet != 'Not Set':
            checked.append('TON (TonAPI + TONCenter)')
        if ns['get_setting']('ltc_address', 'Not Set') != 'Not Set':
            checked.append('LTC')
        if ns['get_setting']('usdt_address', 'Not Set') != 'Not Set' or ns['get_setting']('usdt_bep20_address', 'Not Set') != 'Not Set':
            checked.append('USDT (TRC20/BEP20)')
        if ns['bybit_is_configured']():
            checked.append('Bybit')
        if binance_configured:
            checked.append('Binance')

        self.bot.send_message(
            uid,
            f"❌ <b>لم يتم العثور على المعاملة.</b>\n\n"
            f"🔎 <b>المُدخل:</b> <code>{html.escape(query[:80])}</code>\n"
            f"🛡 <b>تم الفحص في:</b> {' · '.join(checked)}\n\n"
            f"<i>تأكد من إرسال هاش العملية الصحيح (أو رابط Tonviewer) وأن عنوان المحفظة مضبوط في إعدادات المتجر.</i>",
            parse_mode='HTML'
        )

    def wallet_diagnostics_report(self):
        """Live health report for TON, LTC, USDT, Bybit & Binance settings."""
        ns = self.ns
        lines = ['🔬 <b>تقرير فحص المحافظ والشبكات المباشر</b>', '']

        # TON
        ton_addr = str(ns['get_setting']('ton_address', '') or '').strip()
        if not ton_addr or ton_addr == 'Not Set':
            lines.append('💎 <b>TON:</b> ⚪ غير مضبوط (أضف العنوان من إعدادات المتجر)')
        else:
            canon = ton_canon_addr(ton_addr)
            tp = self.get_ton_price_usd()
            txs = self.fetch_wallet_txs(ton_addr, limit=5) if canon else []
            st = '🟢 متصل ويعمل' if (canon and tp) else '🔴 يحتاج مراجعة'
            lines.append(
                f"💎 <b>TON:</b> {st}\n"
                f"   • العنوان: <code>{html.escape(ton_addr[:14])}…{html.escape(ton_addr[-8:])}</code>\n"
                f"   • السعر اللحظي: <b>{f'${tp:.4f}' if tp else '❌ تعذّر الجلب'}</b>\n"
                f"   • آخر إيداعات مقروءة: <b>{len(txs)}</b>"
            )

        # USDT TRC20 & BEP20
        trc = str(ns['get_setting']('usdt_address', '') or '').strip()
        bep = str(ns['get_setting']('usdt_bep20_address', '') or '').strip()
        lines.append(f"🟢 <b>USDT TRC-20:</b> {'🟢 مضبوط' if trc and trc != 'Not Set' else '⚪ غير مضبوط'}")
        lines.append(f"🟡 <b>USDT BEP-20:</b> {'🟢 مضبوط' if bep and bep != 'Not Set' else '⚪ غير مضبوط'}")

        # LTC
        ltc = str(ns['get_setting']('ltc_address', '') or '').strip()
        if ltc and ltc != 'Not Set':
            lp = ns['get_ltc_price_usd']()
            lines.append(f"🔵 <b>Litecoin (LTC):</b> {'🟢 متصل' if lp else '⚠️ السعر غير متاح'} ({f'${lp:.2f}' if lp else '—'})")
        else:
            lines.append('🔵 <b>Litecoin (LTC):</b> ⚪ غير مضبوط')

        # Bybit & Binance
        lines.append(f"🟠 <b>Bybit:</b> {'🟢 مفعّل' if ns['bybit_is_configured']() else '⚪ غير مفعّل'}")
        b_ok = bool(str(ns.get('BINANCE_API_KEY') or '').strip() and str(ns.get('BINANCE_API_SECRET') or '').strip())
        lines.append(f"🟡 <b>Binance API:</b> {'🟢 مفعّل' if b_ok else '⚪ غير مضبوط (يدوي فقط)'}")
        lines.append(f"🛡 <b>حماية المبالغ الفريدة:</b> {'✅ مفعّلة' if ns['is_amount_protection_enabled']() else '⚠️ معطّلة'}")
        return '\n'.join(lines)

    def install(self):
        ns = self.ns
        engine = self

        ns['_ton_canon_addr'] = ton_canon_addr
        ns['_ton_extract_hash'] = ton_extract_hash
        ns['get_ton_price_usd'] = self.get_ton_price_usd
        ns['auto_credit_from_pending'] = self.safe_auto_credit
        ns['check_ton_blockchain_auto'] = self.check_ton_blockchain_auto
        ns['check_ltc_blockchain_auto'] = self.check_ltc_blockchain_auto
        ns['verify_ton_public_blockchain'] = ns['safe_next_step'](self.verify_ton_public_blockchain)
        ns['_admin_search_ton_tx'] = self.admin_search_ton_tx
        ns['_wa_deposit_check'] = self.upgraded_wa_deposit_check
        if '/webapp/deposit/check' in ns.get('_WA_POST_ROUTES', {}):
            ns['_WA_POST_ROUTES']['/webapp/deposit/check'] = self.upgraded_wa_deposit_check

        # Replace ad_check_tx_handle so both Bot and Mini App Virtual Console use the fast, fixed version
        ns['ad_check_tx_handle'] = ns['safe_next_step'](self.fast_ad_check_tx_handle)

        # Upgrade credit_pending_exec to resolve Email accounts too (and merged Telegram accounts!)
        orig_credit_exec = ns['credit_pending_exec']
        @functools.wraps(orig_credit_exec)
        def upgraded_credit_pending_exec(message, pending_id):
            txt = (message.text or '').strip()
            if '@' in txt and not txt.startswith('@') and '.' in txt.split('@', 1)[-1]:
                guard = ns.get('_UNIFIED_BAN_GUARD')
                u, _ = guard.resolve_target(txt) if guard else (None, None)
                if u:
                    merged = u.get('web_merged_into')
                    message.text = str(int(merged) if merged else int(u['user_id']))
            return orig_credit_exec(message, pending_id)
        ns['credit_pending_exec'] = upgraded_credit_pending_exec

        # Register `/ton_status` command (prepended ahead of any catch-all message handler)
        before_mh = len(self.bot.message_handlers)
        @self.bot.message_handler(commands=['ton_status'])
        def ton_status_cmd(message):
            if not ns['_is_admin_check'](message.from_user.id):
                return
            self.bot.send_message(message.chat.id, engine.wallet_diagnostics_report(), parse_mode='HTML')

        added_mh = self.bot.message_handlers[before_mh:]
        del self.bot.message_handlers[before_mh:]
        self.bot.message_handlers[0:0] = added_mh

        def wallet_diag_cb(call):
            self.bot.answer_callback_query(call.id)
            mk = ns['InlineKeyboardMarkup']()
            mk.add(ns['InlineKeyboardButton']('🔄 تحديث الفحص', callback_data='ad_wallet_diag'))
            mk.add(ns['InlineKeyboardButton']('⚙️ إعدادات المتجر', callback_data='ad_shop_settings'))
            mk.add(ns['InlineKeyboardButton']('🔙 لوحة الإدارة', callback_data='admin_panel_main'))
            try:
                self.bot.edit_message_text(
                    engine.wallet_diagnostics_report(),
                    call.message.chat.id, call.message.message_id,
                    parse_mode='HTML', reply_markup=mk
                )
            except Exception:
                self.bot.send_message(call.message.chat.id, engine.wallet_diagnostics_report(), parse_mode='HTML', reply_markup=mk)

        before = len(self.bot.callback_query_handlers)
        self.bot.callback_query_handler(func=lambda call: call.data == 'ad_wallet_diag')(
            ns['admin_required'](wallet_diag_cb)
        )
        added = self.bot.callback_query_handlers[before:]
        del self.bot.callback_query_handlers[before:]
        self.bot.callback_query_handlers[0:0] = added
        ns['_ADM_OPEN_OK'].add('ad_wallet_diag')

        # Add optional TONCenter API key setting support
        if '_SETTING_KEYS' in ns:
            ns['_SETTING_KEYS']['set_v_ton_key'] = 'toncenter_api_key'

        # Allow customers with an active TON/LTC pending deposit to paste their TxID directly in Bot chat
        before_tx_mh = len(self.bot.message_handlers)
        @self.bot.message_handler(
            content_types=['text'],
            func=lambda m: (
                getattr(m.chat, 'type', '') == 'private'
                and isinstance(m.text, str)
                and not m.text.startswith('/')
                and m.from_user.id in ns.get('PENDING_DEPOSIT_USERS', {})
                and (ton_extract_hash(m.text) != '' or 'tonviewer.' in m.text.lower() or 'tonscan.' in m.text.lower())
            )
        )
        def auto_catch_customer_txid(message):
            uid = message.from_user.id
            coin = ns['PENDING_DEPOSIT_USERS'].get(uid)
            if coin == 'TON':
                engine.verify_ton_public_blockchain(message, ns['get_lang'](uid), ns['get_setting']('ton_address', 'Not Set'))
            elif coin == 'LTC' and ns['_ltc_txid'](message.text.strip()):
                ns['verify_ltc_public_blockchain'](message, ns['get_lang'](uid), ns['get_setting']('ltc_address', 'Not Set'))

        added_tx_mh = self.bot.message_handlers[before_tx_mh:]
        del self.bot.message_handlers[before_tx_mh:]
        self.bot.message_handlers[0:0] = added_tx_mh

        return self


def install(namespace):
    if namespace.get('_TON_AND_CHECK_ENGINE') is not None:
        return namespace['_TON_AND_CHECK_ENGINE']
    service = TonAndCheckEngine(namespace).install()
    namespace['_TON_AND_CHECK_ENGINE'] = service
    return service
