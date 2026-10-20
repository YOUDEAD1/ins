"""Read-only referral report. Does not import or start bot.py."""
import argparse
import json
import os
from pathlib import Path
import re
import sys


def _connection_values(values, source):
    uri = str(values.get('MONGO_URI') or '').strip()
    if not uri:
        return None
    if not uri.startswith(('mongodb://', 'mongodb+srv://')):
        raise ValueError('MONGO_URI must start with mongodb:// or mongodb+srv://; its value is hidden.')
    return {'uri': uri, 'database': str(values.get('MONGO_DB_NAME') or '').strip() or 'shop_db',
            'source': source}


def _read_connection_file(path):
    from dotenv import dotenv_values
    path = Path(os.path.expandvars(str(path).strip().strip('"').strip("'"))).expanduser()
    if not path.is_file():
        raise ValueError('The selected settings file does not exist or is not a file.')
    try:
        values = dotenv_values(path, encoding='utf-8-sig')
    except (OSError, UnicodeError):
        raise ValueError('The selected settings file could not be read as UTF-8.') from None
    return _connection_values(values, path.name)


def _choose_connection_file():
    if os.name == 'nt':
        window = None
        try:
            import tkinter as tk
            from tkinter import filedialog
            window = tk.Tk(); window.withdraw()
            window.attributes('-topmost', True)
            chosen = filedialog.askopenfilename(parent=window,
                title='LARA STOCK - Choose the settings file used by your bot',
                initialdir=str(Path(__file__).resolve().parent),
                filetypes=[('Environment files', '.env .env.txt *.env *.txt'), ('All files', '*.*')])
            return chosen or None
        except Exception:
            pass
        finally:
            if window is not None:
                try:
                    window.destroy()
                except Exception:
                    pass
    return input('Path to the bot settings file (.env or .env.txt), or Enter to cancel: ').strip() or None


def resolve_connection(env_file=None, *, allow_prompt=False):
    # Explicit selection is authoritative; never mix its database with another file.
    if env_file:
        result = _read_connection_file(env_file)
        if not result:
            raise ValueError('The selected file has an empty or missing MONGO_URI. Select the filled settings file used by your bot.')
        return result
    result = _connection_values(os.environ, 'process environment')
    if result:
        return result
    folder = Path(__file__).resolve().parent
    seen = set()
    for parent in (folder, Path.cwd(), folder.parent):
        for name in ('.env', '.env.txt'):
            candidate = parent / name
            if candidate in seen:
                continue
            seen.add(candidate)
            if candidate.is_file():
                result = _read_connection_file(candidate)
                if result:
                    return result
    if allow_prompt:
        print('No filled database settings were found. Choose the SAME settings file used by the bot.')
        selected = _choose_connection_file()
        if selected:
            return resolve_connection(selected)
    raise ValueError('MONGO_URI was not found. Put the filled .env beside this tool, select it in the file window, or use --env "path-to-file".')


def build_report(db, target):
    """Read-only, bounded account report. A stored reason is not membership proof."""
    target = str(target or '').strip().lstrip('@')
    if target.isdigit():
        query = {'user_id': int(target)}
    elif re.fullmatch('[A-Za-z0-9_]{3,32}', target):
        query = {'username': {'$regex': '^@?' + re.escape(target) + '$', '$options': 'i'}}
    else:
        return {'state': 'invalid_target', 'read_only': True}
    users = list(db.users.find(query, {'_id': 0, 'user_id': 1, 'username': 1, 'balance': 1, 'ref_debt': 1, 'ref_v2_earned': 1, 'ref_v2_purchase_earned': 1}).limit(3))
    if len(users) != 1:
        return {'state': 'not_found' if not users else 'ambiguous_username', 'read_only': True, 'matching_user_ids': [u['user_id'] for u in users]}
    user, uid = (users[0], int(users[0]['user_id']))
    totals = list(db.balance_logs.aggregate([{'$match': {'user_id': uid}}, {'$group': {'_id': '$type', 'count': {'$sum': 1}, 'cash_amount': {'$sum': '$amount'}, 'debt_delta': {'$sum': '$debt_delta'}}}]))
    recent = list(db.balance_logs.find({'user_id': uid}, {'_id': 0, 'type': 1, 'amount': 1, 'nominal_amount': 1, 'debt_delta': 1, 'new_balance': 1, 'old_balance': 1, 'balance_source': 1, 'date': 1, 'note': 1, 'by_admin': 1, 'ref_event': 1}).sort('date', -1).limit(250))
    for log in recent:
        if 'note' in log:
            log['note'] = str(log['note'])[:400]
    reasons = list(db.ref_clawbacks.aggregate([{'$match': {'referrer_id': uid, 'kind': 'clawback'}}, {'$group': {'_id': '$reason', 'count': {'$sum': 1}, 'nominal_amount': {'$sum': '$amount'}}}]))
    statuses = list(db.referrals_v2.aggregate([{'$match': {'referrer_id': uid}}, {'$group': {'_id': '$status', 'count': {'$sum': 1}}}]))
    tables = {}
    sources = [('orders', {'user_id': uid}), ('ext_orders', {'user_id': uid}), ('api_orders', {'api_user_id': uid}), ('used_transactions', {'user_id': uid}), ('purchase_rewards', {'referrer_id': uid}), ('referral_milestones', {'referrer_id': uid}), ('ref_money_events', {'user_id': uid})]
    money_fields = ('amount', 'price', 'total_price', 'paid_amount', 'reward', 'balance_credited', 'debt_settled')
    safe_fields = {key: 1 for key in ('_id', 'user_id', 'api_user_id', 'referrer_id', 'buyer_id', 'order_id', 'product_id', 'type', 'status', 'via', 'quantity', 'qty', 'date', 'created_at', 'timestamp', 'paid_at', 'batch_no', 'threshold', 'active_count', 'kind', 'applied_at', 'receipt.balance_before', 'receipt.balance_after', 'receipt.balance_delta', 'receipt.debt_before', 'receipt.debt_after', 'receipt.debt_delta', *money_fields)}
    for name, match in sources:
        group = {'_id': None, 'count': {'$sum': 1}}
        group.update({'recorded_' + key: {'$sum': '$' + key} for key in money_fields})
        summary = list(db[name].aggregate([{'$match': match}, {'$group': group}]))
        tables[name] = {'summary': {k: v for k, v in (summary[0] if summary else {'count': 0}).items() if k != '_id'}, 'recent': list(db[name].find(match, safe_fields).sort('_id', -1).limit(150)), 'recent_limit': 150}
    last = next((row for row in recent if isinstance(row.get('new_balance'), (int, float))), None)
    current_balance = round(float(user.get('balance') or 0), 4)
    assessment = {'current_balance': current_balance, 'requires_review': current_balance < 0, 'latest_balance_log_value': round(float(last['new_balance']), 4) if last else None, 'latest_balance_log_date': last.get('date') if last else None, 'change_since_latest_balance_log': round(current_balance - float(last['new_balance']), 4) if last else None, 'latest_log_snapshot_independently_verified': bool(last and last.get('balance_source') == 'atomic_wallet_update'), 'caution': 'Legacy new_balance snapshots may have come from a stale user cache. The change since the latest balance log may include purchases, deposits and rewards kept in other tables. Separate table totals are not combined because a single movement may appear in more than one table.'}
    referral_settings = list(db.settings.find({'key': {'$in': ['referral_threshold', 'referral_reward', 'referral_purchase_reward', 'referral_min_purchase', 'ref_batches_v2', 'ref_dv_since', 'referrals_v2_initialized_v6']}}, {'_id': 0, 'key': 1, 'value': 1, 'init_time': 1, 'at': 1}))
    return {'report_version': 2, 'state': 'found', 'read_only': True, 'account': user, 'ledger_totals': totals, 'recent_changes': recent, 'recent_limit': 250, 'stored_clawback_reasons': reasons, 'referrals_by_status': statuses, 'assessment': assessment, 'financial_tables': tables, 'referral_settings': referral_settings, 'legacy_referrals': db.referrals_v2.count_documents({'referrer_id': uid, 'ref_policy_version': {'$ne': 30}}), 'explanation': 'Recorded left/rejected reasons do not prove that Telegram membership actually changed. No credit, deduction, reclassification or debt forgiveness was performed.'}


def main(argv=None):
    parser = argparse.ArgumentParser(description='Read-only referral balance diagnosis')
    parser.add_argument('target', nargs='?', help='Telegram numeric ID or @username')
    parser.add_argument('--env', '--env-file', dest='env_file', help='Path to the filled bot settings file')
    parser.add_argument('--no-input', action='store_true', help='Do not open a file selector or prompt for input')
    args = parser.parse_args(argv)
    from pymongo import MongoClient
    try:
        config = resolve_connection(args.env_file, allow_prompt=not args.no_input and sys.stdin.isatty())
    except ValueError as exc:
        print(str(exc))
        return 2
    print('Settings source:', config['source'], '| Database:', config['database'])
    target = args.target
    if not target:
        if args.no_input:
            print('Provide a Telegram @username or numeric ID.')
            return 2
        target = input('Telegram @username or numeric ID: ').strip()
    client = None
    try:
        client = MongoClient(config['uri'], serverSelectionTimeoutMS=7000, connectTimeoutMS=5000,
                             socketTimeoutMS=7000, appname='LARA-readonly-referral-report')
        report = build_report(client[config['database']], target)
        if report['state'] != 'found':
            print(json.dumps(report, ensure_ascii=False))
            return 3
        user = report['account']
        path = Path.cwd() / ('referral-report-' + str(int(user['user_id'])) + '.json')
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        print('READ ONLY: no database data or balance was changed.')
        print('User ID:', user['user_id'], 'Balance:', user.get('balance', 0))
        print('Report:', path.name)
        return 0
    except Exception as exc:
        print('Database report unavailable (' + type(exc).__name__ + ').')
        print('Connection details and credentials are intentionally omitted.')
        return 4
    finally:
        if client is not None:
            client.close()


if __name__ == '__main__':
    raise SystemExit(main())
