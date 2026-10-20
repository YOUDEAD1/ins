"""Retained pre-update functions for offline before/after network measurements."""

def _cgpt_account_seat_info(doc):
    """يفحص حساباً ويرجّع معلومات مقاعده."""
    mgr = _cgpt_build_manager_from_doc(doc)
    try:
        rep = mgr.diagnose()
    except Exception:
        rep = {'connected': False, 'error': 'تعذر الاتصال بالحساب أثناء الفحص'}
    total = rep.get('total_seats')
    used = rep.get('used_seats', 0)
    avail = rep.get('available_seats')
    # لو الكشف التلقائي فشل، نستخدم القيمة اليدوية المحفوظة (لو موجودة)
    manual_seats = doc.get('manual_seats')
    if not total and manual_seats:
        total = int(manual_seats)
        avail = max(0, total - used) if rep.get('connected') else None
    return {
        'id': str(doc.get('_id')),
        'name': doc.get('name', doc.get('data', {}).get('user', {}).get('email', 'حساب')),
        'email': doc.get('data', {}).get('user', {}).get('email') or rep.get('owner_email') or '?',
        'connected': rep.get('connected', False),
        'used_seats': used,
        'total_seats': total,
        'available_seats': avail,
        'manual': bool(manual_seats and not rep.get('total_seats')),
        'error': rep.get('error'),
        'mgr': mgr,
    }

def _save_data(self):
        """حفظ بيانات الدعوات في MongoDB (لكل حساب على حدة)"""
        self.invites_data['allowed_emails'] = list(self.allowed_emails)
        _key = getattr(self, '_account_key', 'main')
        try:
            db.cgpt_invites_data.update_one(
                {'_id': _key},
                {'$set': {'data': self.invites_data}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"[CGPT] save_data DB err: {e}")
