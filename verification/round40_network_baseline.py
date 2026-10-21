"""Previous update functions for offline before/after benchmarks only; never imported by the bot."""

def get_setting(key, default="Not Set"):
    res = db.settings.find_one({'key': key})
    return res['value'] if res else default

def background_referral_checker_v2():
    """Existing referrals keep their policy; new referrals require website verification."""
    try:
        logger.info('Negative balances available for administrator ledger review: %s',
                    db.users.count_documents({'balance': {'$lt': 0}}))
    except Exception:
        logger.exception('Could not read referral balance review count')
    time.sleep(90)
    while True:
        try:
            _ref_resume_financial()
            _ref_checker_tick()
        except Exception:
            logger.exception('Background referral verification cycle failed')
        _REF_SCAN_WAKE.wait(timeout=60)
        _REF_SCAN_WAKE.clear()

def check_forced_sub(uid, use_cache=True):
    """
    يفحص اشتراك المستخدم في كل القنوات الإجبارية.
    لازم يكون مشترك في كل القنوات — لو طلع من واحدة = غير مشترك.
    يرجع True (مشترك بالكل) / False (طلع من واحدة على الأقل) / None (خطأ اتصال).

    ⚡ للسرعة: نخزّن نتيجة "مشترك" مؤقتاً (5 دقائق) فلا نستعلم من تيليجرام
    في كل ضغطة — التنقّل يصير فورياً. النتيجة False لا تُخزَّن (نعيد الفحص
    فوراً حتى يشترك). التخزين يُمسح تلقائياً بعد المدة.
    """
    # ⚡ cache: لو عندنا نتيجة "مشترك" حديثة، نرجّعها فوراً بلا استعلام
    if use_cache:
        cached = _FORCED_SUB_CACHE.get(uid)
        if cached and cached[1] > time.time() and cached[0] is True:
            return True

    if uid == OWNER_ID: return True
    user_db = get_user_data_full(uid)
    if user_db and user_db.get('is_admin') == 1: return True
    chans = _fsub_channels()
    if not chans: return True

    had_error = False
    for c in chans:
        try:
            member = bot.get_chat_member(c['channel_id'], uid)
            joined = _fsub_joined(member)
            if joined is None:
                had_error = True
                continue
            if not joined:
                # غير مشترك: نمسح أي cache قديم ونرجّع False فوراً
                _FORCED_SUB_CACHE.pop(uid, None)
                return False  # طلع من قناة واحدة = مو مشترك أكيد
        except Exception:
            had_error = True
            continue  # نكمّل فحص باقي القنوات

    if had_error:
        return None  # ما نخزّن الغموض
    # مشترك بالكل → نخزّن للسرعة
    _FORCED_SUB_CACHE[uid] = (True, time.time() + _FORCED_SUB_CACHE_TTL)
    return True

def forced_sub_status(uid):
    """كل قناة/جروب إجباري مع حالة المستخدم: joined = True / False / None (ما قدرنا نفحص)."""
    out = []
    for c in db.required_channels.find():
        m = _fsub_meta(c)
        try:
            st = bot.get_chat_member(c['channel_id'], uid).status
            m['joined'] = st not in ('left', 'kicked')
        except Exception:
            m['joined'] = None
        out.append(m)
    return out
