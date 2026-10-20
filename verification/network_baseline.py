# Baseline from delivered version 5, for isolated before/after measurements only.

def _auto_sync_ext_stores():
    """مزامنة تلقائية دورية لكل متاجر API:
    - منتج جديد في المتجر الخارجي → إشعار الأدمن (هل تريd إضافته؟)
    - كمية/ستوك تغيّر لمنتج مضاف → تحديث تلقائي + برودكاست توفّر ستوك
    """
    try:
        stores = list(db.ext_stores.find())
    except Exception:
        return
    for store in stores:
        sid = str(store['_id'])
        data = _ext_api_get(store, '/products')
        prods = _ext_parse_products(data)
        # لو الرد فشل (None) نتخطّى المتجر — لا نصفّر بالخطأ
        if not _ext_valid_catalog(data):
            continue
        seen_ext_ids = set()
        for p in prods:
            _f = _ext_extract_fields(p)
            ext_id = _f['ext_id']
            if not ext_id:
                continue
            seen_ext_ids.add(ext_id)
            existing = db.ext_products.find_one({'store_id': sid, 'ext_id': ext_id})
            # Skip the original write/read path when an available product is unchanged.
            if (existing and _f['stock'] > 0 and _f['available'] is not False
                    and existing.get('_supplier_available') is True
                    and existing.get('stock') == _f['stock']
                    and existing.get('unlimited', False) == _f.get('unlimited', False)
                    and not any(existing.get(key) for key in
                                ('_new_notice', '_stock_notice', '_missing_since',
                                 '_supplier_empty_since'))
                    and (not _f['cost_price'] or abs(float(_f['cost_price']) -
                         float(existing.get('cost_price', existing.get('base_price', 0)) or 0)) <= 0.001)):
                continue
            if existing:
                db.ext_products.update_one({'_id': existing['_id']},
                    {'$set': {'unlimited': _f.get('unlimited', False)}})
            new_stock = _f['stock']
            if _f['available'] is False:
                new_stock = 0
            p_name = _f['name']


            if not existing:
                if new_stock <= 0:
                    continue
                already = db.ext_pending_new.find_one({'store_id': sid, 'ext_id': ext_id})
                if already:
                    continue
                try:
                    db.ext_pending_new.insert_one({
                        'store_id': sid, 'ext_id': ext_id, 'name': p_name,
                        'raw': p, 'created_at': int(time.time())
                    })
                    _notify_admins_new_ext_product(store, sid, ext_id, p_name, new_stock)
                except Exception as _ne:
                    logger.debug(f"notify new ext product err: {_ne}")
            else:
                # منتج مضاف: نتحقق من تغيّر التكلفة (سعر المتجر) أولاً — عالمي
                new_cost = _f['cost_price']
                old_cost = existing.get('cost_price', existing.get('base_price', 0)) or 0
                if new_cost and abs(float(new_cost) - float(old_cost)) > 0.001:
                    # المتجر غيّر سعر التكلفة → نعيd حساب سعر البيع بنفس النسبة تلقائياً
                    mt = existing.get('markup_type', 'percent')
                    mv = existing.get('markup_value', 0)
                    new_sell = _ext_compute_sell_price(float(new_cost), mt, mv)
                    db.ext_products.update_one({'_id': existing['_id']},
                        {'$set': {'cost_price': float(new_cost),
                                  'base_price': float(new_cost),
                                  'sell_price': new_sell}})
                    # إشعار الأدمن (رفع/خفض سعر من المتجر)
                    try:
                        direction = "📈 رفع" if float(new_cost) > float(old_cost) else "📉 خفض"
                        notify_admins(
                            f"{direction} <b>سعر من المتجر (API)</b>\n"
                            f"🏪 {html.escape(store.get('name',''))}\n"
                            f"📦 {html.escape(str(existing.get('name', p_name)))}\n"
                            f"💵 التكلفة: ${float(old_cost):.2f} → <b>${float(new_cost):.2f}</b>\n"
                            f"💰 سعرك الجديد (بالنسبة {mv}{'%' if mt=='percent' else '$'}): "
                            f"<b>${new_sell:.2f}</b>\n\n"
                            f"<i>أُعيد حساب سعرك تلقائياً.</i>"
                        )
                    except Exception:
                        pass
                    # لو ارتفع سعر البيع كثيراً، أو انخفض → نبثّ للعملاء
                    try:
                        _emit_event('product.updated', {
                            'source': 'external_api',
                            'product_id': f"ext_{existing['_id']}",
                            'name_ar': existing.get('name', p_name),
                            'name_en': existing.get('name', p_name),
                            'price': new_sell,
                            'is_manual': False, 'is_hidden': False,
                            'stock': new_stock,
                        }, product_id=f"ext_{existing['_id']}")
                    except Exception:
                        pass

                # منتج مضاف: نتحقق من تغيّر الستوك
                old_stock = existing.get('stock', 0) or 0
                try:
                    _ext_commit_stock(existing, new_stock)
                except Exception:
                    logger.exception('Could not update/queue external stock transition')

        # 🔴 المنتجات المضافة عندك لكن غير موجودة في رد المتجر = نفدت (out of stock)
        #    حسب الدوكس: المنتجات النافدة تُحذف من /products. فنصفّر ستوكها.
        try:
            for ep in db.ext_products.find({'store_id': sid}):
                if str(ep.get('ext_id', '')) not in seen_ext_ids:
                    _ext_mark_missing(ep)
        except Exception as _ze:
            logger.debug(f"zero out-of-stock err: {_ze}")

def _ext_api_get(store, path):
    """GET من متجر API خارجي — يكتشف الـ prefix تلقائياً. يرجّع JSON أو None."""
    if _ext_is_insight(store):
        ok, data = _insight_request(store, 'GET', path)
        return data if ok else None
    base = str(store.get('base_url', '')).rstrip('/')
    # المسارات المحتملة للـ prefix
    if '/api/v1' in base or '/shop-api/v1' in base:
        prefixes = ['']  # المستخدم كتبه كاملاً
    elif store.get('api_prefix'):
        prefixes = [store['api_prefix']]  # مكتشف مسبقاً
    else:
        # نجرّب كل المسارات الشائعة للـ prefix
        prefixes = ['/api/v1', '/shop-api/v1', '/api', '/v1', '/shop-api',
                    '/store-api/v1', '/reseller/v1', '']
    for prefix in prefixes:
        url = f"{base}{prefix}{path}"
        try:
            r = requests.get(url, headers=_ext_api_headers(store), timeout=15)
            if path == '/products':
                logger.info('[EXT_SYNC] catalog response bytes=%d status=%d',
                            len(r.content), r.status_code)
            if r.status_code == 200:
                # نحفظ الـ prefix الناجح للمرات القادمة
                if prefix and not store.get('api_prefix'):
                    try:
                        db.ext_stores.update_one({'_id': store['_id']},
                                                 {'$set': {'api_prefix': prefix}})
                        store['api_prefix'] = prefix
                    except Exception:
                        pass
                return r.json()
            if r.status_code != 404:
                logger.warning(f"[EXT_API] GET {path} → {r.status_code}")
        except Exception as e:
            logger.debug(f"[EXT_API] GET err: {e}")
    return None

