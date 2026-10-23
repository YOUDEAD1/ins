"""Additive LARA STOCK/NSS integration. Child bots run exclusively on WWC.

Credentials never enter logs. Setup drafts are encrypted and expire in MongoDB.
The existing store ledger, referral history and payment handlers remain untouched.
"""
import datetime as dt
import hashlib
import hmac
import io
import json
import os
import pathlib
import re
import secrets
import threading
import time
import urllib.parse
from functools import wraps
from html.parser import HTMLParser
from string import Formatter
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import requests
from cryptography.fernet import Fernet
from pymongo.errors import DuplicateKeyError

TEXT = {
 'en': {
  'open':'🏪 Open your store','mine':'📊 My stores','super':'🛡 Super admin · stores',
  'intro':'Create your WWC store. Free stores sell NSS products and may edit names, descriptions, prices and margins. Adding products, accounts, stock or suppliers needs a subscription. Your NSS wallet pays supplier costs; your customers have separate wallets in your store.\n\nCreate a bot with @BotFather, open that bot and send /start, then send its token here. /cancelstore cancels setup.',
  'url':'Send a separate Supabase project URL: https://YOUR-PROJECT.supabase.co',
  'key':'Send the Supabase secret key (sb_secret_...) or legacy service_role from Project Settings → API. Do not use an anon/public key. Your message will be deleted when Telegram permits.',
  'sql':'Download this SQL file. Open your Supabase project → SQL Editor → New query. Paste the whole file and press Run. Then press Check & activate.',
  'activate':'✅ Check & activate','sqlbtn':'📄 Send SQL again','subscribe':'💎 Subscribe','resume':'▶️ Resume','pause':'⏸ Pause','unlock':'🔓 Owner unlock','grant':'🎁 Grant subscription','refresh':'🔄 Refresh','back':'⬅️ My stores','next':'Next ▶️','credentials':'🔑 Update store credentials',
  'empty':'You have no stores yet.','cancelled':'Setup cancelled. Existing stores are unchanged.','busy':'Setup is already saved. Finish it from My stores or use /cancelstore.',
  'active':'✅ Store activated. Open your bot, send /start, then /admin to manage it. Return here with /myshops.',
  'queued':'Activation queued: the host reached its configured capacity. Your store bot will notify you when a slot is available. No need to resubmit credentials.',
  'heading':'Store','state':'Status','plan':'Plan','members':'Registered accounts','orders':'Paid orders','revenue':'Sales revenue','profit':'Known supplier profit','profitnote':'Known profit only includes orders with recorded supplier costs, before other expenses.','strikes':'Inactivity pauses','until':'Paid until','free':'Free · no expiry',
  'configured':'Workers wake on customer messages. A cold start can take a few seconds.','error':'Action unavailable. Reason: {reason}','notready':'The platform owner must configure WWC_PLATFORM_URL, WWC_PLATFORM_SECRET and WWC_DRAFT_ENCRYPTION_KEY.',
  'pending':'Payment saved. Subscription activation is queued; do not pay again. Use Refresh to check.','paid':'✅ Subscription activated. Open your store admin panel.','invoice':'WWC store subscription','invoicedesc':'Full WWC store administration for {days} days. Store data is retained when a plan expires.','confirm':'Confirm this action for the store.','yes':'✅ Confirm','invalid':'Invalid value. Check the requested format and send it again.'
 },
 'ar': {
  'open':'🏪 افتح متجرك','mine':'📊 متاجري','super':'🛡 سوبر أدمن · المتاجر',
  'intro':'افتح متجرك على WWC. المجاني يبيع منتجات NSS ويسمح بتعديل الاسم والوصف والسعر ونسبة الربح. إضافة المنتجات والحسابات والمخزون والموردين تحتاج اشتراكًا. تكلفة المورد تخصم من رصيدك في NSS، وأرصدة عملاء متجرك مستقلة.\n\nأنشئ بوتًا من @BotFather وافتحه وأرسل له /start، ثم أرسل توكنه هنا. للإلغاء: /cancelstore',
  'url':'أرسل رابط مشروع Supabase مستقل: https://YOUR-PROJECT.supabase.co',
  'key':'أرسل المفتاح السري sb_secret_... أو service_role من Project Settings ثم API. لا تستخدم المفتاح العام/anon. سنحذف الرسالة عندما يسمح Telegram.',
  'sql':'نزّل ملف SQL. افتح مشروعك في Supabase ثم SQL Editor ثم New query. الصق الملف كاملًا واضغط Run. بعد نجاح التنفيذ اضغط فحص وتفعيل.',
  'activate':'✅ فحص وتفعيل','sqlbtn':'📄 إرسال SQL مجددًا','subscribe':'💎 اشتراك','resume':'▶️ تشغيل','pause':'⏸ إيقاف','unlock':'🔓 فتح من المالك','grant':'🎁 منح اشتراك','refresh':'🔄 تحديث','back':'⬅️ متاجري','next':'التالي ▶️','credentials':'🔑 تحديث بيانات المتجر',
  'empty':'لا توجد متاجر لك بعد.','cancelled':'تم إلغاء خطوات الإنشاء. المتاجر الموجودة لم تتغير.','busy':'خطوات إنشاء محفوظة بالفعل. أكملها من متاجري أو أرسل /cancelstore.',
  'active':'✅ تم تفعيل المتجر. افتح بوتك وأرسل /start ثم /admin للإدارة. للمتابعة هنا: /myshops',
  'queued':'حُفظ طلب التفعيل في الطابور لأن الاستضافة بلغت الحد المحدد. سيصلك إشعار في بوت متجرك عند توفر مكان. لا يلزم إعادة إرسال البيانات.',
  'heading':'المتجر','state':'الحالة','plan':'الخطة','members':'الحسابات المسجلة','orders':'الطلبات المدفوعة','revenue':'إيراد المبيعات','profit':'ربح المورد المعروف','profitnote':'الربح المعروف يشمل الطلبات ذات تكلفة المورد المسجلة فقط، وقبل بقية المصاريف.','strikes':'إيقافات الخمول','until':'المدفوع حتى','free':'مجاني · بلا انتهاء',
  'configured':'عامل التشغيل يستيقظ عند رسائل العملاء. قد تستغرق البداية بضع ثوانٍ.','error':'تعذر تنفيذ الإجراء. السبب: {reason}','notready':'يلزم عند المالك ضبط WWC_PLATFORM_URL وWWC_PLATFORM_SECRET وWWC_DRAFT_ENCRYPTION_KEY.',
  'pending':'تم حفظ الدفع. التفعيل في الطابور؛ لا تدفع مرة ثانية. اضغط تحديث للمتابعة.','paid':'✅ تم تفعيل الاشتراك. افتح لوحة أدمن متجرك.','invoice':'اشتراك متجر WWC','invoicedesc':'إدارة متجر WWC كاملة لمدة {days} يومًا. تبقى البيانات عند انتهاء الاشتراك.','confirm':'تأكيد تنفيذ هذا الإجراء على المتجر.','yes':'✅ موافق','invalid':'القيمة غير صحيحة. راجع الصيغة المطلوبة وأرسلها مجددًا.'
 },
 'vi': {
  'open':'🏪 Mở cửa hàng','mine':'📊 Cửa hàng của tôi','super':'🛡 Quản trị nền tảng',
  'intro':'Tạo cửa hàng WWC. Gói miễn phí bán sản phẩm NSS, cho đổi tên, mô tả, giá và lợi nhuận. Thêm sản phẩm, tài khoản, kho hoặc nhà cung cấp cần trả phí. Chi phí NSS trừ từ ví NSS riêng của bạn; ví khách trong cửa hàng độc lập.\n\nTạo bot tại @BotFather, mở bot đó và gửi /start, sau đó gửi token ở đây. /cancelstore để hủy.',
  'url':'Gửi URL dự án Supabase riêng: https://YOUR-PROJECT.supabase.co','key':'Gửi khóa bí mật sb_secret_... hoặc service_role từ Project Settings → API. Không dùng khóa anon/công khai. Tin nhắn sẽ được xóa khi Telegram cho phép.',
  'sql':'Tải SQL này. Mở dự án Supabase → SQL Editor → New query. Dán toàn bộ tệp rồi nhấn Run. Sau đó nhấn Kiểm tra và kích hoạt.',
  'activate':'✅ Kiểm tra và kích hoạt','sqlbtn':'📄 Gửi lại SQL','subscribe':'💎 Đăng ký','resume':'▶️ Chạy lại','pause':'⏸ Tạm dừng','unlock':'🔓 Chủ nền tảng mở khóa','grant':'🎁 Tặng gói','refresh':'🔄 Làm mới','back':'⬅️ Cửa hàng của tôi','next':'Tiếp ▶️','credentials':'🔑 Đổi thông tin cửa hàng',
  'empty':'Bạn chưa có cửa hàng.','cancelled':'Đã hủy thiết lập. Cửa hàng hiện có không thay đổi.','busy':'Đã có thiết lập lưu. Hoàn tất từ Cửa hàng của tôi hoặc gửi /cancelstore.',
  'active':'✅ Đã kích hoạt. Mở bot của bạn, gửi /start rồi /admin để quản lý. Dùng /myshops để theo dõi.','queued':'Đã xếp hàng kích hoạt do máy chủ đủ công suất. Bot cửa hàng sẽ báo khi có chỗ. Không cần gửi lại thông tin.',
  'heading':'Cửa hàng','state':'Trạng thái','plan':'Gói','members':'Tài khoản đăng ký','orders':'Đơn đã trả','revenue':'Doanh thu','profit':'Lợi nhuận nhà cung cấp đã biết','profitnote':'Chỉ gồm đơn có giá vốn ghi nhận, chưa trừ chi phí khác.','strikes':'Lần dừng do không hoạt động','until':'Trả phí đến','free':'Miễn phí · không hết hạn',
  'configured':'Tiến trình thức dậy khi khách gửi tin nhắn. Khởi động có thể mất vài giây.','error':'Không thể thực hiện. Lý do: {reason}','notready':'Chủ cần đặt WWC_PLATFORM_URL, WWC_PLATFORM_SECRET và WWC_DRAFT_ENCRYPTION_KEY.',
  'pending':'Đã lưu thanh toán, đang chờ kích hoạt. Đừng trả tiền lần nữa; nhấn Làm mới.','paid':'✅ Đã kích hoạt gói. Mở quản trị cửa hàng.','invoice':'Gói cửa hàng WWC','invoicedesc':'Quản trị WWC đầy đủ trong {days} ngày. Dữ liệu giữ lại khi hết gói.','confirm':'Xác nhận thao tác cho cửa hàng.','yes':'✅ Xác nhận','invalid':'Giá trị không hợp lệ. Kiểm tra định dạng rồi gửi lại.'
 },
 'zh': {
  'open':'🏪 开设商店','mine':'📊 我的商店','super':'🛡 平台超级管理员',
  'intro':'创建 WWC 商店。免费套餐销售 NSS 商品，可修改名称、描述、售价和利润率。添加商品、账户、库存或供应商需要订阅。NSS 成本从您的独立 NSS 钱包支付，客户的本店钱包独立。\n\n在 @BotFather 创建机器人，打开它并发送 /start，然后在此发送令牌。/cancelstore 可取消。',
  'url':'发送独立 Supabase 项目网址：https://YOUR-PROJECT.supabase.co','key':'发送 Project Settings → API 的秘密密钥 sb_secret_... 或 service_role。不要使用 anon/公开密钥。Telegram 允许时将删除消息。',
  'sql':'下载 SQL 文件。打开 Supabase 项目 → SQL Editor → New query，粘贴完整文件并点击 Run，然后点击检查并激活。',
  'activate':'✅ 检查并激活','sqlbtn':'📄 再次发送 SQL','subscribe':'💎 订阅','resume':'▶️ 恢复','pause':'⏸ 暂停','unlock':'🔓 平台所有者解锁','grant':'🎁 赠送订阅','refresh':'🔄 刷新','back':'⬅️ 我的商店','next':'下一页 ▶️','credentials':'🔑 更新商店凭据',
  'empty':'您还没有商店。','cancelled':'已取消设置，现有商店不变。','busy':'已有保存的设置，请在我的商店完成或发送 /cancelstore 取消。',
  'active':'✅ 已激活。打开您的机器人并发送 /start 和 /admin 管理。使用 /myshops 返回查看。','queued':'激活已排队，主机已达容量上限。空位可用时商店机器人会通知您，无需重新提交凭据。',
  'heading':'商店','state':'状态','plan':'套餐','members':'注册账户','orders':'已付款订单','revenue':'销售收入','profit':'已知供应商利润','profitnote':'仅包括记录了供应商成本的订单，尚未扣除其他费用。','strikes':'不活跃暂停次数','until':'付费有效期','free':'免费 · 无到期日',
  'configured':'收到客户消息时启动工作进程。冷启动可能需要几秒。','error':'操作失败，原因：{reason}','notready':'所有者需设置 WWC_PLATFORM_URL、WWC_PLATFORM_SECRET 和 WWC_DRAFT_ENCRYPTION_KEY。',
  'pending':'付款已保存，订阅排队激活中。请勿重复付款，点击刷新查看。','paid':'✅ 订阅已激活，请打开商店管理面板。','invoice':'WWC 商店订阅','invoicedesc':'{days} 天完整 WWC 管理功能，套餐到期后保留商店数据。','confirm':'确认对商店执行此操作。','yes':'✅ 确认','invalid':'无效输入，请检查格式后重试。'
 }
}

# Wallet checkout is the default. Old Stars receipts remain recoverable.
WALLET_TEXT = {
 'ar': {
  'wallet_title':'💎 اشتراك المتجر من محفظة NSS',
  'wallet_quote':'المتجر: @{bot}\nالمدة: {days} يومًا\nالسعر: ${amount}\nرصيد NSS الحالي: ${balance}\n\nبعد التأكيد يُخصم المبلغ من محفظتك في NSS ويُفعّل الاشتراك أو تُمدد مدته.',
  'wallet_pay':'✅ تأكيد الدفع ${amount} من رصيد NSS','wallet_cancel':'❌ إلغاء الشراء','wallet_deposit':'💰 شحن محفظة NSS',
  'wallet_insufficient':'رصيد NSS غير كافٍ. المطلوب ${amount} ورصيدك ${balance}. لم يُخصم شيء؛ اشحن محفظتك ثم أكد الدفع.',
  'wallet_expired':'انتهى عرض السعر أو أُلغي. اضغط اشتراك لعرض سعر جديد. لم يُخصم شيء.',
  'wallet_disabled':'لم يحدد مالك المنصة سعر الاشتراك من المحفظة بعد. يرجى المحاولة لاحقًا.',
  'wallet_cancelled':'تم إلغاء شراء الاشتراك. لم يُخصم شيء.',
  'wallet_receipt':'عملية الدفع: {reference}\nالمبلغ المخصوم: ${amount}\nرصيد NSS بعد الخصم: ${balance}',
  'runtime':'فحص التشغيل','runtime_ready':'🟢 متصل وجاهز','runtime_cold':'🟢 متصل؛ يستيقظ عند الطلب','runtime_bad':'🔴 لم يجتز فحص الاتصال','runtime_unknown':'⚪ تعذر التحقق الآن'
 },
 'en': {
  'wallet_title':'💎 Store subscription · NSS wallet',
  'wallet_quote':'Store: @{bot}\nDuration: {days} days\nPrice: ${amount}\nCurrent NSS balance: ${balance}\n\nConfirm to pay from your NSS wallet and activate or extend the subscription.',
  'wallet_pay':'✅ Pay ${amount} from NSS balance','wallet_cancel':'❌ Cancel purchase','wallet_deposit':'💰 Add funds to NSS',
  'wallet_insufficient':'Insufficient NSS balance. Required: ${amount}; available: ${balance}. Nothing was charged. Add funds, then confirm payment.',
  'wallet_expired':'This quote expired or was cancelled. Press Subscribe for a new quote. Nothing was charged.',
  'wallet_disabled':'The platform owner has not set the wallet subscription price yet. Please try again later.',
  'wallet_cancelled':'Subscription purchase cancelled. Nothing was charged.',
  'wallet_receipt':'Payment reference: {reference}\nCharged: ${amount}\nNSS balance after payment: ${balance}',
  'runtime':'Runtime check','runtime_ready':'🟢 Connected and ready','runtime_cold':'🟢 Connected; wakes on demand','runtime_bad':'🔴 Connection check failed','runtime_unknown':'⚪ Could not verify now'
 },
 'vi': {
  'wallet_title':'💎 Đăng ký cửa hàng · Ví NSS',
  'wallet_quote':'Cửa hàng: @{bot}\nThời hạn: {days} ngày\nGiá: ${amount}\nSố dư NSS: ${balance}\n\nXác nhận thanh toán bằng ví NSS để kích hoạt hoặc gia hạn.',
  'wallet_pay':'✅ Trả ${amount} bằng ví NSS','wallet_cancel':'❌ Hủy mua','wallet_deposit':'💰 Nạp ví NSS',
  'wallet_insufficient':'Số dư NSS không đủ. Cần ${amount}; hiện có ${balance}. Chưa trừ tiền. Hãy nạp ví rồi xác nhận.',
  'wallet_expired':'Báo giá đã hết hạn hoặc bị hủy. Nhấn Đăng ký để lấy giá mới. Chưa trừ tiền.',
  'wallet_disabled':'Chủ nền tảng chưa đặt giá đăng ký qua ví. Vui lòng thử lại sau.',
  'wallet_cancelled':'Đã hủy mua đăng ký. Chưa trừ tiền.',
  'wallet_receipt':'Mã thanh toán: {reference}\nĐã trừ: ${amount}\nSố dư NSS sau thanh toán: ${balance}',
  'runtime':'Kiểm tra chạy','runtime_ready':'🟢 Đã kết nối và sẵn sàng','runtime_cold':'🟢 Đã kết nối; chạy khi có yêu cầu','runtime_bad':'🔴 Kiểm tra kết nối thất bại','runtime_unknown':'⚪ Chưa xác minh được'
 },
 'zh': {
  'wallet_title':'💎 商店订阅 · NSS 钱包',
  'wallet_quote':'商店：@{bot}\n期限：{days} 天\n价格：${amount}\n当前 NSS 余额：${balance}\n\n确认后将从 NSS 钱包扣款，并激活或延长订阅。',
  'wallet_pay':'✅ 从 NSS 余额支付 ${amount}','wallet_cancel':'❌ 取消购买','wallet_deposit':'💰 为 NSS 钱包充值',
  'wallet_insufficient':'NSS 余额不足。需要 ${amount}；当前 ${balance}。未扣款。请先充值再确认。',
  'wallet_expired':'报价已过期或取消。点击订阅获取新报价。未扣款。',
  'wallet_disabled':'平台所有者尚未设置钱包订阅价格，请稍后重试。',
  'wallet_cancelled':'已取消购买订阅，未扣款。',
  'wallet_receipt':'支付编号：{reference}\n扣款金额：${amount}\n扣款后 NSS 余额：${balance}',
  'runtime':'运行检查','runtime_ready':'🟢 已连接并就绪','runtime_cold':'🟢 已连接，按需唤醒','runtime_bad':'🔴 连接检查失败','runtime_unknown':'⚪ 暂时无法确认'
 }
}
for _language, _copy in WALLET_TEXT.items():
    TEXT[_language].update(_copy)
    TEXT[_language]['dashboard_template']='{details}'
for _language,_copy in {
    'ar':{'invalid_invoice':'فاتورة الاشتراك غير صحيحة أو استُخدمت سابقًا.','start_required':'أرسل /start أولًا لتسجيل حسابك.'},
    'en':{'invalid_invoice':'Invalid or already used subscription invoice.','start_required':'Send /start first to register your account.'},
    'vi':{'invalid_invoice':'Hóa đơn đăng ký không hợp lệ hoặc đã dùng.','start_required':'Gửi /start trước để đăng ký tài khoản.'},
    'zh':{'invalid_invoice':'订阅账单无效或已使用。','start_required':'请先发送 /start 注册账户。'},
}.items():TEXT[_language].update(_copy)


for _language,_value in {
    'en':'Open the store manager to create your store, view statistics and manage its subscription.',
    'ar':'افتح بوت إدارة المتاجر لإنشاء متجرك ومتابعة الإحصاءات وإدارة الاشتراك.',
    'vi':'Mở bot quản lý để tạo cửa hàng, xem thống kê và quản lý đăng ký.',
    'zh':'打开管理机器人，创建商店、查看统计及管理订阅。',
}.items():TEXT[_language]['manager_redirect']=_value


BUTTONS = frozenset(('open','mine','super','activate','sqlbtn','subscribe','resume','pause','unlock','grant',
    'refresh','back','next','credentials','yes','wallet_pay','wallet_cancel','wallet_deposit'))


class TemplateHTML(HTMLParser):
    allowed = frozenset(('b','strong','i','em','u','ins','s','strike','del','code','pre','a',
                         'tg-emoji','tg-spoiler','span','blockquote'))
    def __init__(self):
        super().__init__(convert_charrefs=False);self.stack=[]
    def handle_starttag(self,tag,attrs):
        if tag not in self.allowed:raise ValueError('Unsupported HTML tag: '+tag)
        data=dict(attrs)
        if tag=='tg-emoji' and not re.fullmatch(r'\d{5,25}',data.get('emoji-id','')):raise ValueError('Invalid Premium emoji')
        if tag=='a' and not re.match(r'^(https?://|tg://|mailto:)',data.get('href','')):raise ValueError('Invalid link')
        self.stack.append(tag)
    def handle_endtag(self,tag):
        if not self.stack or self.stack.pop()!=tag:raise ValueError('Unbalanced HTML')
    def handle_startendtag(self,tag,attrs):raise ValueError('Unsupported HTML tag')


def fields(text):
    result=set()
    for _,name,spec,conversion in Formatter().parse(text):
        if name is None:continue
        if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',name) or spec or conversion:
            raise ValueError('Use simple named placeholders only')
        result.add(name)
    return result


def validate_copy(value,default,button=False,key=None):
    if not isinstance(value,str) or not value.strip():raise ValueError('Text must not be empty')
    if fields(value)!=fields(default):raise ValueError('Keep placeholders: '+' '.join('{'+key+'}' for key in sorted(fields(default))))
    if len(value.encode('utf-16-le'))//2>(64 if button else 3500):raise ValueError('Text too long')
    if key=='wwc.invoice' and len(value)>32:raise ValueError('Invoice title maximum: 32 characters')
    if key=='wwc.invoicedesc' and len(value)>255:raise ValueError('Invoice description maximum: 255 characters')
    if not button:
        parser=TemplateHTML();parser.feed(value);parser.close()
        if parser.stack:raise ValueError('Unbalanced HTML')


def html_chunks(text,limit=3500):
    """Bound Telegram UTF-16 message length without breaking tags or entities."""
    chunks=[];stack=[];buffer=[];units=0;content=False
    def width(value):return len(value.encode('utf-16-le'))//2
    def flush():
        nonlocal buffer,units,content
        if content:chunks.append(''.join(buffer)+''.join('</'+tag+'>' for tag,_ in reversed(stack)))
        prefix=''.join(opening for _,opening in stack);buffer=[prefix];units=width(prefix);content=False
    def emit(value,reserve=0,is_content=False):
        nonlocal units,content
        closing=sum(len(tag)+3 for tag,_ in stack)
        if units+width(value)+closing+reserve>limit:flush()
        buffer.append(value);units+=width(value);content=content or is_content
    for match in re.finditer(r'<[^>]*>|&(?:#[0-9]+|#x[0-9a-fA-F]+|[a-zA-Z]+);|[^<&]+|[<&]',text):
        token=match.group()
        closing=re.fullmatch(r'</([a-z-]+)\s*>',token)
        opening=re.match(r'<([a-z-]+)(?:\s|>)',token)
        if closing:
            if stack and stack[-1][0]==closing[1]:stack.pop()
            buffer.append(token);units+=width(token)
        elif opening:
            emit(token,len(opening[1])+3);stack.append((opening[1],token))
        elif token.startswith('&'):emit(token,is_content=True)
        else:
            for char in token:emit(char,is_content=True)
    if content or not chunks:chunks.append(''.join(buffer)+''.join('</'+tag+'>' for tag,_ in reversed(stack)))
    return chunks



def native_view(method):
    """Share the original CMS button snapshot across one platform interaction."""
    @wraps(method)
    def wrapped(self,*args,**kwargs):
        with self.g['_cms_button_view']():
            return method(self,*args,**kwargs)
    return wrapped


def wallet_amount(value):
    """Return integer microdollars, rejecting zero, nonfinite or rounded prices."""
    try:
        amount=Decimal(str(value))
        units=amount*1_000_000
        if not amount.is_finite() or units!=units.to_integral_value() or not 0<units<=1_000_000_000_000:
            raise ValueError('INVALID_WALLET_PRICE')
        return int(units)
    except (InvalidOperation,TypeError,ValueError,OverflowError):
        raise ValueError('INVALID_WALLET_PRICE') from None


def money_text(value):
    amount=Decimal(str(value)).quantize(Decimal('0.000001'),rounding=ROUND_HALF_UP)
    return format(amount,'.6f').rstrip('0').rstrip('.') if amount%1 else format(amount,'.2f')

def signed_headers(secret, path, body, timestamp=None, nonce=None):
    timestamp = str(timestamp or int(time.time()))
    nonce = nonce or secrets.token_urlsafe(24)
    value = '\n'.join(('POST', path, timestamp, nonce, hashlib.sha256(body).hexdigest()))
    return {'Content-Type':'application/json','X-WWC-Time':timestamp,'X-WWC-Nonce':nonce,
            'X-WWC-Signature':hmac.new(secret.encode(),value.encode(),hashlib.sha256).hexdigest()}

class PlatformClient:
    def __init__(self):
        self.url = os.getenv('WWC_PLATFORM_URL','').rstrip('/')
        self.secret = os.getenv('WWC_PLATFORM_SECRET','')
        self.session = requests.Session()
        if self.url:
            p=urllib.parse.urlsplit(self.url)
            if p.scheme!='https' or p.username or p.password or p.query or p.fragment or p.path not in ('','/'):
                raise ValueError('WWC_PLATFORM_URL must be an HTTPS origin')
    @property
    def enabled(self):
        return bool(self.url and len(self.secret)>=32)
    def call(self, action, actor, **data):
        if not self.enabled:
            raise RuntimeError('PLATFORM_NOT_CONFIGURED')
        path='/platform/v1/'+action
        body=json.dumps(dict(data,actor_id=int(actor)),ensure_ascii=False,separators=(',',':')).encode()
        r=self.session.post(self.url+path,data=body,headers=signed_headers(self.secret,path,body),timeout=(5,40),allow_redirects=False)
        if len(r.content)>2_000_000:
            raise RuntimeError('RESPONSE_TOO_LARGE')
        result=r.json()
        if r.status_code>=300 or not result.get('ok'):
            code=str(result.get('error','PLATFORM_UNAVAILABLE'))
            raise RuntimeError(code if re.fullmatch(r'[A-Z_0-9]{1,100}',code) else 'PLATFORM_UNAVAILABLE')
        return result

class StorePlatform:
    def __init__(self,g):
        self.g=g;self.bot=g['bot'];self.db=g['db'];self.types=g['types'];self.client=PlatformClient()
        key=os.getenv('WWC_DRAFT_ENCRYPTION_KEY') or os.getenv('SESSION_ENCRYPTION_KEY','')
        self.cipher=Fernet(key.encode()) if key else None
        self.owner=int(g['OWNER_ID'])
        self.super_ids={self.owner}|{int(v) for v in os.getenv('WWC_PLATFORM_OWNERS','').split(',') if v.strip().isdigit()}
        self.indexed=False;self.outbox_lock=threading.Lock();self.outbox_event=threading.Event();self.draft_ids=set()
        if self.client.enabled and self.cipher:
            self.draft_ids={d['_id'] for d in self.db.wwc_drafts.find({'expires_at':{'$gt':dt.datetime.now(dt.timezone.utc)}},{'_id':1}).limit(1000)}
        self.delivery=threading.local()
        self.manager_username=os.getenv('WWC_MANAGER_USERNAME','').strip().lstrip('@')
        if self.manager_username and not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{4,31}',self.manager_username):raise ValueError('Invalid WWC_MANAGER_USERNAME')
        self.customization_migrated=False
        self.customization_lock=threading.Lock()
        self.register_native_customization()
    def register_native_customization(self):
        """Extend the existing LANG/DEFAULT_BUTTONS and edit_txt/edit_btn flows."""
        titles={'intro':'خطوات فتح المتجر','sql':'تعليمات SQL والتفعيل',
                'wallet_quote':'عرض سعر الاشتراك والرصيد','wallet_receipt':'إيصال دفع المحفظة',
                'dashboard_template':'تنسيق لوحة المتجر','error':'رسالة تعذر تنفيذ الإجراء'}
        for key in TEXT['en']:
            native_key='wwc.'+key
            title=titles.get(key,re.sub(r'<[^>]*>|\{[^}]*\}','',TEXT['ar'][key]).split('\n')[0][:46])
            self.g['WWC_CMS_TEXTS'][native_key]=(title,TEXT['ar'][key],TEXT['en'][key])
            if key in BUTTONS:self.g['WWC_CMS_BUTTONS'][native_key]=self.g['WWC_CMS_TEXTS'][native_key]
            for lang,defaults in TEXT.items():
                self.g['LANG'].setdefault(lang,{})[native_key]=defaults[key]
                if key in BUTTONS:self.g['DEFAULT_BUTTONS'].setdefault(lang,{})[native_key]=defaults[key]
        self.g['WWC_CMS_VALIDATE']=validate_copy
    def migrate_customization(self):
        """Read the retired editor once; never overwrite native CMS settings."""
        if self.customization_migrated:return
        with self.customization_lock:
            if self.customization_migrated:return
            marker={'key':'wwc.cms_native_migrated'}
            if self.db.settings.find_one(marker):
                self.customization_migrated=True;return
            native_keys=['wwc.'+key for key in BUTTONS]
            labels={(row.get('lang'),row['key'][4:]):row.get('value') for row in
                    self.db.custom_texts.find({'key':{'$in':native_keys}})}
            styles={(row.get('lang'),row.get('key')):row for row in self.db.wwc_cms_buttons.find({})}
            for lang,key in set(labels)|set(styles):
                if lang not in TEXT or key not in BUTTONS:continue
                values={};value=labels.get((lang,key))
                if value:
                    try:validate_copy(value,TEXT['en'][key],button=True)
                    except ValueError:pass
                    else:values['text']=value
                old=styles.get((lang,key),{})
                if re.fullmatch(r'\d{5,25}',str(old.get('emoji_id') or '')):values['emoji_id']=str(old['emoji_id'])
                if old.get('style') in ('primary','success','danger'):values['style']=old['style']
                if values:self.db.custom_buttons.update_one({'lang':lang,'key':'wwc.'+key},{'$setOnInsert':values},upsert=True)
            if labels or styles:
                self.db.settings.update_one(marker,{'$set':{'value':1}},upsert=True)
            self.customization_migrated=True
    @native_view
    def text(self,uid):
        self.migrate_customization()
        lang=self.g['get_lang'](uid);lang=lang if lang in TEXT else 'en'
        values={}
        for key,default in TEXT[lang].items():
            value=(self.g['get_btn_data'](uid,'wwc.'+key)[0] if key in BUTTONS
                   else self.g['get_text'](uid,'wwc.'+key))
            try:validate_copy(value,TEXT['en'][key],button=key in BUTTONS,key='wwc.'+key)
            except ValueError:value=default
            values[key]=value
        return values
    def ensure_indexes(self):
        if self.indexed:return
        self.db.wwc_drafts.create_index('expires_at',expireAfterSeconds=0)
        self.db.wwc_subscriptions.create_index('charge_id',unique=True,sparse=True)
        self.db.wwc_subscriptions.create_index([('state',1),('next_retry',1)])
        self.db.wwc_subscriptions.create_index([('payer_id',1),('shop_id',1),('state',1)])
        self.indexed=True
    @native_view
    def button(self,uid,key,target,label=None):
        self.migrate_customization()
        params={'url':target} if target.startswith('https://') else {'callback_data':target}
        button=self.g['create_btn'](uid,'wwc.'+key,**params)
        if label is not None:button.text=label
        return button
    @native_view
    def keyboard(self,rows,uid=None):
        kb=self.types.InlineKeyboardMarkup()
        for row in rows:
            buttons=[]
            for item in row:
                label,target=item[:2];key=item[2] if len(item)==3 else None
                if key:
                    buttons.append(self.button(uid,key,target,label));continue
                b=self.types.InlineKeyboardButton(label,**({'url':target} if target.startswith('https://') else {'callback_data':target}))
                if hasattr(b,'style'):b.style='primary'
                buttons.append(b)
            kb.row(*buttons)
        return kb
    @native_view
    def send(self,uid,text,rows=None):
        capture=getattr(self.delivery,'capture',None)
        if capture is not None:
            capture.append((uid,text,rows));return None
        parts=html_chunks(text) if len(text.encode('utf-16-le'))//2>3900 else [text]
        result=None
        for index,part in enumerate(parts):
            result=self.bot.send_message(uid,part,parse_mode='HTML',reply_markup=self.keyboard(rows,uid) if rows and index==len(parts)-1 else None)
        return result
    def error(self,uid,exc):
        reason=str(exc)
        if not re.fullmatch(r'[A-Z_0-9]{1,100}',reason):reason='PLATFORM_UNAVAILABLE'
        self.send(uid,self.text(uid)['error'].format(reason=reason))
    def guard(self,uid):
        if self.g.get('is_user_banned',lambda _:False)(uid):return False
        if not self.client.enabled or not self.cipher:
            self.send(uid,self.text(uid)['notready']);return False
        self.ensure_indexes();return True
    def draft(self,uid):
        d=self.db.wwc_drafts.find_one({'_id':uid,'expires_at':{'$gt':dt.datetime.now(dt.timezone.utc)}})
        if not d:self.draft_ids.discard(uid);return None
        try:return dict(json.loads(self.cipher.decrypt(d['box'].encode())),step=d['step'])
        except Exception:self.db.wwc_drafts.delete_one({'_id':uid});self.draft_ids.discard(uid);return None
    def save_draft(self,uid,step,data):
        self.draft_ids.add(uid)
        self.db.wwc_drafts.update_one({'_id':uid},{'$set':{'step':step,'box':self.cipher.encrypt(json.dumps(data).encode()).decode(),'expires_at':dt.datetime.now(dt.timezone.utc)+dt.timedelta(minutes=20)}},upsert=True)
    def source_connection(self,uid):
        d=self.db.api_keys.find_one({'user_id':uid,'is_active':True,'wwc_platform':True})
        if not d:
            self.db.api_keys.update_one({'_id':'wwc_user_'+str(uid)},{'$setOnInsert':{'user_id':uid,'api_key':self.g['_generate_api_key'](),'is_active':True,'wwc_platform':True,'created_at':int(time.time())}},upsert=True)
            d=self.db.api_keys.find_one({'_id':'wwc_user_'+str(uid),'is_active':True})
        if not d:raise RuntimeError('NSS_API_KEY_DISABLED')
        base=self.g['_get_server_url']()
        if not base or not base.startswith('https://'):raise RuntimeError('NSS_PUBLIC_HTTPS_URL_REQUIRED')
        return base.rstrip('/')+'/'+self.g['_get_api_gateway'](),d['api_key']
    @native_view
    def create(self,uid):
        if not self.guard(uid):return
        if not self.g['get_user_data_full'](uid):self.send(uid,self.text(uid)['start_required']);return
        if self.draft(uid):self.send(uid,self.text(uid)['busy']);return
        self.bot.clear_step_handler_by_chat_id(chat_id=uid)
        self.save_draft(uid,'token',{});self.send(uid,self.text(uid)['intro'])
    def receive(self,message):
        uid=message.from_user.id
        if not self.guard(uid):return
        data=self.draft(uid)
        if not data:return
        value=(message.text or '').strip();t=self.text(uid)
        try:self.bot.delete_message(message.chat.id,message.message_id)
        except Exception:pass
        if data['step']=='token':
            if not re.fullmatch(r'\d{6,15}:[A-Za-z0-9_-]{25,80}',value):self.send(uid,t['invalid']);return
            self.save_draft(uid,'url',{'bot_token':value});self.send(uid,t['url'])
        elif data['step']=='url':
            if not re.fullmatch(r'https://[a-z0-9-]+\.supabase\.co/?',value):self.send(uid,t['invalid']);return
            self.save_draft(uid,'key',{'bot_token':data['bot_token'],'database_url':value.rstrip('/')});self.send(uid,t['key'])
        elif data['step']=='key':
            if not 30<=len(value)<=4096 or not value.startswith(('eyJ','sb_secret_')):self.send(uid,t['invalid']);return
            source_url,source_key=self.source_connection(uid)
            result=self.client.call('register',uid,bot_token=data['bot_token'],database_url=data['database_url'],database_key=value,source_url=source_url,source_key=source_key,lang=self.g['get_lang'](uid))
            self.db.wwc_drafts.delete_one({'_id':uid});self.draft_ids.discard(uid);self.sql(uid,result['shop']['id'])
    @native_view
    def receive_safe(self,message):
        try:self.receive(message)
        except Exception as exc:self.error(message.from_user.id,exc)
    def _own_shop(self,uid,shop_id):
        return self.client.call('info',uid,shop_id=shop_id)['shop']
    @native_view
    def sql(self,uid,shop_id=None):
        if not self.guard(uid):return
        shop=self._own_shop(uid,shop_id) if shop_id else None
        file=pathlib.Path(__file__).with_name('WWC-STORE-SETUP.sql')
        content=file.read_bytes() if file.is_file() else self.client.call('schema',uid)['sql'].encode()
        doc=io.BytesIO(content);doc.name='WWC-STORE-SETUP.sql';instructions=self.text(uid)['sql']
        self.bot.send_document(uid,doc,**({'caption':instructions,'parse_mode':'HTML'} if len(instructions)<900 else {}))
        if len(instructions)>=900:self.send(uid,instructions)
        if shop:self.send(uid,'@'+shop['bot_username'],[[(self.text(uid)['activate'],'wwc:activate:'+shop_id,'activate')],[(self.text(uid)['back'],'wwc:list','back')]])
    @native_view
    def listing(self,uid,all_shops=False,offset=0):
        if not self.guard(uid):return
        result=self.client.call('list',uid,all=bool(all_shops and uid in self.super_ids),offset=offset)
        rows=[[('@'+s['bot_username']+' · '+s['status'],'wwc:show:'+s['id'])] for s in result['shops']]
        if result.get('next_offset') is not None:rows.append([(self.text(uid)['next'],'wwc:list:'+str(result['next_offset'])+(':all' if all_shops else ''),'next')])
        rows.append([(self.text(uid)['open'],'wwc:create','open')]);self.send(uid,self.text(uid)['super' if all_shops else 'mine'] if result['shops'] else self.text(uid)['empty'],rows)
    @native_view
    def dashboard(self,uid,shop_id):
        if not self.guard(uid):return
        self.flush_outbox();result=self.client.call('dashboard',uid,shop_id=shop_id);s=result['shop'];stats=result['stats'];t=self.text(uid)
        self.db.wwc_shop_links.update_one({'_id':shop_id},{'$set':{'owner_id':s['owner_id'],'bot_username':s['bot_username']}},upsert=True)
        body='\n'.join((f"{t['heading']}: @{s['bot_username']}",f"{t['state']}: {s['status']}",f"{t['plan']}: {s['plan']}",f"{t['until']}: {s['paid_until'] or t['free']}",f"{t['members']}: {stats.get('users',0)}",f"{t['orders']}: {stats.get('orders',0)}",f"{t['revenue']}: ${float(stats.get('revenue',0)):.2f}",f"{t['profit']}: ${float(stats.get('known_supplier_profit',0)):.2f}",f"{t['strikes']}: {s['idle_strikes']}/3",'',t['profitnote'],t['configured']))
        body=t['dashboard_template'].format(details=body)
        if stats.get('available') is False:body+='\n⚠️ SHOP_DATABASE_UNAVAILABLE'
        if stats.get('last_runtime_error'):body+='\n⚠️ '+str(stats['last_runtime_error']['code'])
        runtime='runtime_unknown'
        if stats.get('working') is True:runtime='runtime_ready' if stats.get('ready') else 'runtime_cold'
        elif stats.get('working') is False and stats.get('available') is not False:runtime='runtime_bad'
        body+='\n'+t['runtime']+': '+t[runtime]
        rows=[[(t['refresh'],'wwc:show:'+shop_id,'refresh')],[(t['subscribe'],'wwc:subscribe:'+shop_id,'subscribe')]]
        if s['status']=='setup':rows.extend([[(t['activate'],'wwc:activate:'+shop_id,'activate')],[(t['sqlbtn'],'wwc:sql:'+shop_id,'sqlbtn')]])
        else:
            action='pause' if s['status']=='active' else 'resume';rows.append([(t[action],'wwc:confirm:'+action+':'+shop_id,action)])
        if uid in self.super_ids:rows.extend([[(t['unlock'],'wwc:confirm:super_resume:'+shop_id,'unlock')],[(t['grant'],'wwc:confirm:grant:'+shop_id,'grant')]])
        rows.extend([[(t['credentials'],'wwc:create','credentials')],[(t['back'],'wwc:list','back')]]);self.send(uid,body,rows)
    @native_view
    def subscribe(self,uid,shop_id,method='wallet'):
        if method=='wallet':return self.wallet_subscribe(uid,shop_id)
        if not self.guard(uid):return
        shop=self._own_shop(uid,shop_id)
        if int(shop['owner_id'])!=uid:raise RuntimeError('FORBIDDEN')
        if shop['status']!='active':raise RuntimeError('ACTIVATE_STORE_BEFORE_SUBSCRIBING')
        if self.db.wwc_subscriptions.find_one({'payer_id':uid,'shop_id':shop_id,'state':'payment_received'}):self.flush_outbox();self.send(uid,self.text(uid)['pending']);return
        plan=self.client.call('config',uid);payload='wwcsub:'+secrets.token_hex(16);t=self.text(uid)
        self.db.wwc_subscriptions.insert_one({'_id':payload,'payer_id':uid,'shop_id':shop_id,'stars':int(plan['stars']),'days':int(plan['days']),'state':'created','created_at':dt.datetime.now(dt.timezone.utc)})
        self.bot.send_invoice(uid,t['invoice'],t['invoicedesc'].format(days=plan['days']),payload,'','XTR',[self.types.LabeledPrice(t['invoice'],int(plan['stars']))])
    def wallet_subscribe(self,uid,shop_id):
        if not self.guard(uid):return
        shop=self._own_shop(uid,shop_id)
        if int(shop['owner_id'])!=uid:raise RuntimeError('FORBIDDEN')
        if shop['status']!='active':raise RuntimeError('ACTIVATE_STORE_BEFORE_SUBSCRIBING')
        pending=self.db.wwc_subscriptions.find_one({'payer_id':uid,'shop_id':shop_id,'state':{'$in':['wallet_debiting','payment_received']}})
        if pending:
            self.flush_outbox();return self.wallet_result(uid,self.db.wwc_subscriptions.find_one({'_id':pending['_id']}))
        plan=self.client.call('config',uid)
        if not plan.get('wallet_price_usd'):
            self.send(uid,self.text(uid)['wallet_disabled']);return
        amount=wallet_amount(plan['wallet_price_usd']);days=int(plan['days'])
        if not 1<=days<=365:raise RuntimeError('INVALID_SUBSCRIPTION_DURATION')
        for _attempt in range(8):
            pointer=self.db.wwc_wallet_checkouts.find_one({'_id':shop_id})
            if pointer and pointer['payer_id']!=uid:raise RuntimeError('FORBIDDEN')
            current=self.db.wwc_subscriptions.find_one({'_id':pointer['checkout_id']}) if pointer else None
            now=dt.datetime.now(dt.timezone.utc)
            if current and (current['state'] in ('wallet_debiting','payment_received') or
                (current['state']=='wallet_quote' and current['expires_at'].replace(tzinfo=dt.timezone.utc)>now)):
                return self.wallet_result(uid,current)
            token=secrets.token_hex(16);payload='wwcwallet:'+token
            quote={'_id':payload,'payment_method':'nss_wallet','payer_id':uid,'shop_id':shop_id,
                   'bot_username':shop['bot_username'],'amount_micros':amount,'amount_usd':money_text(Decimal(amount)/1_000_000),
                   'currency':'USD','days':days,'state':'wallet_quote','created_at':now,'expires_at':now+dt.timedelta(minutes=20)}
            self.db.wwc_subscriptions.insert_one(quote)
            if pointer:
                chosen=self.db.wwc_wallet_checkouts.update_one({'_id':shop_id,'checkout_id':pointer['checkout_id']},
                    {'$set':{'checkout_id':payload}}).modified_count
            else:
                try:
                    self.db.wwc_wallet_checkouts.insert_one({'_id':shop_id,'payer_id':uid,'checkout_id':payload});chosen=True
                except DuplicateKeyError:chosen=False
            if chosen:return self.wallet_result(uid,quote)
            self.db.wwc_subscriptions.update_one({'_id':payload,'state':'wallet_quote'},{'$set':{'state':'cancelled'}})
        raise RuntimeError('CHECKOUT_BUSY')
    @native_view
    def wallet_result(self,uid,quote):
        t=self.text(uid)
        if quote.get('payment_method')!='nss_wallet':
            self.send(uid,t['paid' if quote['state']=='applied' else 'pending']);return
        token=quote['_id'].split(':')[-1];rows=[[(t['back'],'wwc:show:'+quote['shop_id'],'back')]]
        if quote['state'] in ('wallet_debiting','payment_received','applied'):
            body=t['paid' if quote['state']=='applied' else 'pending']
            receipt=quote.get('wallet_receipt')
            if receipt:body+='\n\n'+t['wallet_receipt'].format(reference='NSS-'+token,amount=quote['amount_usd'],balance=money_text(receipt['balance_after']))
            rows.insert(0,[(t['refresh'],'wwc:show:'+quote['shop_id'],'refresh')]);self.send(uid,body,rows);return
        if quote['state']!='wallet_quote' or quote['expires_at'].replace(tzinfo=dt.timezone.utc)<=dt.datetime.now(dt.timezone.utc):
            self.send(uid,t['wallet_expired'],rows);return
        user=self.db.users.find_one({'user_id':uid}) or {};balance=user.get('balance',0)
        body=t['wallet_title']+'\n\n'+t['wallet_quote'].format(bot=quote['bot_username'],days=quote['days'],amount=quote['amount_usd'],balance=money_text(balance))
        if Decimal(str(balance))+Decimal('0.0000005')<Decimal(quote['amount_usd']):
            body+='\n\n'+t['wallet_insufficient'].format(amount=quote['amount_usd'],balance=money_text(balance))
        rows[0:0]=[[(t['wallet_pay'].format(amount=quote['amount_usd']),'wwc:walletpay:'+token,'wallet_pay')],
                    [(t['wallet_deposit'],'open_deposit','wallet_deposit')],[(t['wallet_cancel'],'wwc:walletcancel:'+token,'wallet_cancel')]]
        self.send(uid,body,rows)
    def wallet_confirm(self,uid,token):
        if not self.guard(uid):return
        if not re.fullmatch(r'[a-f0-9]{32}',token):raise RuntimeError('INVALID_CHECKOUT')
        quote=self.db.wwc_subscriptions.find_one({'_id':'wwcwallet:'+token,'payer_id':uid,'payment_method':'nss_wallet'})
        if not quote:raise RuntimeError('FORBIDDEN')
        if quote['state'] in ('wallet_debiting','payment_received','applied'):
            self.flush_outbox();return self.wallet_result(uid,self.db.wwc_subscriptions.find_one({'_id':quote['_id']}))
        if not self.db.wwc_wallet_checkouts.find_one({'_id':quote['shop_id'],'payer_id':uid,'checkout_id':quote['_id']}):
            raise RuntimeError('STALE_CHECKOUT')
        if quote['state']!='wallet_quote' or quote['expires_at'].replace(tzinfo=dt.timezone.utc)<=dt.datetime.now(dt.timezone.utc):
            return self.wallet_result(uid,quote)
        shop=self._own_shop(uid,quote['shop_id'])
        if int(shop['owner_id'])!=uid or shop['status']!='active':raise RuntimeError('ACTIVATE_STORE_BEFORE_SUBSCRIBING')
        if self.g.get('is_deposit_locked',lambda _:False)(uid):raise RuntimeError('FINISH_OR_CANCEL_PENDING_DEPOSIT')
        now=dt.datetime.now(dt.timezone.utc)
        self.db.wwc_subscriptions.update_one({'_id':quote['_id'],'state':'wallet_quote','expires_at':{'$gt':now}},
            {'$set':{'state':'wallet_debiting','confirmed_at':now,'next_retry':now,'attempts':0}})
        self.outbox_event.set();self.flush_outbox()
        return self.wallet_result(uid,self.db.wwc_subscriptions.find_one({'_id':quote['_id']}))
    def wallet_cancel(self,uid,token):
        if not re.fullmatch(r'[a-f0-9]{32}',token):raise RuntimeError('INVALID_CHECKOUT')
        quote=self.db.wwc_subscriptions.find_one({'_id':'wwcwallet:'+token,'payer_id':uid,'payment_method':'nss_wallet'})
        if not quote:raise RuntimeError('FORBIDDEN')
        changed=self.db.wwc_subscriptions.update_one({'_id':quote['_id'],'payer_id':uid,'state':'wallet_quote'},
            {'$set':{'state':'cancelled'}}).modified_count
        if changed:self.send(uid,self.text(uid)['wallet_cancelled']);return
        return self.wallet_result(uid,self.db.wwc_subscriptions.find_one({'_id':quote['_id']}))
    def _wallet_debit(self,quote):
        """Balance and its permanent receipt change in ONE conditional user write.

        A crash before the journal is updated can replay this operation safely.
        Existing wallet/referral fields and the old bot code are not rewritten.
        """
        uid=quote['payer_id'];token=quote['_id'].split(':')[-1]
        amount=Decimal(quote['amount_micros'])/1_000_000;field='_wwc_wallet_receipts.'+token
        if wallet_amount(quote['amount_usd'])!=quote['amount_micros']:raise RuntimeError('CHECKOUT_AMOUNT_MISMATCH')
        for _attempt in range(12):
            user=self.db.users.find_one({'user_id':uid})
            if not user:raise RuntimeError('WALLET_USER_NOT_FOUND')
            receipt=user.get('_wwc_wallet_receipts',{}).get(token)
            if receipt:
                if receipt['amount_micros']!=quote['amount_micros']:raise RuntimeError('CHECKOUT_AMOUNT_MISMATCH')
                return receipt
            if user.get('is_banned'):raise RuntimeError('FORBIDDEN')
            balance=Decimal(str(user.get('balance',0)))
            if not balance.is_finite():raise RuntimeError('INVALID_WALLET_BALANCE')
            if balance+Decimal('0.0000005')<amount:return None
            after=max(Decimal(0),(balance-amount).quantize(Decimal('0.000001'),rounding=ROUND_HALF_UP))
            receipt={'amount_micros':quote['amount_micros'],'balance_before':float(balance),
                     'balance_after':float(after),'at':int(time.time())}
            query={'user_id':uid,'balance':user.get('balance',0),field:{'$exists':False},'is_banned':{'$ne':True}}
            changed=self.db.users.update_one(query,{'$set':{'balance':float(after),field:receipt}})
            if changed.modified_count:
                self.g['_invalidate_user_cache'](uid);return receipt
        raise RuntimeError('WALLET_CHANGED_RETRY')
    def _prepare_wallet_payment(self,quote):
        receipt=self._wallet_debit(quote)
        if not receipt:
            self.db.wwc_subscriptions.update_one({'_id':quote['_id'],'state':'wallet_debiting'},
                {'$set':{'state':'wallet_quote'},'$unset':{'next_retry':''}});return
        token=quote['_id'].split(':')[-1]
        self.db.balance_logs.update_one({'_id':'wwcwallet:'+token},{'$setOnInsert':{
            'user_id':quote['payer_id'],'type':'wwc_subscription','amount':-float(Decimal(quote['amount_usd'])),
            'old_balance':receipt['balance_before'],'new_balance':receipt['balance_after'],
            'balance_source':'atomic_wallet_update','note':'WWC subscription · '+quote['shop_id']+' · NSS-'+token,
            'wwc_shop_id':quote['shop_id'],'wwc_payment_id':quote['_id'],
            'date':dt.datetime.fromtimestamp(receipt['at'],dt.timezone.utc).replace(tzinfo=None)}},upsert=True)
        self.db.wwc_subscriptions.update_one({'_id':quote['_id'],'state':'wallet_debiting'},{'$set':{
            'state':'payment_received','wallet_receipt':receipt,'next_retry':dt.datetime.now(dt.timezone.utc)}})
    def valid_invoice(self,payload,payer,currency,amount):
        d=self.db.wwc_subscriptions.find_one({'_id':payload,'payer_id':int(payer)})
        if not d or d['state']!='created' or currency!='XTR' or int(amount)!=d['stars']:return None
        return d if dt.datetime.now(dt.timezone.utc)-d['created_at'].replace(tzinfo=dt.timezone.utc)<dt.timedelta(hours=24) else None
    def precheckout(self,q):
        ok=bool(self.valid_invoice(q.invoice_payload,q.from_user.id,q.currency,q.total_amount) and not self.g.get('is_user_banned',lambda _:False)(q.from_user.id))
        self.bot.answer_pre_checkout_query(q.id,ok=ok,**({} if ok else {'error_message':self.text(q.from_user.id)['invalid_invoice'][:240]}))
    def payment(self,message):
        p=message.successful_payment;uid=message.from_user.id;self.ensure_indexes();d=self.db.wwc_subscriptions.find_one({'_id':p.invoice_payload,'payer_id':uid})
        if not d or p.currency!='XTR' or p.total_amount!=d['stars'] or (d.get('charge_id') and d['charge_id']!=p.telegram_payment_charge_id):self.error(uid,RuntimeError('PAYMENT_REQUIRES_REVIEW'));return
        if d['state']=='created':self.db.wwc_subscriptions.update_one({'_id':d['_id'],'state':'created'},{'$set':{'state':'payment_received','charge_id':p.telegram_payment_charge_id,'next_retry':dt.datetime.now(dt.timezone.utc),'attempts':0}})
        self.outbox_event.set();self.flush_outbox();current=self.db.wwc_subscriptions.find_one({'_id':d['_id']})
        self.send(uid,self.text(uid)['paid' if current['state']=='applied' else 'pending'],[[(self.text(uid)['refresh'],'wwc:show:'+d['shop_id'],'refresh')]])
    def flush_outbox(self):
        if not self.client.enabled or not self.outbox_lock.acquire(blocking=False):return
        try:
            # Clear before reading: another callback can set it during this flush
            # without its wakeup being lost at the end of the current batch.
            self.outbox_event.clear()
            now=dt.datetime.now(dt.timezone.utc)
            for d in self.db.wwc_subscriptions.find({'state':'wallet_debiting','next_retry':{'$lte':now}}).limit(10):
                try:self._prepare_wallet_payment(d)
                except Exception:
                    attempts=int(d.get('attempts',0))+1
                    self.db.wwc_subscriptions.update_one({'_id':d['_id'],'state':'wallet_debiting'},{'$set':{
                        'attempts':attempts,'next_retry':now+dt.timedelta(seconds=min(3600,30*2**min(attempts,7)))}})
            now=dt.datetime.now(dt.timezone.utc)
            for d in self.db.wwc_subscriptions.find({'state':'payment_received','next_retry':{'$lte':now}}).limit(10):
                try:
                    if d.get('payment_method')=='nss_wallet':
                        self.client.call('subscribe',d['payer_id'],shop_id=d['shop_id'],receipt='nss:'+d['_id'].split(':')[-1],
                            payment_method='nss_wallet',amount=d['amount_usd'],currency='USD',days=d['days'])
                    else:
                        receipt='tg:'+hashlib.sha256(str(d['charge_id']).encode()).hexdigest()
                        self.client.call('subscribe',d['payer_id'],shop_id=d['shop_id'],receipt=receipt,stars=d['stars'],days=d['days'])
                    self.db.wwc_subscriptions.update_one({'_id':d['_id'],'state':'payment_received'},{'$set':{'state':'applied','applied_at':now}})
                except Exception:
                    attempts=int(d.get('attempts',0))+1;self.db.wwc_subscriptions.update_one({'_id':d['_id'],'state':'payment_received'},{'$set':{'attempts':attempts,'next_retry':now+dt.timedelta(seconds=min(3600,30*2**min(attempts,7)))}})
            if self.db.wwc_subscriptions.find_one({'state':{'$in':['wallet_debiting','payment_received']}}):self.outbox_event.set()
        except Exception:
            self.outbox_event.set();raise
        finally:self.outbox_lock.release()
    def outbox_loop(self):
        try:
            if self.db.wwc_subscriptions.find_one({'state':{'$in':['wallet_debiting','payment_received']}}):self.outbox_event.set()
        except Exception:pass
        while True:
            self.outbox_event.wait()
            try:self.flush_outbox()
            except Exception:pass
            if self.outbox_event.is_set():time.sleep(60)
    @native_view
    def callback(self,call):
        uid=call.from_user.id
        parts=call.data.split(':');action=parts[1] if len(parts)>1 else ''
        redirect=bool(self.manager_username and action in ('create','list','super','show','sql','subscribe','activate'))
        if call.message.chat.type!='private' or self.g.get('is_user_banned',lambda _:False)(uid):return
        if not redirect and not self.guard(uid):return
        self.bot.clear_step_handler_by_chat_id(chat_id=uid)
        self.bot.answer_callback_query(call.id)
        if redirect:
            target='open' if action=='create' else ('wwcsub_'+parts[2] if action=='subscribe' and len(parts)>2 else 'wwc_'+parts[2] if action in ('show','sql','activate') and len(parts)>2 else 'shops')
            if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}',target):target='shops'
            self.send(uid,self.text(uid)['manager_redirect'],[[(self.text(uid)['open'],'https://t.me/'+self.manager_username+'?start='+target,'open')]])
            return
        try:
            if action=='create':self.create(uid)
            elif action=='list':self.listing(uid,len(parts)>3 and parts[3]=='all',int(parts[2]) if len(parts)>2 else 0)
            elif action=='super' and uid in self.super_ids:self.listing(uid,True)
            elif action=='show':self.dashboard(uid,parts[2])
            elif action=='sql':self.sql(uid,parts[2])
            elif action=='subscribe':self.subscribe(uid,parts[2])
            elif action=='walletpay':self.wallet_confirm(uid,parts[2])
            elif action=='walletcancel':self.wallet_cancel(uid,parts[2])
            elif action=='activate':
                result=self.client.call('activate',uid,shop_id=parts[2]);s=result['shop'];t=self.text(uid)
                self.send(uid,t['queued' if result.get('queued') else 'active'],[[('@'+s['bot_username'],s['bot_url'])],[(t['mine'],s['dashboard_url'],'mine')],[(t['subscribe'],'wwc:subscribe:'+parts[2],'subscribe')]])
            elif action in ('confirm','do') and len(parts)==4 and parts[2] in ('pause','resume','super_resume','grant'):
                if parts[2] in ('super_resume','grant') and uid not in self.super_ids:raise RuntimeError('FORBIDDEN')
                self._own_shop(uid,parts[3])
                if action=='confirm':self.send(uid,self.text(uid)['confirm'],[[(self.text(uid)['yes'],'wwc:do:'+parts[2]+':'+parts[3],'yes')],[(self.text(uid)['back'],'wwc:show:'+parts[3],'back')]])
                else:self.client.call(parts[2],uid,shop_id=parts[3],receipt='manual:'+call.id);self.dashboard(uid,parts[3])
        except Exception as exc:self.error(uid,exc)
    @native_view
    def command(self,message):
        uid=message.from_user.id
        if message.chat.type!='private':return
        self.bot.clear_step_handler_by_chat_id(chat_id=uid)
        command,*args=(message.text or '').split();command=command.split('@')[0]
        if self.manager_username and command!='/cancelstore':
            if self.g.get('is_user_banned',lambda _:False)(uid):return
            target=args[0] if command=='/start' and args else ('open' if command in ('/createstore','/openstore') else 'shops')
            if target=='wwc_create':target='open'
            if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}',target):target='shops'
            self.send(uid,self.text(uid)['manager_redirect'],[[(self.text(uid)['open'],'https://t.me/'+self.manager_username+'?start='+target,'open')]])
            return
        try:
            if command=='/cancelstore':self.db.wwc_drafts.delete_one({'_id':uid});self.draft_ids.discard(uid);self.send(uid,self.text(uid)['cancelled'])
            elif command in ('/createstore','/openstore'):self.create(uid)
            elif command in ('/myshops','/stores'):self.listing(uid)
            elif command=='/superstores' and uid in self.super_ids:self.listing(uid,True)
            elif command=='/shop_sql':self.sql(uid)
            elif command=='/start' and args:
                if not self.g['get_user_data_full'](uid):self.g['start_handler'](message)
                value=args[0]
                if value=='wwc_create':self.create(uid)
                elif value.startswith('wwcsub_'):self.subscribe(uid,value[7:])
                elif value.startswith('wwc_'):self.dashboard(uid,value[4:])
        except Exception as exc:self.error(uid,exc)
    def install(self):
        commands={'createstore','openstore','myshops','stores','superstores','shop_sql','cancelstore'}
        def is_command(m):
            words=(m.text or '').split()
            return bool(words and ((words[0].startswith('/') and words[0].split('@')[0][1:] in commands) or (words[0].split('@')[0]=='/start' and len(words)>1 and words[1].startswith(('wwc_','wwcsub_')))))
        before=len(self.bot.message_handlers)
        self.bot.message_handler(func=is_command,content_types=['text'])(self.command)
        self.bot.message_handler(func=lambda m:m.chat.type=='private' and not (m.text or '').startswith('/') and bool(self.cipher and m.from_user.id in self.draft_ids and self.draft(m.from_user.id)),content_types=['text'])(self.receive_safe)
        self.bot.message_handler(func=lambda m:str(getattr(m.successful_payment,'invoice_payload','')).startswith('wwcsub:'),content_types=['successful_payment'])(self.payment)
        new=self.bot.message_handlers[before:];del self.bot.message_handlers[before:];self.bot.message_handlers[0:0]=new
        before=len(self.bot.pre_checkout_query_handlers);self.bot.pre_checkout_query_handler(func=lambda q:str(q.invoice_payload).startswith('wwcsub:'))(self.precheckout)
        new=self.bot.pre_checkout_query_handlers[before:];del self.bot.pre_checkout_query_handlers[before:];self.bot.pre_checkout_query_handlers[0:0]=new
        before=len(self.bot.callback_query_handlers);self.bot.callback_query_handler(func=lambda c:str(c.data).startswith('wwc:'))(self.callback)
        new=self.bot.callback_query_handlers[before:];del self.bot.callback_query_handlers[before:];self.bot.callback_query_handlers[0:0]=new
        for name in ('send_message','edit_message_text'):
            original=getattr(self.bot,name)
            def wrap_menu(fn,kind):
                @wraps(fn)
                def wrapped(*args,**kwargs):
                    markup=kwargs.get('reply_markup');uid=kwargs.get('chat_id') or (args[0] if kind=='send_message' and args else None)
                    if kind=='edit_message_text' and not uid and len(args)>1:uid=args[1]
                    if uid and isinstance(uid,int) and uid>0 and (self.manager_username or self.client.enabled) and hasattr(markup,'keyboard'):
                        values={getattr(b,'callback_data','') or getattr(b,'url','') for row in markup.keyboard for b in row}
                        if {'open_shop','open_settings'}<=values and 'wwc:create' not in values and 'https://t.me/'+self.manager_username+'?start=open' not in values:
                            with self.g['_cms_button_view']():
                                if self.manager_username:
                                    markup.row(self.button(uid,'open','https://t.me/'+self.manager_username+'?start=open'))
                                else:
                                    markup.row(self.button(uid,'open','wwc:create'));markup.row(self.button(uid,'mine','wwc:list'))
                                    if uid in self.super_ids:markup.row(self.button(uid,'super','wwc:super'))
                    return fn(*args,**kwargs)
                return wrapped
            setattr(self.bot,name,wrap_menu(original,name))
        from wwc_manager_bridge import install as install_manager_bridge
        self.manager_bridge=install_manager_bridge(self)
        self.install_status_endpoint()
        if self.client.enabled:threading.Thread(target=self.outbox_loop,name='wwc-subscription-outbox',daemon=True).start()
    def install_status_endpoint(self):
        platform=self;handler=self.g['APIHandler'];original=handler.do_GET
        pattern=re.compile(r'/([A-Za-z0-9_-]{3,128})/platform_request/([A-Za-z0-9_-]{1,128})')
        @platform.g['_api_http_guard']
        def lookup(h):
            match=pattern.fullmatch(urllib.parse.urlsplit(h.path).path)
            if match[1]!=platform.g['_get_api_gateway']():return original(h)
            d,user=h._auth()
            if not d or not user:return platform.g['_json_resp'](h,401,{'error':'unauthorized'})
            uid=int(d['user_id']);ref='IDEM_'+hashlib.sha256(f'{uid}:{match[2]}'.encode()).hexdigest()[:40]
            result=platform.db.api_idempotency.find_one({'_id':ref,'api_user_id':uid})
            if not result:return platform.g['_json_resp'](h,404,{'error':'request_not_found'})
            return platform.g['_json_resp'](h,200,{'status':result['status'],'code':result.get('code'),'data':result.get('data') if result['status']=='done' else None})
        @wraps(original)
        def get(h):
            return lookup(h) if pattern.fullmatch(urllib.parse.urlsplit(h.path).path) else original(h)
        handler.do_GET=get

def install(namespace):
    if namespace.get('_wwc_platform'):return namespace['_wwc_platform']
    addon=StorePlatform(namespace);addon.install();namespace['_wwc_platform']=addon;return addon
