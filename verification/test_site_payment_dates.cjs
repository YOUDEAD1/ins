/* No browser or provider traffic: execute the actual date and payment action helpers. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
process.env.TZ = 'Africa/Casablanca';
const html = fs.readFileSync(path.join(__dirname, '..', 'webapp', 'index.html'), 'utf8');
let parsed = 0;
for (const match of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script\s*>/gi)) {
  if (/\bsrc\s*=|type\s*=\s*["']application\//i.test(match[1])) continue;
  new vm.Script(match[2], {filename:`inline-script-${++parsed}.js`});
}
assert(parsed >= 1);
const dateCode = html.slice(html.indexOf('function fmtDate(ts)'), html.indexOf('/* ───────────────────────── icons'));
const normalizeCode = html.match(/^function normalizeBinanceId.*$/m)[0];
const actionCode = html.slice(html.indexOf('async function payAction('), html.indexOf('/* ───────────────────────── profile'));
const calls = [];
const input = {value:'\u200f١٢٣٤٥٦٧٨٩٠١٢٣٤٥٦٧٨', scrollIntoView(){calls.push('scroll');}, focus(){calls.push('focus');}};
const context = vm.createContext({
  paymentText: (key, fallback) => fallback || key, isAr:()=>true,
  renderPayment:()=>calls.push('render'), pollPay:async()=>calls.push('poll'),
  api:async(route,body)=>{calls.push({route,body});return {ok:true};},
  toast:message=>{throw new Error(message);}, errText:()=> 'unexpected API error'
});
vm.runInContext(dateCode + '\n' + normalizeCode + '\n' + actionCode, context);
const today = Date.UTC(2026,10,1,10,9,8)/1000;
assert.equal(context.fmtDate(today), '2026-11-01 · 11:09:08 UTC+01:00');
assert.match(context.fmtDate(Date.UTC(2024,4,6,11,2,3)/1000), /^2024-05-06 · \d\d:02:03 UTC/);
assert.equal(context.fmtDate('invalid'), '');
assert.equal(context.normalizeBinanceId(' ۱۲۳۴۵۶۷۸۹۰۱۲۳۴۵۶۷۸\u200f '),'123456789012345678');
(async()=>{
  const sh = {el:{isConnected:true,querySelector:()=>input}}, state={};
  assert.equal(await context.payAction(sh,state,'pay-manual-open',{}),true);
  assert.equal(state.showManual,true);
  assert(calls.includes('focus'));
  assert.equal(await context.payAction(sh,state,'pay-manual',{}),true);
  const submitted = calls.find(x=>x && x.route==='deposit/check');
  assert.equal(submitted.body.transaction_id,'123456789012345678');
  assert(calls.includes('poll'));
  console.log(`${parsed} scripts parsed; date precision, timezone, Order ID entry, normalization, and submission passed`);
})().catch(error=>{console.error(error);process.exitCode=1;});
