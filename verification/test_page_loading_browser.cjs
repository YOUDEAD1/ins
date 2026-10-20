/* Real page-loading transitions; fake signed identities, loopback services only. */
const {spawn}=require('child_process'),assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const BASE='http://127.0.0.1:8786';
const server=spawn(process.env.LARA_TEST_PYTHON||'python',['verification/serve_referral_test.py'],{
 env:{...process.env,LARA_TEST_PORT:'8786'},stdio:['ignore','pipe','pipe']});
const ready=new Promise((resolve,reject)=>{
 server.stdout.on('data',d=>{if(String(d).includes('LOCAL REFERRAL SERVER'))resolve();});
 server.stderr.on('data',()=>{});server.once('exit',c=>reject(Error('Fixture exited '+c)));
});
function gate(){let release;const wait=new Promise(r=>release=r);return {wait,release};}
(async()=>{
 await ready;
 const browser=await chromium.launch({headless:true,
  ...(process.env.LARA_TEST_BROWSER_EXECUTABLE?{executablePath:process.env.LARA_TEST_BROWSER_EXECUTABLE}:{}),
  args:['--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1','--disable-background-networking',
   '--disable-component-update','--disable-sync','--no-pings','--no-sandbox','--disable-dev-shm-usage','--disable-gpu']});
 try{
  const page=await browser.newPage({viewport:{width:390,height:844},locale:'ar-MA'}),errors=[],calls=[];
  page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>{if(r.url().includes('/webapp/'))calls.push(r.url());});
  await page.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():r.abort());
  const boot=gate();
  await page.route('**/webapp/bootstrap',async r=>{const response=await r.fetch();await boot.wait;await r.fulfill({response});});
  await page.goto(BASE+'/__ref_test__/?uid=1001',{waitUntil:'domcontentloaded'});
  const loader=page.locator('#page-loading');
  await loader.waitFor({state:'visible'});
  assert.equal(await page.locator('#view').getAttribute('aria-busy'),'true');
  assert((await loader.locator('.lm-plane').evaluate(e=>getComputedStyle(e).animationName)).includes('ldFly'));
  assert(await loader.locator('svg.ld-mark .lm-rib path').count()>=8);   /* the LARA logo is drawn in the page (vector), not a picture */
  assert.equal(await loader.locator('img').count(),0);
  /* Telegram + bot in English on an Arabic phone: the loader follows Telegram/the bot, never the phone */
  const ltxt=await loader.innerText();
  assert(/Connecting to the store|Checking your account/.test(ltxt),ltxt);
  assert(!/[\u0600-\u06FF]/.test(ltxt),ltxt);
  assert.equal(await page.evaluate(()=>document.documentElement.dir),'ltr');
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  if(process.env.LARA_TEST_LOADING_SCREENSHOT)await page.screenshot({path:process.env.LARA_TEST_LOADING_SCREENSHOT});
  boot.release();await loader.waitFor({state:'hidden'});
  assert.equal(calls.filter(x=>x.endsWith('/bootstrap')).length,1);
  console.log('PASS first paint has the real store logo animating; label in the Telegram/bot language (not the phone), mobile layout, one bootstrap request');
  const refs=gate(),subs=gate();
  await page.route('**/webapp/referrals',async r=>{const response=await r.fetch();await refs.wait;await r.fulfill({response});});
  await page.route('**/webapp/cgpt/subs',async r=>{const response=await r.fetch();await subs.wait;await r.fulfill({response});});
  await page.locator('[data-act="tab"][data-v="profile"]').first().click();
  await loader.waitFor({state:'visible'});
  refs.release();await page.waitForResponse(r=>r.url().endsWith('/referrals'));
  assert(await loader.isVisible());
  subs.release();await loader.waitFor({state:'hidden'});
  await page.locator('[data-act="lang"][data-v="ar"]').first().click();
  const count=calls.length;
  await page.locator('[data-act="tab"][data-v="store"]').first().click();
  await page.locator('[data-act="tab"][data-v="profile"]').first().click();
  assert(await loader.isHidden());
  assert.equal(calls.length,count);
  console.log('PASS profile waits for concurrent reads; cached navigation adds no requests or artificial loading delay');
  const orders=gate();
  await page.route('**/webapp/orders',async r=>{const response=await r.fetch();await orders.wait;await r.fulfill({response});});
  await page.locator('[data-act="tab"][data-v="orders"]').click();await loader.waitFor({state:'visible'});
  await page.evaluate(()=>document.querySelector('[data-act="tab"][data-v="store"]').click());
  await loader.waitFor({state:'hidden'});orders.release();
  await page.waitForResponse(r=>r.url().endsWith('/orders'));
  assert(await loader.isHidden());
  const fail=async r=>r.fulfill({status:503,contentType:'application/json',body:'{"ok":false,"error":"server_error"}'});
  await page.route('**/webapp/bootstrap',fail);
  await page.reload({waitUntil:'domcontentloaded'});await page.locator('[data-act="retry"]').waitFor();
  await loader.waitFor({state:'hidden'});
  await page.unroute('**/webapp/bootstrap',fail);
  await page.locator('[data-act="retry"]').click();await page.locator('[data-act="tab"][data-v="store"]').first().waitFor();
  await loader.waitFor({state:'hidden'});
  await page.emulateMedia({reducedMotion:'reduce'});
  assert.equal(await loader.locator('.lm-plane').evaluate(e=>getComputedStyle(e).animationName),'none');
  assert.deepEqual(errors,[]);
  console.log('PASS leaving a slow page, errors and retry never leave overlay stuck; reduced motion respected; zero JS exceptions');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);});
