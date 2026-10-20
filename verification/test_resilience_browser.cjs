/* Round 32: early Telegram (iOS) events before the SDK, one dropped read request, offline customers — no errors, no false alerts. */
const {spawn}=require('child_process'),assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const PORT='8791',BASE='http://127.0.0.1:'+PORT;
const server=spawn(process.env.LARA_TEST_PYTHON||'python',['verification/serve_referral_test.py'],{
 env:{...process.env,LARA_TEST_PORT:PORT},stdio:['ignore','pipe','pipe']});
const ready=new Promise((resolve,reject)=>{
 server.stdout.on('data',d=>{if(String(d).includes('LOCAL REFERRAL SERVER'))resolve();});
 server.stderr.on('data',()=>{});server.once('exit',c=>reject(Error('Fixture exited '+c)));
});
(async()=>{
 await ready;
 const browser=await chromium.launch({headless:true,
  ...(process.env.LARA_TEST_BROWSER_EXECUTABLE?{executablePath:process.env.LARA_TEST_BROWSER_EXECUTABLE}:{}),
  args:['--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1','--no-sandbox','--disable-dev-shm-usage','--disable-gpu']});
 try{
  const ctx=await browser.newContext({viewport:{width:390,height:844}});
  const page=await ctx.newPage(),errors=[],reports=[];let boots=0;
  page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():r.abort());
  await page.route('**/__ref_test__/*',async route=>{
   const response=await route.fetch();let html=await response.text();
   /* what Telegram iOS does: deliver an event before telegram-web-app.js exists */
   html=html.replace('<script id="lara-error-bootstrap">','<script>window.TelegramGameProxy.receiveEvent("viewport_changed",{"height":800,"is_state_stable":true});</script><script id="lara-error-bootstrap">');
   const headers={...response.headers()};delete headers['content-encoding'];delete headers['content-length'];
   await route.fulfill({status:200,headers,body:html});
  });
  await page.route('**/webapp/client/error',async route=>{reports.push(route.request().postDataJSON());await route.fulfill({status:200,contentType:'application/json',body:'{"ok":true}'});});
  await page.route('**/webapp/bootstrap',async route=>{boots++;if(boots===1)return route.abort('failed');return route.continue();});
  await page.goto(BASE+'/__ref_test__/?uid=1001',{waitUntil:'domcontentloaded'});
  await page.locator('[data-act="tab"][data-v="store"]').first().waitFor({timeout:20000});
  await page.waitForTimeout(600);
  assert.deepEqual(errors,[],'no TelegramGameProxy TypeError');
  assert.equal(boots,2,'the dropped startup request was retried once');
  assert(!reports.some(r=>/Network request failed|TelegramGameProxy/.test(r.message||'')),JSON.stringify(reports));
  console.log('PASS early Telegram event before the SDK is queued (no TypeError); a dropped startup request retries once and the store opens with no alert');
  await ctx.setOffline(true);
  await page.locator('[data-act="tab"][data-v="profile"]').first().click();
  await page.waitForTimeout(2500);
  assert(!reports.some(r=>/Network request failed/.test(r.message||'')),'offline customer is not reported as a store error');
  await ctx.setOffline(false);
  assert.deepEqual(errors,[]);
  console.log('PASS offline customer never triggers a «Network request failed» alert; zero JS exceptions');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);});
