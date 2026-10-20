/* Real Chromium against signed local admin HTTP; integrations permanently blocked. */
const {spawn}=require('child_process'),assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const BASE='http://127.0.0.1:8782';
const server=spawn(process.env.LARA_TEST_PYTHON||'python',['verification/serve_user_inspection_test.py'],{
 env:{...process.env,LARA_TEST_PORT:'8782'},stdio:['ignore','pipe','pipe']});
const ready=new Promise((resolve,reject)=>{
 server.stdout.on('data',d=>{if(String(d).includes('LOCAL USER INSPECTION SERVER'))resolve();});
 server.stderr.on('data',()=>{});server.once('exit',c=>reject(Error('Fixture exited '+c)));
});
(async()=>{
 await ready;
 const browser=await chromium.launch({headless:true,
  ...(process.env.LARA_TEST_BROWSER_EXECUTABLE?{executablePath:process.env.LARA_TEST_BROWSER_EXECUTABLE}:{}),
  args:['--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1','--disable-background-networking',
   '--disable-component-update','--disable-sync','--no-pings','--no-sandbox','--disable-dev-shm-usage','--disable-gpu']});
 try{
  const page=await browser.newPage({viewport:{width:390,height:844}}),errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():r.abort());
  await page.goto(BASE+'/__user_test__/',{waitUntil:'domcontentloaded'});
  await page.locator('[data-act="tab"][data-v="admin"]').click();
  async function searchUser(){
   await page.locator('#admq').fill('1001');
   await page.locator('[data-act="adm-search"]').click();
   await page.waitForFunction(()=>document.querySelector('.up-profile')||document.querySelector('[data-act="adm-user"][data-v="1001"]'));
   if(!await page.locator('.up-profile').isVisible())await page.locator('[data-act="adm-user"][data-v="1001"]').first().click();
  }
  await searchUser();
  const top=()=>page.locator('.sheet:not(.closing)').last();
  await top().locator('.up-profile').waitFor();
  assert.equal(await top().locator('.up-record').count(),0);
  await top().locator('.up-tabs [data-v="purchases"]').click();
  assert.equal(await top().locator('.up-product').count(),3);
  assert(!(await top().innerText()).includes('OTHER-USER-PRIVATE-ACCOUNT'));
  console.log('PASS user inspection opens clean overview then product buttons across both ledgers');
  await top().locator('.up-product').filter({hasText:'Netflix Premium'}).click();
  await top().locator('[data-act="up-records-page"][data-v="2"]').click();
  await top().locator('.up-record').last().waitFor();
  await page.waitForFunction(()=>[...document.querySelectorAll('.sheet:not(.closing)')].at(-1)?.querySelector('.up-pager')?.textContent.includes('2 / 2'));
  assert.equal(await top().locator('.up-record').count(),4);
  const ref=await top().locator('.up-record .mono').first().innerText();
  await top().locator('.up-record').first().click();
  await top().locator('.up-codes').waitFor();
  assert((await top().innerText()).includes('Password: TEST-'));
  const download=page.waitForEvent('download');await top().locator('[data-act="up-export"]').click();
  assert((await download).suggestedFilename().startsWith('user_1001_'));
  await top().locator('[data-act="up-back"]').click();
  assert((await top().locator('.up-pager').innerText()).includes('2 / 2'));
  assert.equal(await top().locator('.up-record .mono').first().innerText(),ref);
  await top().locator('[data-act="up-back"]').click();
  await top().locator('.up-product').filter({hasText:'ChatGPT Business'}).click();
  await top().locator('.up-record').first().click();
  await top().locator('.up-subscription').waitFor();
  assert((await top().innerText()).includes('member@example.test'));
  assert((await top().innerText()).includes('2026-01-01'));
  assert(!(await top().innerText()).includes('NEVER-EXPOSE'));
  if(process.env.LARA_TEST_INSPECTION_SCREENSHOT)await page.screenshot({path:process.env.LARA_TEST_INSPECTION_SCREENSHOT,fullPage:false});
  await top().locator('[data-act="up-back"]').click();await top().locator('[data-act="up-back"]').click();
  console.log('PASS paginated product records, credentials, complete download and saved subscription state; back preserves page');
  await top().locator('[data-act="adm-open"][data-v="ad_u_det_1001"]').click();
  await top().locator('[data-act="ap-btn"][data-v="ad_ugift_1001"]').click();
  await top().locator('[data-act="ap-btn"][data-v="ad_gift_comp_1001"]').click();
  await top().locator('#apin').waitFor();
  await top().locator('[data-act="ap-back"]').click();
  await top().locator('[data-act="ap-btn"][data-v="ad_gift_comp_1001"]').waitFor();
  assert.equal(await top().locator('#apin').count(),0);
  await top().locator('[data-act="ap-back"]').click();
  await top().locator('[data-act="ap-btn"][data-v="ad_uh_buy_1001"]').waitFor();
  await top().locator('[data-act="ap-back"]').click();
  await top().locator('.up-profile').waitFor();
  console.log('PASS virtual console back cancels pending amount, returns to actual prior member screens, then native profile');
  await page.setViewportSize({width:1360,height:960});
  await top().locator('[data-act="sheet-close"]').first().click();
  await page.locator('[data-act="lang"][data-v="ar"]').click();
  await searchUser();
  await top().locator('.up-profile').waitFor();
  await top().locator('.up-tabs [data-v="purchases"]').click();
  await top().locator('.up-product').filter({hasText:'Canva Pro Supplier'}).click();
  await top().locator('.up-record').first().click();
  await top().locator('.up-codes').waitFor();
  assert((await top().innerText()).includes('supplier@example.test:TEST-PASS'));
  assert.equal(await page.locator('html').getAttribute('dir'),'rtl');
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth));
  const state=await(await page.request.get(BASE+'/__user_test__/state')).json();
  assert.equal(state.balance,100);assert.equal(state.balance_logs,0);assert.equal(state.orders,18);
  assert.deepEqual(errors,[]);
  console.log('PASS desktop/mobile, English/Arabic RTL, supplier delivery, zero JS errors and zero balance mutations');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);});
