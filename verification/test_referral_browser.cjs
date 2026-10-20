/* Real Chromium, signed fake users, memory DB; all external requests blocked. */
const {spawn}=require('child_process'),assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const BASE='http://127.0.0.1:8785';
const server=spawn(process.env.LARA_TEST_PYTHON||'python',['verification/serve_referral_test.py'],{
 env:{...process.env,LARA_TEST_PORT:'8785'},stdio:['ignore','pipe','pipe']});
const ready=new Promise((resolve,reject)=>{
 server.stdout.on('data',d=>{if(String(d).includes('LOCAL REFERRAL SERVER'))resolve();});
 server.stderr.on('data',()=>{});server.once('exit',c=>reject(Error('Fixture exited '+c)));
});
(async()=>{
 await ready;
 const browser=await chromium.launch({headless:true,
  ...(process.env.LARA_TEST_BROWSER_EXECUTABLE?{executablePath:process.env.LARA_TEST_BROWSER_EXECUTABLE}:{}),
  args:['--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1','--disable-background-networking',
   '--disable-component-update','--disable-sync','--no-pings','--no-sandbox','--disable-dev-shm-usage','--disable-gpu']});
 try{
  const page=await browser.newPage({viewport:{width:390,height:844}}),errors=[],verifies=[];
  page.on('pageerror',e=>errors.push(e.message));
  page.on('request',r=>{if(r.url().endsWith('/webapp/verify'))verifies.push(r.postDataJSON());});
  await page.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():r.abort());
  await page.goto(BASE+'/__ref_test__/',{waitUntil:'domcontentloaded'});
  await page.locator('[data-act="dv-pick"]:not([disabled])').first().waitFor();
  let state=await(await page.request.get(BASE+'/__ref_test__/state')).json();
  assert.equal(state.status,'pending');assert.equal(state.verified,false);
  let target=await page.locator('.vf-target').getAttribute('aria-label');
  const wrong=await page.locator('[data-act="dv-pick"]').evaluateAll((buttons,t)=>buttons.find(b=>b.dataset.v!==t).dataset.v,target);
  await page.locator('[data-act="dv-pick"]').filter({hasText:wrong}).click();
  await page.locator('[data-act="dv-pick"]:not([disabled])').first().waitFor();
  assert.equal(verifies.length,0);
  state=await(await page.request.get(BASE+'/__ref_test__/state')).json();
  assert.equal(state.status,'pending');assert.equal(state.used_challenges,0);
  target=await page.locator('.vf-target').getAttribute('aria-label');
  await page.waitForTimeout(500);
  await page.locator('[data-act="dv-pick"]').filter({hasText:target}).click();
  await page.locator('[data-act="dv-enter"]').waitFor();
  assert.equal(verifies.length,1);assert.equal(verifies[0].ch.trusted,true);
  assert.match(verifies[0].challenge,/^[0-9a-f]{40}$/);
  state=await(await page.request.get(BASE+'/__ref_test__/state')).json();
  assert.equal(state.status,'active');assert.equal(state.verified,true);
  assert.equal(state.proof_version,2);assert.equal(state.used_challenges,1);
  await page.locator('[data-act="dv-enter"]').click();
  await page.locator('[data-act="tab"][data-v="store"]').first().waitFor();
  console.log('PASS wrong answer never counted; trusted website click uses server nonce and activates referral once');
  await page.goto(BASE+'/__ref_test__/?uid=1001',{waitUntil:'domcontentloaded'});
  await page.locator('[data-act="tab"][data-v="profile"]').first().click();
  await page.getByRole('note').filter({hasText:'Pending referral reversal'}).waitFor();
  assert((await page.getByRole('note').innerText()).includes('$0.75'));
  assert((await page.locator('.stats3 .stat').first().innerText()).includes('$0.00'));
  await page.locator('[data-act="lang"][data-v="ar"]').first().click();
  await page.getByRole('note').filter({hasText:'مستحق استرجاع إحالات'}).waitFor();
  assert.equal(await page.locator('html').getAttribute('dir'),'rtl');
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth));
  await page.setViewportSize({width:1360,height:960});
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth));
  state=await(await page.request.get(BASE+'/__ref_test__/state')).json();
  assert.equal(state.balance,0);assert.equal(state.debt,.75);assert.equal(state.money_events,0);
  await page.request.post(BASE+'/__ref_test__/settle');
  await page.evaluate(()=>document.dispatchEvent(new Event('visibilitychange')));
  await page.waitForFunction(()=>!document.querySelector('[role="note"]'));
  state=await(await page.request.get(BASE+'/__ref_test__/state')).json();
  assert.equal(state.balance,0);assert.equal(state.debt,0);assert.equal(state.money_events,1);
  assert.deepEqual(errors,[]);
  console.log('PASS English/Arabic mobile/desktop balances; verification never changes money; settled liability clears without reload');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);});
