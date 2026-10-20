/* Signed local app/DB integration: folder counts, hiding and all four persisted UI languages. */
const {spawn}=require('child_process'),assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const BASE='http://127.0.0.1:8794';
const server=spawn(process.env.LARA_TEST_PYTHON||'python',['verification/serve_round37_test.py'],{
 env:{...process.env,LARA_TEST_PORT:'8794'},stdio:['ignore','pipe','pipe']});
const ready=new Promise((resolve,reject)=>{
 server.stdout.on('data',d=>{if(String(d).includes('ROUND37 LOCAL SERVER'))resolve();});
 server.stderr.on('data',()=>{});server.once('exit',c=>reject(Error('Fixture exited '+c)));
});
(async()=>{
 await ready;
 const browser=await chromium.launch({headless:true,
  ...(process.env.LARA_TEST_BROWSER_EXECUTABLE?{executablePath:process.env.LARA_TEST_BROWSER_EXECUTABLE}:{}),
  args:['--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1','--disable-background-networking',
   '--disable-component-update','--disable-sync','--no-pings','--no-sandbox','--disable-dev-shm-usage','--disable-gpu']});
 const errors=[];
 async function open(){
  const context=await browser.newContext({viewport:{width:360,height:740},isMobile:true,hasTouch:true,reducedMotion:'reduce'});
  const page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():r.abort());
  await page.goto(BASE+'/__round37_test__/',{waitUntil:'domcontentloaded'});
  await page.locator('#page-loading').waitFor({state:'hidden'});return{context,page};
 }
 try{
  let {context,page}=await open();
  assert.equal(await page.locator('#grid .card').count(),1);
  const folderData=await page.locator('.chip[data-act="cat"]').evaluateAll(rows=>rows.map(el=>({id:el.dataset.v,n:el.querySelector('.n')?.textContent})));
  assert.equal(folderData.length,2);assert(folderData.every(x=>x.n==='1'),JSON.stringify(folderData));
  console.log('PASS hidden/empty folders omitted; duplicate aliases and hidden products excluded from count');
  for(const lang of ['en','ru','zh','ar','zh']){
   const response=page.waitForResponse(r=>r.url().endsWith('/webapp/lang')&&r.request().postDataJSON().lang===lang);
   await page.locator(`[data-act="lang"][data-v="${lang}"]`).click();
   assert.equal((await response).status(),200);
   assert.equal(await page.evaluate(()=>document.documentElement.lang),lang==='zh'?'zh-CN':lang);
   const state=await (await page.request.get(BASE+'/__round37_test__/state')).json();
   assert.equal(state.user.ui_lang,lang);assert.equal(state.balance,100);assert.equal(state.orders,0);
  }
  await context.close();({context,page}=await open());
  assert.equal(await page.evaluate(()=>document.documentElement.lang),'zh-CN');
  console.log('PASS ar/en/ru/zh saved through the real local endpoint and restored on a fresh browser context');
  await page.request.get(BASE+'/__round37_test__/hide');
  await page.evaluate(()=>window.__round37Test.refresh());
  assert.equal(await page.locator('#grid .card').count(),0);
  assert.equal(await page.locator('.chip[data-act="cat"]').count(),1);
  await page.request.get(BASE+'/__round37_test__/show');
  await page.evaluate(()=>window.__round37Test.refresh());
  assert.equal(await page.locator('#grid .card').count(),1);
  assert.equal(await page.locator('.chip[data-act="cat"]').count(),2);
  assert.deepEqual(errors,[]);await context.close();
  console.log('PASS hide/show invalidates the live catalogue; no JS errors or financial operations');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);});
