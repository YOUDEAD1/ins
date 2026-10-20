/* Real Chromium with an isolated local server. Telegram and payment APIs are blocked. */
const assert=require('assert/strict');
const {spawn}=require('child_process');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const BASE='http://127.0.0.1:8770';
const server=spawn(process.env.LARA_TEST_PYTHON||'python', ['verification/serve_cart_test.py'],{
 env:{...process.env,LARA_TEST_PORT:'8770'},stdio:['ignore','pipe','pipe']});
const ready=new Promise((resolve,reject)=>{
 server.stdout.on('data',d=>{if(String(d).includes('LOCAL CART SERVER'))resolve();});
 server.stderr.on('data',()=>{});server.once('exit',c=>reject(Error('Local fixture exited '+c)));
});
(async()=>{
 await ready;
 const browser=await chromium.launch({headless:true,
  ...(process.env.LARA_TEST_BROWSER_EXECUTABLE?{executablePath:process.env.LARA_TEST_BROWSER_EXECUTABLE}:{}),
  args:['--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1','--disable-background-networking',
   '--disable-component-update','--disable-sync','--no-pings','--no-sandbox','--disable-dev-shm-usage','--disable-gpu']});
 try{
  for(const [sp,query,title] of [['p_101','','Netflix Premium'],['p_102','','Spotify Premium'],
                               ['p_102','?p=101','Netflix Premium'],['','','']]){
   const context=await browser.newContext({viewport:{width:390,height:844}});
   const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
   await page.route('**/*',route=>route.request().url().startsWith(BASE+'/')?route.continue():route.abort());
   await page.route('**/__cart_test__/**',async route=>{
    const response=await route.fetch();
    if(route.request().url().includes('/state'))return route.fulfill({response});
    const body=(await response.text()).replace('initDataUnsafe:{user:',
        'initDataUnsafe:{start_param:'+JSON.stringify(sp)+',user:');
    return route.fulfill({response,body});
   });
   await page.goto(BASE+'/__cart_test__/'+query,{waitUntil:'domcontentloaded'});
   await page.locator('[data-act="cart-open"]').first().waitFor();
   if(title){
    const sheet=page.locator('.psheet:not(.closing)').last();await sheet.waitFor();
    assert((await sheet.innerText()).includes(title));
    assert(await sheet.locator('[data-act="buy"]').count());
   }else assert.equal(await page.locator('.psheet:not(.closing)').count(),0);
   const state=await (await page.request.get(BASE+'/__cart_test__/state')).json();
   assert.equal(state.orders,0);assert.equal(state.balance,100);
   assert.deepEqual(errors,[]);
   console.log('PASS channel Mini App launch',sp||'store',query||'',title||'catalogue','no purchase');
   await context.close();
  }
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{
 server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);
});
