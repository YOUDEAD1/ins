/* Actual browser, signed local HTTP, fake integrations and blocked external network. */
const {spawn}=require('child_process');
const assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const BASE='http://127.0.0.1:8771';
const server=spawn(process.env.LARA_TEST_PYTHON||'python',['verification/serve_cart_test.py'],{
 env:{...process.env,LARA_TEST_PORT:'8771'},stdio:['ignore','pipe','pipe']});
const ready=new Promise((resolve,reject)=>{
 server.stdout.on('data',d=>{if(String(d).includes('LOCAL CART SERVER'))resolve();});
 server.stderr.on('data',()=>{});server.once('exit',c=>reject(Error('Fixture exited '+c)));
});
(async()=>{
 await ready;
 const browser=await chromium.launch({headless:true,
  ...(process.env.LARA_TEST_BROWSER_EXECUTABLE?{executablePath:process.env.LARA_TEST_BROWSER_EXECUTABLE}:{}),
  args:['--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1','--disable-background-networking',
   '--disable-component-update','--disable-sync','--no-pings','--no-sandbox','--disable-dev-shm-usage','--disable-gpu']});
 try{
  const page=await browser.newPage({viewport:{width:390,height:844}}),errors=[],catalog=[];
  page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():r.abort());
  // Expose the real closure only in the served test HTML, to await completed
  // parsing/finally without relying on global network-idle during emoji loading.
  await page.route('**/__cart_test__/',async route=>{
   const response=await route.fetch(),source=await response.text();
   const anchor='let lastCat = 0, catalogFlight = null;';
   assert(source.includes(anchor));
   const html=source.replace(anchor,anchor+'\nwindow.__networkTest={refresh:()=>refreshCatalog(),busy:()=>catalogFlight!==null};');
   const headers={...response.headers()};delete headers['content-length'];delete headers['content-encoding'];
   await route.fulfill({status:response.status(),headers,body:html});
  });
  let hold=false,release=null;
  await page.route('**/webapp/catalog',async route=>{
   const body=route.request().postDataJSON(),response=await route.fetch(),result=await response.json();
   catalog.push({body,result});
   if(hold)await new Promise(resolve=>{release=resolve;});
   await route.fulfill({response});
  });
  await page.goto(BASE+'/__cart_test__/',{waitUntil:'domcontentloaded'});
  await page.locator('[data-act="product"][data-v="101"]').waitFor();
  const count=await page.locator('#results [data-act="product"]').count();assert.equal(count,3);
  await page.clock.install();
  let got=page.waitForResponse(r=>r.url()===BASE+'/webapp/catalog');
  await page.clock.fastForward(80000);await got;await page.waitForFunction(()=>!window.__networkTest.busy());
  assert.equal(catalog.length,1);assert(catalog[0].body.catalog_version);
  assert.equal(catalog[0].result.not_modified,true);assert(!catalog[0].result.products);
  assert.equal(await page.locator('#results [data-act="product"]').count(),count);
  console.log('PASS unchanged catalog uses version and preserves product cards');
  await page.locator('[data-act="tab"][data-v="profile"]').click();
  await page.clock.fastForward(130000);
  assert.equal(catalog.length,1);
  got=page.waitForResponse(r=>r.url()===BASE+'/webapp/catalog');
  await page.locator('[data-act="tab"][data-v="store"]').click();await got;await page.waitForFunction(()=>!window.__networkTest.busy());
  assert.equal(catalog.length,2);
  console.log('PASS no catalogue polling on profile; returning to store refreshes');
  hold=true;
  await page.evaluate(()=>{window.__networkTest.refresh();});
  // Wait for the local handler without advancing the browser's 25-second timeout.
  for(let n=0;n<100&&!release;n++)await new Promise(r=>setTimeout(r,20));
  assert(release,'Pending local catalogue request did not arrive');
  assert.equal(catalog.length,3);
  await page.evaluate(()=>{
   window.__networkTestDateNow=Date.now;
   Date.now=()=>window.__networkTestDateNow()+65000;
   for(let n=0;n<3;n++)document.dispatchEvent(new Event('visibilitychange'));
  });
  await new Promise(r=>setTimeout(r,150));assert.equal(catalog.length,3);
  got=page.waitForResponse(r=>r.url()===BASE+'/webapp/catalog');hold=false;release();await got;await page.waitForFunction(()=>!window.__networkTest.busy());
  await page.evaluate(()=>{Date.now=window.__networkTestDateNow;delete window.__networkTestDateNow;});
  assert.equal(await page.locator('#results [data-act="product"]').count(),count);
  console.log('PASS overlapping refresh triggers share one in-flight request');
  const state=await (await page.request.get(BASE+'/__cart_test__/state')).json();
  assert.equal(state.orders,0);assert.equal(state.balance,100);assert.deepEqual(errors,[]);
  console.log('PASS zero JS exceptions, no purchases or charges');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{
 server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);
});
