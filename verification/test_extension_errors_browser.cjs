/* Round 34: a wallet/translator extension's own script error (e.g. «isTerraExtensionAvailable» from inpage.js) is not reported as a store error; the store's own errors still are. */
const {spawn}=require('child_process'),assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const PORT='8792',BASE='http://127.0.0.1:'+PORT;
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
  const page=await ctx.newPage(),reports=[];
  await page.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():r.abort());
  await page.route('**/webapp/client/error',async route=>{reports.push(route.request().postDataJSON());await route.fulfill({status:200,contentType:'application/json',body:'{"ok":true}'});});
  await page.goto(BASE+'/__ref_test__/?uid=1001',{waitUntil:'domcontentloaded'});
  await page.locator('[data-act="tab"][data-v="store"]').first().waitFor({timeout:20000});
  await page.evaluate(()=>{
   const fire=(filename,message)=>window.dispatchEvent(new ErrorEvent('error',{message,filename,lineno:164,colno:178359,error:new TypeError(message)}));
   fire('chrome-extension://lgmpcpglpngdoalbgeoldeajfclnhafa/inpage.js',"Uncaught TypeError: Cannot assign to read only property 'isTerraExtensionAvailable' of object '#<Window>'");
   fire('moz-extension://5f1c/content.js','Extension A failed');
   fire('safari-web-extension://AB12/inpage.js','Extension B failed');
   fire('webkit-masked-url://hidden/','Extension C failed');
   const e=new Error('Extension promise failed');e.stack='Error: Extension promise failed\n    at hook (chrome-extension://abc/inpage.js:1:9)';
   window.dispatchEvent(new PromiseRejectionEvent('unhandledrejection',{promise:Promise.resolve(),reason:e}));
  });
  await page.waitForTimeout(800);
  assert.deepEqual(reports.filter(r=>/extension|Extension|isTerra/.test(JSON.stringify(r))),[],JSON.stringify(reports));
  console.log('PASS browser-extension errors (chrome / firefox / safari / masked, error + promise) are not sent as store errors');
  await page.evaluate(base=>window.dispatchEvent(new ErrorEvent('error',{message:'Store render failed',filename:base+'/__ref_test__/',lineno:12,colno:3,error:new Error('Store render failed')})),BASE);
  await page.waitForTimeout(800);
  const own=reports.find(r=>r.message==='Store render failed');
  assert(own&&/__ref_test__/.test(own.location)&&own.build==='lara-38.1',JSON.stringify(reports));
  console.log('PASS the store\'s own error is still reported with its location and build lara-38.1');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);});
