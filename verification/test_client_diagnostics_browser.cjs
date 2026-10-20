/* Browser errors, early bootstrap queue and redaction against an isolated HTTP fixture. */
const {spawn}=require('child_process'),assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const BASE='http://127.0.0.1:8783';
const server=spawn(process.env.LARA_TEST_PYTHON||'python',['verification/serve_user_inspection_test.py'],{
 env:{...process.env,LARA_TEST_PORT:'8783'},stdio:['ignore','pipe','pipe']});
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
  const page=await browser.newPage({viewport:{width:390,height:844}}),reports=[];
  await page.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():r.abort());
  await page.route('**/__user_test__/',async route=>{
   const response=await route.fetch(),html=await response.text();
   const marker='</script>\n\n<link rel="preconnect"';
   // The fixture removes Telegram's external SDK. Insert immediately after
   // our collector, before the normal application script connects transport.
   const anchor='</script>\n<!--TG-SDK-->';
   let altered=html.replace('</script>\n\n<link rel="preconnect"', '</script><script>window.dispatchEvent(new ErrorEvent("error",{message:"Early boot proof",filename:"/boot-proof.js",lineno:7}));</script>\n<link rel="preconnect"');
   if(altered===html)altered=html.replace('</script>\n\n<link', '</script><script>window.dispatchEvent(new ErrorEvent("error",{message:"Early boot proof",filename:"/boot-proof.js",lineno:7}));</script>\n<link');
   assert.notEqual(altered,html,'Early-error fixture anchor must match');
   const headers={...response.headers()};delete headers['content-encoding'];delete headers['content-length'];
   await route.fulfill({status:200,headers,body:altered});
  });
  await page.route('**/webapp/client/error',async route=>{
   const body=route.request().postDataJSON(),response=await route.fetch();reports.push(body);await route.fulfill({response});
  });
  await page.goto(BASE+'/__user_test__/',{waitUntil:'domcontentloaded'});
  await page.locator('[data-act="tab"][data-v="admin"]').waitFor();
  for(let n=0;n<100&&!reports.some(r=>r.message==='Early boot proof');n++)await new Promise(r=>setTimeout(r,20));
  assert(reports.some(r=>r.message==='Early boot proof'&&r.location.includes('boot-proof.js:7')));
  console.log('PASS error before application initialization is queued and sent after signed transport connects');
  await page.evaluate(()=>{
   const b=document.createElement('button');b.dataset.act='diagnostic-proof';b.dataset.v='DO-NOT-COLLECT-PASSWORD';document.body.append(b);b.click();b.remove();
   setTimeout(function realBrowserProof(){throw new Error('Runtime proof password=PRIVATE_PASSWORD');},0);
  });
  for(let n=0;n<100&&!reports.some(r=>r.message.includes('Runtime proof'));n++)await new Promise(r=>setTimeout(r,20));
  const runtime=reports.find(r=>r.message.includes('Runtime proof'));
  assert(runtime.stack.includes('realBrowserProof'));assert.equal(runtime.last_action,'diagnostic-proof');
  assert.equal(runtime.build,'lara-33.1');assert(!JSON.stringify(runtime).includes('PRIVATE_PASSWORD'));
  assert(!JSON.stringify(runtime).includes('DO-NOT-COLLECT-PASSWORD'));
  console.log('PASS actual runtime exception includes stack, build and action without form/button values');
  const count=reports.length;
  await page.evaluate(()=>{
   for(let n=0;n<5;n++)window.dispatchEvent(new ErrorEvent('error',{message:'Script error.',filename:'',lineno:0}));
   Promise.reject(new Error('Promise diagnostic proof'));
   const s=document.createElement('script');s.src='/missing-proof-script.js?token=PRIVATE_QUERY';document.head.append(s);
  });
  for(let n=0;n<100&&reports.length<count+3;n++)await new Promise(r=>setTimeout(r,20));
  assert.equal(reports.filter(r=>r.operation==='runtime-masked').length,1);
  const masked=reports.find(r=>r.operation==='runtime-masked');assert(masked.masked);
  assert(masked.location.includes('did not expose'));
  assert(reports.some(r=>r.operation==='promise'&&r.stack.includes('Promise diagnostic proof')));
  assert(reports.some(r=>r.operation==='script-load'&&r.location.includes('missing-proof-script.js')));
  assert(!JSON.stringify(reports).includes('PRIVATE_QUERY'));
  const diagnostics=await(await page.request.get(BASE+'/__user_test__/diagnostics')).json();
  assert(diagnostics.some(r=>r.trace.includes('build: lara-33.1')));
  assert(diagnostics.some(r=>r.trace.includes('cannot be identified')));
  assert(diagnostics.every(r=>r.user_id===9001));
  const state=await(await page.request.get(BASE+'/__user_test__/state')).json();assert.equal(state.balance,100);assert.equal(state.balance_logs,0);
  console.log('PASS masked errors remain explicit, repeats are coalesced, promise/resource source captured, authenticated records redacted');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);});
