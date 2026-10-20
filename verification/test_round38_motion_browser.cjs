/* Original motion on weak/strong devices: real animated assets and advancing Lottie frames, loopback only. */
const {spawn}=require('child_process'),fs=require('fs'),zlib=require('zlib'),crypto=require('crypto'),assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const BASE='http://127.0.0.1:8795';
const server=spawn(process.env.LARA_TEST_PYTHON||'python',['verification/serve_cart_test.py'],{
 env:{...process.env,LARA_TEST_PORT:'8795'},stdio:['ignore','pipe','pipe']});
const ready=new Promise((resolve,reject)=>{
 server.stdout.on('data',d=>{if(String(d).includes('LOCAL CART SERVER'))resolve();});
 server.stderr.on('data',()=>{});server.once('exit',c=>reject(Error('Fixture exited '+c)));
});
const animation=zlib.gzipSync(Buffer.from(JSON.stringify({v:'5.7.4',fr:30,ip:0,op:120,w:96,h:96,nm:'Motion test',ddd:0,assets:[],layers:[{
 ddd:0,ind:1,ty:4,nm:'ellipse',sr:1,ks:{o:{a:0,k:100},r:{a:1,k:[{t:0,s:[0],e:[360]},{t:120,s:[360]}]},p:{a:0,k:[48,48,0]},a:{a:0,k:[0,0,0]},s:{a:0,k:[100,100,100]}},
 shapes:[{ty:'el',p:{a:0,k:[0,0]},s:{a:0,k:[64,32]}},{ty:'fl',c:{a:0,k:[0.1,0.5,1,1]},o:{a:0,k:100},r:1}],ip:0,op:120,st:0,bm:0}]})));
const digest=buffer=>crypto.createHash('sha256').update(buffer).digest('hex');
(async()=>{
 await ready;
 const browser=await chromium.launch({headless:true,
  ...(process.env.LARA_TEST_BROWSER_EXECUTABLE?{executablePath:process.env.LARA_TEST_BROWSER_EXECUTABLE}:{}),
  args:['--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1','--disable-background-networking',
   '--disable-component-update','--disable-sync','--no-pings','--no-sandbox','--disable-dev-shm-usage','--disable-gpu']});
 const results=[];
 try{
  for(const profile of [
   {name:'weak Android',memory:2,cores:2,height:1500,mobile:true},
   {name:'data saver Android',memory:2,cores:2,height:740,mobile:true,saveData:true},
   {name:'strong Android',memory:8,cores:8,height:844,mobile:true},
   {name:'iOS without hardware hints',memory:0,cores:0,height:844,mobile:true,ios:true},
   {name:'no IntersectionObserver',memory:2,cores:2,height:740,mobile:true,noObserver:true}
  ]){
   const context=await browser.newContext({viewport:{width:390,height:profile.height},isMobile:profile.mobile,hasTouch:true,
    reducedMotion:'no-preference',userAgent:profile.ios?'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 Version/16.0 Mobile/15E148 Safari/604.1':'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/130.0.0.0 Mobile Safari/537.36'});
   const page=await context.newPage(),errors=[],posters=[];
   page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>{if(r.url().includes('/emoji/poster/'))posters.push(r.url());});
   await page.addInitScript(p=>{
    Object.defineProperty(navigator,'deviceMemory',{get:()=>p.memory});Object.defineProperty(navigator,'hardwareConcurrency',{get:()=>p.cores});
    Object.defineProperty(navigator,'connection',{get:()=>({saveData:!!p.saveData})});
    if(p.noObserver)delete window.IntersectionObserver;
   },profile);
   await page.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():r.abort());
   await page.route('**/webapp/ce/*',r=>r.fulfill({status:200,contentType:'application/x-tgsticker',body:animation}));
   let release;const gate=new Promise(resolve=>release=resolve);
   await page.route('**/webapp/bootstrap',async route=>{
    const response=await route.fetch(),data=await response.json(),sample=data.products[0];
    data.products=Array.from({length:24},(_,i)=>({...sample,id:i===0?'101':String(10000+i),
     name_en:'Motion '+String(i).padStart(3,'0'),name_ar:'Motion '+i,ae:i%2?'fire':null,
     emoji_id:i%2?null:String(910000+i),cats:['motion'],stock:10,in_stock:true}));
    data.catalogs=[{id:'motion',name_en:'Motion',name_ar:'Motion'}];
    await gate;await route.fulfill({response,json:data});
   });
   await page.goto(BASE+'/__cart_test__/',{waitUntil:'domcontentloaded'});
   await page.locator('#page-loading').waitFor({state:'visible'});
   const loader=await page.evaluate(()=>({mask:getComputedStyle(document.querySelector('.lm-rib')).mask,
    draw:getComputedStyle(document.querySelector('.lm-draw')).animationName,
    lite:window.__LARA_PERF__.lite,unlimited:window.__LARA_PERF__.mediaLimit===Infinity}));
   assert.notEqual(loader.mask,'none');assert.notEqual(loader.draw,'none');assert.equal(loader.lite,false);assert(loader.unlimited);
   release();await page.locator('#grid .card').first().waitFor();await page.locator('#page-loading').waitFor({state:'hidden'});
   assert.equal(await page.locator('#grid .card').count(),24,'Initial product list is restored in full');
   await page.waitForFunction(()=>window.lottie&&window.lottie.getRegisteredAnimations().some(a=>!a.isPaused&&a.currentFrame>1));
   await page.waitForTimeout(500);
   const playing=await page.evaluate(()=>window.lottie.getRegisteredAnimations().filter(a=>!a.isPaused).length);
   if(profile.height===1500)assert(playing>3,`Animations unexpectedly capped: ${playing}`);
   const before=await page.evaluate(()=>window.lottie.getRegisteredAnimations().filter(a=>!a.isPaused).map(a=>a.currentFrame));
   await page.waitForTimeout(250);
   const after=await page.evaluate(()=>window.lottie.getRegisteredAnimations().filter(a=>!a.isPaused).map(a=>a.currentFrame));
   assert(after.some((value,i)=>value!==before[i]),'Custom emoji frames must actually advance');
   const native=page.locator('#grid img[data-ae="fire"]').first();
   await native.waitFor();assert((await native.getAttribute('src')).endsWith('emoji/fire.webp'));
   await native.evaluate(el=>{for(let node=el;node&&node!==document.body;node=node.parentElement){node.style.animation='none';node.style.transition='none';node.style.transform='none';}});
   const first=await native.screenshot({animations:'allow'});await page.waitForTimeout(400);
   const second=await native.screenshot({animations:'allow'});
   assert.notEqual(digest(first),digest(second),'The native WebP emoji must animate, rather than showing a still poster');
   assert.equal(posters.length,0,'The restored store never substitutes still emoji posters');
   await page.locator('#q').fill('Motion 000');await page.waitForFunction(()=>document.querySelectorAll('#grid .card').length===1);
   await page.locator('#q').fill('');await page.waitForFunction(()=>document.querySelectorAll('#grid .card').length===24);
   await page.locator('#grid .card[data-v="101"]').click();await page.locator('.psheet:not(.closing)').waitFor();
   await page.locator('.psheet:not(.closing) [data-act="sheet-close"]').click();
   assert.deepEqual(errors,[]);
   results.push({profile:profile.name,passed:true,playing,loader,native_webp_frames_changed:true,poster_requests:posters.length});
   console.log('PASS original loader, all product animations, advancing Lottie/native WebP frames:',profile.name);
   await context.close();
  }
  if(process.env.LARA_TEST_MOTION_RESULT)fs.writeFileSync(process.env.LARA_TEST_MOTION_RESULT,JSON.stringify(results,null,2));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);});
