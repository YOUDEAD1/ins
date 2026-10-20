/* Mobile rendering regression: signed loopback API, fake products, no external integrations. */
const {spawn}=require('child_process'),fs=require('fs'),zlib=require('zlib'),assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const BASE='http://127.0.0.1:8793',optimized=process.env.LARA_TEST_EXPECT_OPTIMIZED!=='0';
const server=spawn(process.env.LARA_TEST_PYTHON||'python',['verification/serve_cart_test.py'],{
 env:{...process.env,LARA_TEST_PORT:'8793'},stdio:['ignore','pipe','pipe']});
const ready=new Promise((resolve,reject)=>{
 server.stdout.on('data',d=>{if(String(d).includes('LOCAL CART SERVER'))resolve();});
 server.stderr.on('data',()=>{});server.once('exit',c=>reject(Error('Fixture exited '+c)));
});
const animation=zlib.gzipSync(Buffer.from(JSON.stringify({v:'5.7.4',fr:30,ip:0,op:60,w:96,h:96,nm:'Local test',ddd:0,assets:[],layers:[{
 ddd:0,ind:1,ty:4,nm:'circle',sr:1,ks:{o:{a:0,k:100},r:{a:1,k:[{t:0,s:[0],e:[360]},{t:60,s:[360]}]},p:{a:0,k:[48,48,0]},a:{a:0,k:[0,0,0]},s:{a:0,k:[100,100,100]}},
 shapes:[{ty:'el',p:{a:0,k:[0,0]},s:{a:0,k:[64,48]}},{ty:'fl',c:{a:0,k:[0.1,0.5,1,1]},o:{a:0,k:100},r:1}],ip:0,op:60,st:0,bm:0}]})));
(async()=>{
 await ready;
 const browser=await chromium.launch({headless:true,
  ...(process.env.LARA_TEST_BROWSER_EXECUTABLE?{executablePath:process.env.LARA_TEST_BROWSER_EXECUTABLE}:{}),
  args:['--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1','--disable-background-networking',
   '--disable-component-update','--disable-sync','--no-pings','--no-sandbox','--disable-dev-shm-usage','--disable-gpu']});
 try{
  const page=await browser.newPage({viewport:{width:360,height:740},isMobile:true,hasTouch:true,
   userAgent:'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/130.0.0.0 Mobile Safari/537.36'});
  const errors=[],calls=[],media=[];
  page.on('pageerror',e=>errors.push(e.message));
  page.on('request',r=>{const u=new URL(r.url());if(u.pathname.startsWith('/webapp/'))calls.push(u.pathname);if(u.pathname.includes('/emoji/'))media.push(u.pathname);});
  await page.addInitScript(()=>{
   Object.defineProperty(navigator,'deviceMemory',{get:()=>2});
   Object.defineProperty(navigator,'hardwareConcurrency',{get:()=>2});
   window.__perf={long:[],frames:[],last:0};
   try{new PerformanceObserver(list=>{for(const e of list.getEntries())window.__perf.long.push({at:e.startTime,duration:e.duration});}).observe({type:'longtask',buffered:true});}catch(e){}
   function tick(t){const p=window.__perf;if(p.last&&p.frames.length<3000)p.frames.push({at:t,gap:t-p.last});p.last=t;requestAnimationFrame(tick);}requestAnimationFrame(tick);
  });
  await page.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():r.abort());
  await page.route('**/__cart_test__/',async route=>{
   const response=await route.fetch(),current=await response.text();let html=current;
   if(process.env.LARA_TEST_SOURCE_HTML){
    const prefix=current.slice(current.indexOf('<head>')+6,current.indexOf('<meta charset'));
    html=fs.readFileSync(process.env.LARA_TEST_SOURCE_HTML,'utf8').replace('<head>','<head>'+prefix);
   }
   const headers={...response.headers()};delete headers['content-length'];delete headers['content-encoding'];
   await route.fulfill({status:response.status(),headers,body:html});
  });
  await page.route('**/webapp/ce/*',r=>r.fulfill({status:200,contentType:'application/x-tgsticker',body:animation}));
  let release;
  const gate=new Promise(r=>release=r);
  await page.route('**/webapp/bootstrap',async route=>{
   const response=await route.fetch(),data=await response.json(),sample=data.products[0];
   assert(sample,'Fixture bootstrap missing a product');
   data.products=data.products.concat(Array.from({length:597},(_,i)=>({...sample,id:String(10000+i),
    name_en:(i%2?'Netflix':'Spotify')+' Mobile Test '+String(i).padStart(4,'0'),name_ar:'Mobile Test '+i,
    ae:['fire','crown','store','key'][i%4],emoji_id:i%5===0?String(900000+i%12):null,
    cats:[i%2?'a':'b'],in_stock:i%7!==0,stock:i%7===0?0:10,price:i%10+1})));
   data.catalogs=[{id:'a',name_en:'Netflix',name_ar:'Netflix'},{id:'b',name_en:'Spotify',name_ar:'Spotify'}];
   await gate;await route.fulfill({response,json:data});
  });
  const cdp=await page.context().newCDPSession(page);await cdp.send('Emulation.setCPUThrottlingRate',{rate:6});
  await page.goto(BASE+'/__cart_test__/',{waitUntil:'domcontentloaded'});
  await page.locator('#page-loading').waitFor({state:'visible'});
  const loadingStart=await page.evaluate(()=>performance.now());
  await page.waitForTimeout(1500);
  const loading=await page.evaluate(start=>({mask:getComputedStyle(document.querySelector('.lm-rib')).mask,
   frameGaps:window.__perf.frames.filter(f=>f.at>=start).map(f=>f.gap),
   longTasks:window.__perf.long.filter(e=>e.at>=start).map(e=>e.duration)}),loadingStart);
  if(process.env.LARA_TEST_SHOTS){fs.mkdirSync(process.env.LARA_TEST_SHOTS,{recursive:true});await page.screenshot({path:process.env.LARA_TEST_SHOTS+'/loading.png'});}
  const renderStart=await page.evaluate(()=>performance.now());release();
  await page.locator('#results .card').first().waitFor();
  await page.locator('#page-loading').waitFor({state:'hidden'});
  const first=await page.evaluate(start=>({ms:performance.now()-start,cards:document.querySelectorAll('#grid .card').length,
   low:document.documentElement.dataset.perf||'',longTasks:window.__perf.long.filter(e=>e.at>=start).map(e=>e.duration)}),renderStart);
  if(optimized){assert(first.cards<=24,JSON.stringify(first));assert.equal(first.low,'lite');assert.equal(loading.mask,'none');}
  console.log('PASS 600-product fixture; first render:',JSON.stringify({ms:Math.round(first.ms),cards:first.cards,mode:first.low}));
  if(process.env.LARA_TEST_SHOTS)await page.screenshot({path:process.env.LARA_TEST_SHOTS+'/products.png'});
  await page.locator('#q').fill('Netflix');
  await page.waitForFunction(()=>!Array.from(document.querySelectorAll('#grid .card')).some(el=>!el.textContent.includes('Netflix')));
  assert(await page.locator('#grid .card').count()>0);
  if(optimized)assert(await page.locator('#grid .card').count()<=24);
  await page.locator('#q').fill('');await page.waitForTimeout(350);
  await page.locator('[data-act="sort"]').click();
  await page.locator('[data-act="stockonly"]').click();
  assert.equal(await page.locator('#grid .card.oos').count(),0);
  await page.locator('[data-act="stockonly"]').click();
  await page.waitForTimeout(600);
  const peakBefore=await page.evaluate(()=>window.lottie?window.lottie.getRegisteredAnimations().length:0);
  for(let n=0;n<5;n++){
   await page.locator('[data-act="cat"][data-v="a"]').click();
   await page.locator('[data-act="cat"][data-v="all"]').click();
  }
  await page.waitForTimeout(700);
  const peakAfter=await page.evaluate(()=>window.lottie?window.lottie.getRegisteredAnimations().length:0);
  if(optimized)assert(peakAfter<=24,`Leaked Lottie players: ${peakAfter}`);
  console.log('PASS search, sort, stock filter and category changes; Lottie players:',peakBefore,'->',peakAfter);
  const profiles=[];
  if(optimized){
   const initial=await page.locator('#grid .card').count();
   await page.locator('#products-more').scrollIntoViewIfNeeded();
   await page.waitForFunction(n=>document.querySelectorAll('#grid .card').length>n,initial);
   for(let n=0;n<3;n++){await page.locator('[data-act="products-more"]').click();await page.waitForTimeout(60);}
   assert(await page.locator('#grid .card').count()>initial);
   await page.waitForTimeout(200);
   assert((await page.evaluate(()=>window.lottie?window.lottie.getRegisteredAnimations().length:0))<=24,'Offscreen players should be released on mobile');
   await page.locator('#q').fill('Mobile Test 0596');
   await page.waitForFunction(()=>document.querySelector('#grid .card[data-v="10596"]'));
   await page.locator('#grid .card[data-v="10596"]').click();await page.locator('.psheet:not(.closing)').waitFor();
   await page.locator('.psheet:not(.closing) [data-act="sheet-close"]').click();
   await page.locator('#q').fill('');await page.waitForTimeout(350);
   console.log('PASS incremental rendering and search/open of a product beyond the first batch');
   await page.waitForTimeout(300);
   const moving=await page.evaluate(()=>({
    lottie:window.lottie?window.lottie.getRegisteredAnimations().filter(a=>!a.isPaused).length:0,
    images:[...document.querySelectorAll('img[data-ae]')].filter(el=>/\/emoji\/[^/]+\.webp$/.test(el.src)).length}));
   assert(moving.lottie+moving.images<=3,JSON.stringify(moving));
   await page.emulateMedia({reducedMotion:'reduce'});await page.waitForTimeout(150);
   assert.equal(await page.evaluate(()=>window.lottie?window.lottie.getRegisteredAnimations().filter(a=>!a.isPaused).length:0),0);
   assert.equal(await page.evaluate(()=>[...document.querySelectorAll('img[data-ae]')].filter(el=>/\/emoji\/[^/]+\.webp$/.test(el.src)).length),0);
   assert.equal(await page.locator('.ld-mark').evaluate(el=>getComputedStyle(el).animationName),'none');
   await page.emulateMedia({reducedMotion:'no-preference'});await page.waitForTimeout(150);
   await page.evaluate(()=>{Object.defineProperty(document,'hidden',{configurable:true,get:()=>true});document.dispatchEvent(new Event('visibilitychange'));});
   assert.equal(await page.evaluate(()=>window.lottie?window.lottie.getRegisteredAnimations().filter(a=>!a.isPaused).length:0),0);
   assert.equal(await page.evaluate(()=>[...document.querySelectorAll('img[data-ae]')].filter(el=>/\/emoji\/[^/]+\.webp$/.test(el.src)).length),0);
   console.log('PASS shared three-animation budget; reduced motion and background pause both native emoji and Lottie');
   for(const profile of [
    {name:'strong Android',mode:'mobile',memory:8,cores:8,mobile:true},
    {name:'iOS without memory hints',mode:'mobile',memory:0,cores:0,mobile:true,ios:true,deep:true},
    {name:'no IntersectionObserver',mode:'lite',memory:2,cores:2,mobile:true,noObserver:true},
    {name:'desktop',mode:'full',memory:8,cores:8,mobile:false}
   ]){
    const prof=await browser.newPage({viewport:profile.mobile?{width:390,height:844}:{width:1280,height:900},
     isMobile:profile.mobile,hasTouch:profile.mobile,
     userAgent:profile.ios?'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 Version/16.0 Mobile/15E148 Safari/604.1':profile.mobile?'Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/130.0.0.0 Mobile Safari/537.36':undefined});
    const profileErrors=[];prof.on('pageerror',e=>profileErrors.push(e.message));
    await prof.addInitScript(p=>{
     Object.defineProperty(navigator,'deviceMemory',{get:()=>p.memory});Object.defineProperty(navigator,'hardwareConcurrency',{get:()=>p.cores});
     if(p.noObserver)delete window.IntersectionObserver;
    },profile);
    await prof.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():r.abort());
    await prof.route('**/webapp/bootstrap',async route=>{
     const response=await route.fetch(),data=await response.json(),sample=data.products[0];
     data.products=data.products.concat(Array.from({length:197},(_,i)=>({...sample,id:String(20000+i),name_en:'Profile product '+i,name_ar:'Profile product '+i,emoji_id:null,ae:'fire'})));
     await route.fulfill({response,json:data});
    });
    await prof.goto(BASE+'/__cart_test__/'+(profile.deep?'?p=20196':''),{waitUntil:'domcontentloaded'});
    await prof.locator('#grid .card').first().waitFor();await prof.locator('#page-loading').waitFor({state:'hidden'});
    assert.equal(await prof.evaluate(()=>document.documentElement.dataset.perf),profile.mode);
    const motion=await prof.evaluate(()=>({mask:getComputedStyle(document.querySelector('.lm-rib')).mask,
      draw:getComputedStyle(document.querySelector('.lm-draw')).animationName,
      unlimited:window.__LARA_PERF__.mediaLimit===Infinity}));
    if(profile.mode==='lite'){
     assert.equal(motion.mask,'none');assert.equal(motion.draw,'none');assert.equal(motion.unlimited,false);
    }else{
     assert.notEqual(motion.mask,'none','Normal phones keep the SVG loader mask');
     assert.notEqual(motion.draw,'none','Normal phones keep the original stroke animation');
     assert.equal(motion.unlimited,true,'Only limited devices get the three-animation budget');
    }
    assert(await prof.locator('#grid .card').count()<=48);
    if(profile.deep){await prof.locator('.psheet:not(.closing)').waitFor();const text=await prof.locator('.psheet:not(.closing)').innerText();assert(text.includes('Profile product 196'),text);await prof.locator('.psheet:not(.closing) [data-act="sheet-close"]').click();}
    if(profile.noObserver){const n=await prof.locator('#grid .card').count();await prof.locator('[data-act="products-more"]').click();assert((await prof.locator('#grid .card').count())>n);}
    await prof.locator('[data-act="lang"][data-v="ar"]').click();assert.equal(await prof.evaluate(()=>document.documentElement.dir),'rtl');
    assert(await prof.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));assert.deepEqual(profileErrors,[]);
    profiles.push({profile:profile.name,passed:true,motion});await prof.close();
   }
   console.log('PASS strong Android, iOS without hardware hints, no observer fallback, desktop, Arabic RTL and direct links outside the first batch');
  }
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  assert.deepEqual(errors,[]);
  const result={cpu_throttle:6,viewport:'360x740',synthetic_products:600,loading,first,
   lottie:{before:peakBefore,after:peakAfter},animated_builtin_requests:media.filter(u=>/\/emoji\/[^/]+\.webp$/.test(u)).length,
   poster_requests:media.filter(u=>u.includes('/emoji/poster/')).length,profiles,errors,calls};
  if(process.env.LARA_TEST_PERF_RESULT)fs.writeFileSync(process.env.LARA_TEST_PERF_RESULT,JSON.stringify(result,null,2));
  console.log('PASS zero JS errors, mobile layout; no financial operations');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);});
