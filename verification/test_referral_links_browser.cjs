/* Round 41: the profile referral card offers the bot link and the store link (copy + share), in every language, no overflow. */
const {spawn}=require('child_process'),assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const PORT='8793',BASE='http://127.0.0.1:'+PORT;
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
  const page=await ctx.newPage(),errors=[],opened=[];let withApp=true;
  page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',r=>r.request().url().startsWith(BASE+'/')?r.continue():(opened.push(r.request().url()),r.abort()));
  /* the fixture bot has no Main Mini App; add the store link the way a bot with it enabled answers */
  await page.route('**/webapp/referrals',async route=>{
   const res=await route.fetch();const data=await res.json();
   assert.equal(data.links.bot,data.link);
   if(withApp)data.links.app=data.links.bot.replace('?start=','?startapp=');
   await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(data)});
  });
  await page.goto(BASE+'/__ref_test__/?uid=1001',{waitUntil:'domcontentloaded'});
  await page.locator('[data-act="tab"][data-v="profile"]').first().click();
  await page.locator('.ref-app .linkbox').waitFor({timeout:15000});
  const bot=await page.locator('.ref-bot .linkbox span').innerText(),app=await page.locator('.ref-app .linkbox span').innerText();
  assert.match(bot,/^https:\/\/t\.me\/[A-Za-z0-9_]+\?start=1001$/);assert.match(app,/\?startapp=1001$/);
  assert.equal(await page.locator('.ref-bot [data-act="copy"]').getAttribute('data-v'),bot);
  assert.equal(await page.locator('.ref-app [data-act="share"]').getAttribute('data-v'),app);
  assert(await page.locator('.ref-pick').isVisible());
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth));
  await page.locator('.invite').screenshot({path:process.env.LARA_SHOT_DIR?process.env.LARA_SHOT_DIR+'/ref-links-en.png':'/dev/null'}).catch(()=>{});
  await page.evaluate(()=>{window.__opened=[];const w=window.Telegram.WebApp;w.openTelegramLink=u=>window.__opened.push(u);});
  await page.locator('.ref-app [data-act="share"]').click();
  const shared=await page.evaluate(()=>window.__opened);
  assert.deepEqual(shared,['https://t.me/share/url?url='+encodeURIComponent(app)]);
  console.log('PASS the referral card shows the bot link and the store link, each with copy and share');
  for(const [lang,label] of [['ar','رابط الموقع'],['ru','Ссылка на магазин'],['zh','商店链接']]){
   await page.locator(`[data-act="lang"][data-v="${lang}"]`).first().click();
   await page.locator('.ref-app .ref-lt').filter({hasText:label}).waitFor();
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),lang);
  }
  if(process.env.LARA_SHOT_DIR)await page.locator('.invite').screenshot({path:process.env.LARA_SHOT_DIR+'/ref-links-zh.png'});
  await page.locator('[data-act="lang"][data-v="ar"]').first().click();
  if(process.env.LARA_SHOT_DIR)await page.locator('.invite').screenshot({path:process.env.LARA_SHOT_DIR+'/ref-links-ar.png'});
  console.log('PASS the labels follow Arabic, Russian and Chinese without horizontal overflow');
  withApp=false;
  await page.goto(BASE+'/__ref_test__/?uid=1001',{waitUntil:'domcontentloaded'});
  await page.locator('[data-act="tab"][data-v="profile"]').first().click();
  await page.locator('.ref-bot .linkbox').waitFor();
  assert.equal(await page.locator('.ref-app').count(),0);assert.equal(await page.locator('.ref-pick').count(),0);
  assert.deepEqual(errors,[]);
  console.log('PASS without Main Mini App only the bot link is shown; zero JS errors');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);});
