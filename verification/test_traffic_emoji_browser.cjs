/* Real Chromium, original animated assets, signed fake user and loopback-only API. */
const fs = require('fs'), assert = require('assert/strict'), crypto = require('crypto'), zlib = require('zlib');
const {spawn} = require('child_process');
const {chromium} = require(process.env.LARA_TEST_PLAYWRIGHT || 'playwright');
const BASE = 'http://127.0.0.1:8801', ID = '990001';
const server = spawn(process.env.LARA_TEST_PYTHON || 'python', ['verification/serve_cart_test.py'], {
  env: {...process.env, LARA_TEST_PORT: '8801'}, stdio: ['ignore', 'pipe', 'pipe']
});
const ready = new Promise((resolve, reject) => {
  server.stdout.on('data', data => {if (String(data).includes('LOCAL CART SERVER')) resolve();});
  server.stderr.on('data', () => {}); server.once('exit', code => reject(Error('Fixture exited ' + code)));
});
const animation = zlib.gzipSync(Buffer.from(JSON.stringify({v:'5.7.4',fr:30,ip:0,op:60,w:96,h:96,ddd:0,assets:[],layers:[{
  ddd:0,ind:1,ty:4,nm:'spinner',sr:1,ks:{o:{a:0,k:100},r:{a:1,k:[{t:0,s:[0],e:[360]},{t:60,s:[360]}]},
    p:{a:0,k:[48,48,0]},a:{a:0,k:[0,0,0]},s:{a:0,k:[100,100,100]}},
  shapes:[{ty:'el',p:{a:0,k:[0,0]},s:{a:0,k:[64,48]}},{ty:'fl',c:{a:0,k:[.1,.5,1,1]},o:{a:0,k:100},r:1}],ip:0,op:60,st:0,bm:0
}]})));
const digest = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
(async () => {
  await ready;
  const browser = await chromium.launch({headless:true,
    ...(process.env.LARA_TEST_BROWSER_EXECUTABLE ? {executablePath:process.env.LARA_TEST_BROWSER_EXECUTABLE} : {}),
    args:['--no-sandbox','--disable-dev-shm-usage','--disable-background-networking','--disable-component-update','--disable-sync','--no-pings','--disable-gpu']});
  const results = [];
  try {
    for (const mode of ['baseline','optimized','failed','stalled']) {
      const context = await browser.newContext({viewport:{width:390,height:844},isMobile:true,hasTouch:true,reducedMotion:'no-preference'});
      const page = await context.newPage(); page.setDefaultTimeout(15000);
      const errors = [], nativeRequests = [];
      page.on('pageerror', error => errors.push(error.message));
      page.on('request', request => {if (new URL(request.url()).pathname === '/app/emoji/purse.webp') nativeRequests.push(request.url());});
      const nativeRequested = mode === 'optimized' ? null : page.waitForRequest(
        request => new URL(request.url()).pathname === '/app/emoji/purse.webp');
      await page.route('**/*', route => route.request().url().startsWith(BASE + '/') ? route.continue() : route.abort());
      if (mode === 'baseline') await page.route('**/__cart_test__/', async route => {
        const response = await route.fetch(); let html = await response.text();
        const start = html.indexOf('/* A Premium emoji must not download a second full animation just as a placeholder. */');
        const end = html.indexOf('\nboot();', start);
        assert(start > 0 && end > start);
        html = html.slice(0, start) + html.slice(end);
        const headers = {...response.headers()}; delete headers['content-length']; delete headers['content-encoding'];
        await route.fulfill({response,headers,body:html});
      });
      await page.route('**/webapp/bootstrap', async route => {
        const response = await route.fetch(), data = await response.json();
        data.nav = {balance:ID};
        await route.fulfill({response,json:data});
      });
      let release; const gate = new Promise(resolve => {release = resolve;});
      await page.route('**/webapp/ce/' + ID, async route => {
        if (mode === 'failed') return route.fulfill({status:404,body:'not available'});
        await gate;
        return route.fulfill({status:200,contentType:'application/x-tgsticker',body:animation});
      });
      await page.goto(BASE + '/__cart_test__/', {waitUntil:'domcontentloaded'});
      const target = page.locator('.chip-bal [data-ce="' + ID + '"]');
      await target.waitFor();
      if (mode === 'baseline') {
        await target.locator('img[data-ae="purse"]').waitFor();
        await nativeRequested;
        assert(nativeRequests.length >= 1, 'Baseline should download the native waiting animation');
      } else if (mode === 'optimized') {
        await target.locator('[data-native-fallback="purse"]').waitFor();
        assert.equal(nativeRequests.length, 0);
      } else {
        const native = target.locator('img[data-ae="purse"]');
        await native.waitFor();
        await nativeRequested;
        assert(nativeRequests.length >= 1, 'A failed/stalled Premium request must restore the original animated WebP: ' + mode);
        await native.evaluate(element => {for (let node = element; node && node !== document.body; node = node.parentElement) {
          node.style.animation='none'; node.style.transition='none'; node.style.transform='none';
        }});
        const first = await native.screenshot({animations:'allow'});
        await page.waitForTimeout(430);
        const second = await native.screenshot({animations:'allow'});
        assert.notEqual(digest(first), digest(second), 'Native fallback must really animate');
      }
      release();
      if (mode !== 'failed') {
        await target.locator('svg').waitFor();
        await page.waitForFunction(() => window.lottie?.getRegisteredAnimations().some(animation => !animation.isPaused && animation.currentFrame > 1));
        await page.waitForTimeout(mode === 'optimized' ? 3100 : 250);
        if (mode === 'optimized') assert.equal(nativeRequests.length, 0, 'A successful Premium mount must never start the heavy fallback later');
      }
      assert.deepEqual(errors, []);
      results.push({mode,native_animation_requests:nativeRequests.length,custom_animation_played:mode!=='failed',
        failed_or_stalled_native_frames_changed:mode==='failed'||mode==='stalled',javascript_errors:errors});
      await context.close();
    }
    const output = {scope:'Local Chromium; original full-frame animations unchanged; external requests blocked',
      avoided_bytes_for_one_successful_premium_purse:fs.statSync('webapp/emoji/purse.webp').size,results};
    fs.writeFileSync('verification/TRAFFIC-EMOJI-BROWSER.json',JSON.stringify(output,null,2));
    console.log(JSON.stringify(output,null,2));
  } finally {await browser.close();}
})().catch(error => {console.error(error);process.exitCode=1;}).finally(() => {server.kill();setTimeout(() => process.exit(process.exitCode||0),100);});
