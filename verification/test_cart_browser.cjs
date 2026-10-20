/* Real Chromium + signed local HTTP + in-memory bot. Optional: npm install playwright. */
const {spawn}=require('child_process');
const fs=require('fs'),path=require('path'),assert=require('assert/strict');
const {chromium}=require(process.env.LARA_TEST_PLAYWRIGHT||'playwright');
const importedProvider=process.env.LARA_TEST_CHROMIUM_PROVIDER?require(process.env.LARA_TEST_CHROMIUM_PROVIDER):null;
const provider=importedProvider&&(importedProvider.default||importedProvider);
const BASE='http://127.0.0.1:8769',shots=process.env.LARA_TEST_SCREENSHOTS||'';
const server=spawn(process.env.LARA_TEST_PYTHON||'python',['verification/serve_cart_test.py'],{env:{...process.env,LARA_TEST_PORT:'8769'},stdio:['ignore','pipe','pipe']});
const ready=new Promise((resolve,reject)=>{server.stdout.on('data',d=>{if(d.toString().includes('LOCAL CART SERVER'))resolve();});server.stderr.on('data',()=>{});server.once('exit',code=>reject(Error('Local fixture exited '+code)));});
const snap=async(p,name)=>{if(shots){fs.mkdirSync(shots,{recursive:true});await p.screenshot({path:path.join(shots,name+'.png'),fullPage:false,animations:'disabled',timeout:45000});}};
(async()=>{
 await ready; console.log('UI fixture ready');
 const localArgs=['--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1','--disable-background-networking','--disable-component-update','--disable-sync','--no-pings','--no-sandbox','--disable-dev-shm-usage','--disable-gpu'];
 const browser=await chromium.launch({headless:true,args:localArgs,...(provider?{executablePath:process.env.LARA_TEST_BROWSER_EXECUTABLE||await provider.executablePath()}:{})});
 try{
 const context=await browser.newContext({viewport:{width:1360,height:960},colorScheme:'light',reducedMotion:'reduce'});
 const page=await context.newPage(),errors=[]; page.setDefaultTimeout(30000);page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/*',route=>route.request().url().startsWith(BASE)?route.continue():route.abort());
 await page.goto(BASE+'/__cart_test__/',{waitUntil:'domcontentloaded'});
 await page.locator('[data-act="product"][data-v="101"]').waitFor();
 const cart=()=>page.locator('.cart-sheet:not(.closing)');
 await page.locator('[data-act="cart-open"]').first().click();
 await cart().locator('.cart-empty').waitFor();await snap(page,'cart-empty-desktop'); console.log('UI empty cart rendered');
 await cart().locator('[data-act="cart-browse"]').click();
 console.log('UI returned to store');
 for(const pid of ['101','102']){
  await page.locator('[data-act="product"][data-v="'+pid+'"]').click();
  const product=page.locator('.psheet:not(.closing)').last();
  await product.locator('[data-act="cart-add"]').click();
  await product.locator('[data-act="sheet-close"]').click(); console.log('UI added product',pid);
 }
 await page.locator('[data-act="cart-open"]').first().click();
 await cart().locator('.cart-item').nth(1).waitFor();
 await cart().locator('[data-act="cart-plus"][data-v="0"]').click();
 await cart().locator('[data-act="cart-plus"][data-v="0"]').click();
 await cart().locator('[data-act="cart-plus"][data-v="1"]').click();
 assert((await cart().innerText()).includes('$18.00'));
 assert(await cart().locator('.cart-wallet').evaluate(el=>el.getBoundingClientRect().height<95));
 await snap(page,'cart-filled-desktop-light'); console.log('UI filled desktop rendered');
 await page.setViewportSize({width:390,height:844});await snap(page,'cart-filled-mobile-light');
 assert(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth));
 await page.evaluate(()=>document.documentElement.dataset.theme='dark');await snap(page,'cart-filled-mobile-dark');
 await cart().locator('[data-act="sheet-close"]').click();
 await page.locator('[data-act="lang"][data-v="ar"]').click();
 await page.locator('[data-act="cart-open"]').first().click();
 await page.waitForFunction(()=>document.documentElement.dir==='rtl');
 await snap(page,'cart-filled-mobile-arabic');
 await cart().locator('[data-act="cart-review"]').click();
 await cart().locator('[data-act="cart-pay"]').waitFor();
 assert((await cart().locator('.cart-grand').innerText()).includes('$18.00'));
 await snap(page,'cart-review-mobile-arabic');console.log('UI mobile RTL review rendered');
 // Complete the purchase server-side, then lose the response. UI must recover it.
 let checkoutRequests=0;
 await page.route('**/webapp/cart/checkout',async route=>{checkoutRequests++;await route.fetch();await route.abort('failed');});
 await cart().locator('[data-act="cart-pay"]').click();
 await cart().locator('.cart-receipt-hero.complete').waitFor({timeout:20000});
 assert.equal(await cart().locator('.code').count(),5);
 let state=await (await page.request.get(BASE+'/__cart_test__/state')).json();
 assert.deepEqual({balance:state.balance,orders:state.orders},{balance:82,orders:5});
 assert.equal(checkoutRequests,1);await snap(page,'cart-receipt-mobile-arabic');console.log('UI lost response recovered');
 await page.reload({waitUntil:'domcontentloaded'});
 await page.locator('[data-act="cart-open"]').first().click();
 await cart().locator('.cart-empty').waitFor();
 await cart().locator('[data-act="cart-last"]').click();
 await cart().locator('.cart-receipt-hero.complete').waitFor();assert.equal(await cart().locator('.code').count(),5);
 assert.equal(checkoutRequests,1);
 await cart().locator('[data-act="sheet-close"]').click();
 // A request lost before reaching the server can be safely cancelled by quote ID.
 await page.unroute('**/webapp/cart/checkout');
 await page.locator('[data-act="product"][data-v="101"]').click();
 await page.locator('.psheet:not(.closing)').last().locator('[data-act="cart-add"]').click();
 await page.locator('.psheet:not(.closing)').last().locator('[data-act="sheet-close"]').click();
 await page.locator('[data-act="cart-open"]').first().click();await cart().locator('[data-act="cart-review"]').click();
 await cart().locator('[data-act="cart-pay"]').waitFor();
 await page.route('**/webapp/cart/checkout',route=>route.abort('failed'));
 await cart().locator('[data-act="cart-pay"]').click();
 await cart().locator('.cart-banner').waitFor();await cart().locator('[data-act="cart-edit"]').click();
 await cart().locator('[data-act="cart-review"]').waitFor();
 state=await (await page.request.get(BASE+'/__cart_test__/state')).json();assert.equal(state.balance,82);assert.equal(state.orders,5);
 // Product invitation details are editable inside the cart.
 await cart().locator('[data-act="cart-browse"]').click();
 await page.locator('[data-act="product"][data-v="103"]').click();
 const product=page.locator('.psheet:not(.closing)').last();
 await product.locator('#pemail').fill('first@example.test');await product.locator('[data-act="cart-add"]').click();await product.locator('[data-act="sheet-close"]').click();
 await page.locator('[data-act="cart-open"]').first().click();await cart().locator('[data-in="cart-email"]').fill('updated@example.test');
 await cart().locator('[data-act="cart-review"]').click();await cart().locator('[data-act="cart-pay"]').waitFor();
 assert((await cart().innerText()).includes('updated@example.test'));
 assert.deepEqual(errors,[]);console.log('PASS real Chromium: desktop/mobile light/dark/RTL; quantities and bulk discount; lost-response recovery; reload receipt; safe cancellation; editable invitation email; zero JS exceptions');
 await context.close();
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{server.kill();setTimeout(()=>process.exit(process.exitCode||0),200);});
