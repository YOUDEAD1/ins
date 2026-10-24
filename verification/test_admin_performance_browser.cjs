/* Signed loopback API and real Chromium; all external requests are blocked. */
const {spawn} = require('child_process');
const fs = require('fs'), assert = require('assert/strict');
const {chromium} = require(process.env.LARA_TEST_PLAYWRIGHT || 'playwright');
const BASE = 'http://127.0.0.1:8798';
const server = spawn(process.env.LARA_TEST_PYTHON || 'python', ['verification/serve_user_inspection_test.py'], {
  env: {...process.env, LARA_TEST_PORT: '8798'}, stdio: ['ignore', 'pipe', 'pipe']
});
const ready = new Promise((resolve, reject) => {
  server.stdout.on('data', data => {if (String(data).includes('LOCAL USER INSPECTION SERVER')) resolve();});
  server.stderr.on('data', () => {});
  server.once('exit', code => reject(Error('Local fixture exited ' + code)));
});
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
(async () => {
  await ready;
  const browser = await chromium.launch({headless: true,
    ...(process.env.LARA_TEST_BROWSER_EXECUTABLE ? {executablePath: process.env.LARA_TEST_BROWSER_EXECUTABLE} : {}),
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-background-networking',
      '--disable-component-update', '--disable-sync', '--no-pings', '--disable-gpu']});
  try {
    const page = await browser.newPage({viewport: {width: 390, height: 844}, isMobile: true, hasTouch: true});
    page.setDefaultTimeout(10000);
    const errors = [], calls = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', request => {if (request.url().endsWith('/webapp/admin/home')) calls.push(request.postDataJSON());});
    await page.route('**/*', route => route.request().url().startsWith(BASE + '/') ? route.continue() : route.abort());
    let mode = 'held', release;
    const gate = new Promise(resolve => {release = resolve;});
    await page.route('**/webapp/admin/home', async route => {
      if (mode === 'error') return route.fulfill({status: 503, contentType: 'application/json', body: '{"error":"unavailable"}'});
      const response = await route.fetch();
      if (mode === 'held') await gate;
      return route.fulfill({response});
    });
    await page.goto(BASE + '/__user_test__/', {waitUntil: 'domcontentloaded'});
    await page.locator('[data-act="tab"][data-v="admin"]').waitFor();
    await page.waitForFunction(() => document.querySelector('#page-loading')?.hidden);
    const start = Date.now();
    await page.locator('[data-act="tab"][data-v="admin"]').click();
    const products = page.locator('#view [data-act="adm-open"][data-v="ad_prod_manage"]');
    await products.waitFor();
    const firstMenuMs = Date.now() - start;
    assert(firstMenuMs < 1200, 'Admin menu waited for statistics: ' + firstMenuMs);
    assert.equal(await page.locator('#view .ad-main').count(), 0, 'Pending metrics displayed invented zero values');
    assert.equal(await page.locator('#view .ad-led[aria-busy="true"]').count(), 1);
    assert(await page.locator('#page-loading').evaluate(element => element.hidden));
    // All original admin buttons, not just a placeholder, must already exist.
    assert(await page.locator('#view [data-act="adm-open"]').count() >= 20);
    const openStart = Date.now();
    await products.click();
    await page.locator('.sheet:not(.closing) [data-act="ap-btn"]').first().waitFor();
    const productPanelMs = Date.now() - openStart;
    assert.equal(await page.locator('#view .ad-main').count(), 0, 'Metrics response should still be held');
    const close = page.locator('.sheet:not(.closing) [data-act="sheet-close"]:visible').first();
    await close.click();
    assert.equal(calls.length, 1, 'Opening a section duplicated the dashboard request');
    release(); mode = 'normal';
    await page.locator('#view .ad-main').waitFor();
    const originalNumbers = await page.locator('#view .ad-main').innerText();
    assert.equal(await page.locator('#view .ad-led[aria-busy="true"]').count(), 0);
    assert.equal(calls.length, 1);
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
    mode = 'error';
    await page.locator('#view [data-act="adm-reload"]').click();
    await page.locator('#view .ad-ht [role="status"]').filter({hasText: 'could not be updated'}).waitFor();
    assert.deepEqual(calls[1], {refresh: true});
    assert.equal(await page.locator('#view .ad-main').innerText(), originalNumbers, 'Failed refresh erased real figures');
    assert(await products.isVisible());
    const beforeIdle = calls.length;
    await pause(300);
    assert.equal(calls.length, beforeIdle, 'A metrics failure created a retry loop');
    // A failed FIRST statistics fetch still leaves the buttons usable.
    await page.reload({waitUntil: 'domcontentloaded'});
    await page.locator('[data-act="tab"][data-v="admin"]').waitFor();
    await page.waitForFunction(() => document.querySelector('#page-loading')?.hidden);
    await page.locator('[data-act="tab"][data-v="admin"]').click();
    await page.locator('#view .ad-ht [role="status"]').filter({hasText: 'could not be updated'}).waitFor();
    assert(await products.isVisible());
    assert.equal(await page.locator('#view .ad-main').count(), 0);
    mode = 'normal';
    await page.locator('#view [data-act="adm-reload"]').click();
    await page.locator('#view .ad-main').waitFor();
    // Existing localized labels, routing and all dashboard sections still render.
    for (const language of ['ar', 'en', 'ru', 'zh']) {
      await page.locator('[data-act="tab"][data-v="profile"]').click();
      await page.locator('#view [data-act="lang"][data-v="' + language + '"]').click();
      await page.locator('[data-act="tab"][data-v="admin"]').click();
      assert(await products.isVisible());
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
    }
    await page.setViewportSize({width: 1360, height: 960});
    assert(await products.isVisible());
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
    assert.equal(calls.length, 4);
    assert.equal((await page.request.post(BASE + '/webapp/admin/home', {data: {}})).status(), 401);
    assert.deepEqual(errors, []);
    const result = {browser: 'Chromium', scope: 'signed local HTTP; in-memory DB; external requests blocked',
      first_menu_before_statistics: true, first_menu_ms: firstMenuMs,
      usable_product_panel_while_statistics_pending_ms: productPanelMs,
      initial_dashboard_requests: 1, refresh_failure_keeps_buttons_and_real_figures: true,
      first_load_failure_keeps_buttons_without_false_zero_figures: true,
      manual_refresh: true, failed_refresh_retry_loop: false,
      mobile_and_desktop_layout: 'passed', languages_checked: ['ar', 'en', 'ru', 'zh'],
      javascript_errors: errors};
    fs.writeFileSync('verification/ADMIN-PERFORMANCE-BROWSER.json', JSON.stringify(result, null, 2));
    console.log(JSON.stringify(result, null, 2));
  } finally {await browser.close();}
})().catch(error => {console.error(error); process.exitCode = 1;}).finally(() => {
  server.kill(); setTimeout(() => process.exit(process.exitCode || 0), 100);
});
