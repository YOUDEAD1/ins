const fs = require('fs');
const assert = require('assert/strict');
const {JSDOM, VirtualConsole} = require('./ui-deps/node_modules/jsdom');
const BASE='http://127.0.0.1:8766';
const {spawn}=require('child_process');
const server=spawn('python',['verification/serve_test.py'],{env:{...process.env,LARA_TEST_PORT:'8766'},stdio:['ignore','pipe','pipe']});
const ready=new Promise((resolve,reject)=>{server.stdout.on('data',d=>{if(d.toString().includes('LOCAL TEST SERVER'))resolve()});server.once('exit',code=>reject(new Error('Test server exited '+code)));server.stderr.on('data',()=>{});});
(async()=>{
 await ready;
 const html=await (await fetch(BASE+'/__integration_test__/')).text();
 const errors=[],calls=[];
 const vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e.message));vc.on('error',(...args)=>errors.push(args.join(' ')));
 const dom=new JSDOM(html,{url:BASE+'/app/',runScripts:'dangerously',pretendToBeVisual:true,virtualConsole:vc,beforeParse(w){
  w.fetch=(url,opts)=>{calls.push(String(url));return fetch(new URL(url,BASE),opts)};
  w.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});
  w.ResizeObserver=class{observe(){}unobserve(){}disconnect(){}};
  w.IntersectionObserver=class{observe(){}unobserve(){}disconnect(){}};
  w.scrollTo=()=>{};w.HTMLElement.prototype.scrollIntoView=()=>{};
  w.HTMLCanvasElement.prototype.getContext=()=>null;
  w.AbortController=AbortController;
 }});
 const w=dom.window;
 try{
  for(let i=0;i<80;i++){if(w.document.querySelector('#results [data-act="product"]'))break;await new Promise(r=>setTimeout(r,50));}
  const doc=w.document;
  const wait=async fn=>{for(let i=0;i<100;i++){if(fn())return;await new Promise(r=>setTimeout(r,30));}throw new Error('UI wait timed out: '+doc.querySelector('#view').textContent.slice(0,180));};
  const click=selector=>{const el=doc.querySelector(selector);assert(el,'Missing control '+selector);el.click();};
  assert(doc.querySelector('#results').textContent.includes('Integration Test Product'));
  assert(calls.some(p=>p.endsWith('/webapp/bootstrap')));
  click('[data-act="product"]');
  await wait(()=>doc.querySelector('[data-act="buy"]'));
  click('[data-act="buy"]');
  await wait(()=>doc.querySelector('[data-act="buy"].confirm'));
  click('[data-act="buy"]');
  await wait(()=>calls.some(p=>p.endsWith('/webapp/buy')) && doc.body.textContent.includes('LOCAL-ONLY-'));
  const receipt=doc.body.textContent;
  assert(receipt.includes('LOCAL-ONLY-0'));
  console.log('PASS frontend product -> confirmation -> real backend purchase -> delivered code');
  // Close the receipt before navigating between tabs.
  const close=doc.querySelector('.sheet.top [data-act="sheet-close"]')||doc.querySelector('[data-act="sheet-close"]')||doc.querySelector('[data-act="done"]');
  if(close)close.click();
  // «طلباتي» صارت داخل «حسابي»: نفتح حسابي أول ثم بطاقة طلباتي
  for(const v of ['profile','orders','balance','api','store']){
    click('[data-act="tab"][data-v="'+v+'"]');
    await new Promise(r=>setTimeout(r,80));
    assert(!doc.querySelector('#view').textContent.includes('undefined'));
    console.log('PASS frontend tab '+v);
  }
  for(const lang of ['ar','ru','zh','en']){
    click('[data-act="lang"][data-v="'+lang+'"]');
    await new Promise(r=>setTimeout(r,50));
    assert.equal(doc.documentElement.lang,lang==='zh'?'zh-CN':lang);
    assert.equal(doc.documentElement.dir,lang==='ar'?'rtl':'ltr');
    assert(doc.querySelector('#results [data-act="product"]'));
    console.log('PASS frontend language '+lang);
  }
  assert.deepEqual(errors,[]);
  console.log('UI integration tests passed; JavaScript errors: 0; requests:',calls.map(x=>new URL(x).pathname));
 }finally{w.close()}
})().catch(e=>{console.error(e);process.exitCode=1}).finally(()=>server.kill());
