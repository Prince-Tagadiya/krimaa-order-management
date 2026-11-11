const {test}=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');

function setup() {
 const documents=new Map();
 let writes=0;
 const ref=id=>({id,get:async()=>({exists:documents.has(id),data:()=>documents.get(id)})});
 const db={collection:name=>({doc:id=>ref(name+'/'+id)}),runTransaction:async fn=>fn({get:r=>r.get(),set:(r,value)=>{documents.set(r.id,structuredClone(value));writes++;}})};
 const firestore=()=>db;firestore.FieldValue={serverTimestamp:()=> 'server-time'};
 const window={firebase:{apps:[{}],firestore},FIREBASE_CONFIG:{projectId:'test'},APP_CONFIG:{writeBufferMs:1200}};
 const context=vm.createContext({window,firebase:window.firebase,setTimeout,clearTimeout,console,Date,Map,APP_CONFIG:window.APP_CONFIG});
 vm.runInContext(fs.readFileSync(require('node:path').join(__dirname,'../firebase-service.js'),'utf8')+'\nthis.service=FirebaseService;',context);
 return {service:context.service,documents,writes:()=>writes};
}
test('saving either channel preserves the other across subsequent reads',async()=>{
 const {service:s,documents,writes}=setup();
 await s.submitOrders('2026-11-11',[{accountId:'a',meesho:12,flipkart:7}],'company1');
 await s.updateOrder('2026-11-11','a','meesho',0,'company1');
 let rows=(await s.getOrderRowsForDate('company1','2026-11-11')).data;
 assert.equal(rows[0].meesho,0);assert.equal(rows[0].flipkart,7);assert.equal(rows[0].total,7);
 await s.updateOrder('2026-11-11','a','flipkart',9,'company1');
 rows=(await s.getOrderRowsForDate('company1','2026-11-11')).data;
 assert.equal(rows[0].meesho,0);assert.equal(rows[0].flipkart,9);assert.equal(rows[0].total,9);
 assert.equal(writes(),3);assert.equal(documents.size,1,'one daily document, no duplicate partitions');
});
test('company/day keys isolate the same account ID',async()=>{
 const {service:s}=setup();
 await s.updateOrder('2026-11-11','a','meesho',4,'company1');
 await s.updateOrder('2026-11-11','a','meesho',9,'company2');
 assert.equal((await s.getOrderRowsForDate('company1','2026-11-11')).data[0].meesho,4);
});
test('buffer serializes overlapping flushes and applies newest edit last',async()=>{
 const {service:s}=setup();const applied=[];let release;
 s.bufferWrite('cell',async()=>{await new Promise(resolve=>release=resolve);applied.push(1);});
 const first=s.flushWrites();await Promise.resolve();
 assert.equal(s.getPendingCount(),1);
 s.bufferWrite('cell',async()=>applied.push(2));
 const second=s.flushWrites();release();await Promise.all([first,second]);
 assert.deepEqual(applied,[1,2]);assert.equal(s.getPendingCount(),0);
});
test('a failed save stays pending without replaying successful writes',async()=>{
 const {service:s}=setup();let count=0;let fail=true;
 s.bufferWrite('a',async()=>count++);
 s.bufferWrite('b',async()=>{if(fail)throw new Error('offline');count++;});
 await assert.rejects(s.flushWrites(),/offline/);
 assert.equal(s.getPendingCount(),1);fail=false;await s.flushWrites();assert.equal(count,2);
});
test('invalid channel or quantity fails before writing',async()=>{
 const {service:s,writes}=setup();
 await assert.rejects(s.updateOrder('2026-11-11','a','quantity',5,'company1'),/channel/);
 await assert.rejects(s.updateOrder('2026-11-11','a','meesho',-1,'company1'),/quantity/);
 assert.equal(writes(),0);
});
