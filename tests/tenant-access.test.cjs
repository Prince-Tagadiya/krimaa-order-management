const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const handler = require('../netlify/functions/env.js').handler;

test('Purva runtime config never publishes passwords or legacy default users', async () => {
    const saved = { ...process.env };
    try {
        Object.assign(process.env, { APP_TENANT_ID: 'purva', FIREBASE_PROJECT_ID: 'purva-2150e', AUTH_USERS_JSON: '[{"username":"secret","password":"do-not-publish"}]' });
        const result = await handler();
        assert.equal(result.statusCode, 200);
        assert.match(result.body, /window.AUTH_USERS = \[\]/);
        assert.doesNotMatch(result.body, /do-not-publish|Krimaa4484|order123/);
        process.env.FIREBASE_PROJECT_ID = 'another-project';
        assert.equal((await handler()).statusCode, 503);
    } finally { process.env = saved; }
});

function fixture(claims = { tenant: 'purva', role: 'admin' }) {
    const values = new Map([['isLogged', 'true'], ['userRole', 'admin']]);
    const user = { email: 'admin@example.com', getIdTokenResult: async () => ({ claims }) };
    let signedOut = false;
    const auth = {
        onAuthStateChanged(fn) { queueMicrotask(() => fn(null)); return () => {}; },
        signInWithEmailAndPassword: async () => ({ user }),
        signOut: async () => { signedOut = true; }
    };
    const context = vm.createContext({ window: { APP_TENANT_ID: 'purva', FIREBASE_CONFIG: { projectId: 'purva-2150e' } }, location: { hostname: 'purvafashion.netlify.app' }, localStorage: { getItem: k => values.get(k), setItem: (k,v) => values.set(k,v), removeItem: k => values.delete(k) }, FirebaseService: { getDb() {} }, firebase: { auth: () => auth } });
    vm.runInContext(fs.readFileSync(require.resolve('../tenant-access.js'), 'utf8') + ';globalThis.access=TenantAccess;', context);
    return { access: context.access, values, context, signedOut: () => signedOut };
}

test('Purva clears legacy sessions and establishes a verified Firebase session', async () => {
    const f = fixture();
    await f.access.ready();
    assert.equal(f.values.has('isLogged'), false);
    await f.access.login('admin@example.com', 'test');
    assert.equal(f.values.get('userUsername'), 'admin@example.com');
    await f.access.logout();
    assert.equal(f.values.has('isLogged'), false);
    assert.equal(f.signedOut(), true);
});

test('Purva rejects other tenants and incorrect Firebase projects', async () => {
    const f = fixture({ tenant: 'krimaa', role: 'admin' });
    await f.access.ready();
    await assert.rejects(f.access.login('admin@example.com', 'test'), /does not have access/);
    assert.equal(f.values.has('isLogged'), false);
    f.context.window.FIREBASE_CONFIG.projectId = 'krimaa-f7fc3';
    await assert.rejects(f.access.ready(), /configuration unavailable/);
});
