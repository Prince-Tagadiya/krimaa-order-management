// Separate deployments share application code, never Firebase projects or credentials.
const TenantAccess = (() => {
    const isPurva = () => window.APP_TENANT_ID === 'purva' || location.hostname === 'purvafashion.netlify.app';
    const companyName = () => isPurva() ? 'PURVA FASHION' : 'SHREE SAI';
    let auth = null;
    function clearSession() {
        ['isLogged', 'userRole', 'userName', 'userUsername', 'userAllowedCompanies'].forEach(key => localStorage.removeItem(key));
    }
    async function setSession(user) {
        const result = await user.getIdTokenResult();
        if (result.claims.tenant !== 'purva' || result.claims.role !== 'admin') {
            await auth.signOut();
            throw new Error('This login does not have access to Purva Fashion.');
        }
        const profile = { role: 'admin', displayName: user.displayName || 'Purva Fashion Admin', username: user.email, allowedCompanies: ['company1'] };
        localStorage.setItem('isLogged', 'true');
        localStorage.setItem('userRole', profile.role);
        localStorage.setItem('userName', profile.displayName);
        localStorage.setItem('userUsername', profile.username);
        localStorage.setItem('userAllowedCompanies', JSON.stringify(profile.allowedCompanies));
        return profile;
    }
    async function ready() {
        if (!isPurva()) return;
        clearSession(); // Old browser-side logins cannot authorize Firebase access.
        if (window.APP_TENANT_ID !== 'purva' || window.FIREBASE_CONFIG?.projectId !== 'purva-2150e') {
            throw new Error('Purva Fashion configuration unavailable. Please reload while online.');
        }
        FirebaseService.getDb();
        auth = firebase.auth();
        const user = await new Promise((resolve, reject) => {
            const unsubscribe = auth.onAuthStateChanged(user => { unsubscribe(); resolve(user); }, reject);
        });
        if (user) await setSession(user);
    }
    async function login(email, password) {
        if (!auth) throw new Error('Secure sign-in is unavailable. Please reload while online.');
        const result = await auth.signInWithEmailAndPassword(email.trim(), password);
        return setSession(result.user);
    }
    async function logout() {
        if (isPurva() && auth) await auth.signOut();
        clearSession();
    }
    function applyBrand() {
        if (!isPurva()) return;
        document.title = 'Purva Fashion';
        document.body.classList.add('tenant-purva');
        document.documentElement.style.setProperty('--primary', '#0f766e');
        document.documentElement.style.setProperty('--primary-hover', '#115e59');
        const logo = document.querySelector('.sidebar-header span');
        if (logo) logo.textContent = 'Purva Fashion';
        const username = document.getElementById('username');
        if (username) { username.type = 'email'; username.placeholder = 'Enter your email'; username.autocomplete = 'username'; }
        const label = document.getElementById('username')?.previousElementSibling;
        if (label) label.textContent = 'Email';
        const password = document.getElementById('password');
        if (password) password.autocomplete = 'current-password';
        const manifest = document.querySelector('link[rel="manifest"]');
        if (manifest) manifest.href = '/purva-manifest.json';
    }
    return { isPurva, companyName, ready, login, logout, applyBrand };
})();
