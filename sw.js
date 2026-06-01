// ===== KRIMAA SERVICE WORKER — OFFLINE-FIRST =====
// Cache all app shell + CDN assets. Serve from cache first, network as fallback.
// Increment CACHE_VER whenever you deploy new code.
const CACHE_VER = 'krimaa-v8';

const SHELL_FILES = [
    '/',
    '/index.html',
    '/style.css',
    '/app.js',
    '/firebase-service.js',
    '/lan-storage-service.js',
    '/runtime-env-loader.js',
    '/env.js',
];

// External CDN files that must also be cached for full offline use
const CDN_FILES = [
    'https://www.gstatic.com/firebasejs/10.14.1/firebase-app-compat.js',
    'https://www.gstatic.com/firebasejs/10.14.1/firebase-firestore-compat.js',
    'https://www.gstatic.com/firebasejs/10.14.1/firebase-database-compat.js',
    'https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js',
    'https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js',
    'https://cdnjs.cloudflare.com/ajax/libs/jspdf-autotable/3.5.28/jspdf.plugin.autotable.min.js',
    'https://cdn.jsdelivr.net/npm/sortablejs@1.15.6/Sortable.min.js',
    'https://unpkg.com/boxicons@2.1.4/css/boxicons.min.css',
    'https://unpkg.com/boxicons@2.1.4/fonts/boxicons.woff2',
    'https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&display=swap',
];

const ALL_CACHE_FILES = [...SHELL_FILES, ...CDN_FILES];

// ──── INSTALL: pre-cache everything ────
self.addEventListener('install', (event) => {
    event.waitUntil(
        caches.open(CACHE_VER).then(async (cache) => {
            // Cache shell files (must succeed)
            await cache.addAll(SHELL_FILES);
            // Cache CDN files (best-effort — don't fail if CDN is down)
            for (const url of CDN_FILES) {
                try {
                    await cache.add(new Request(url, { mode: 'cors' }));
                } catch (e) {
                    console.warn('[SW] Could not cache CDN file:', url, e.message);
                }
            }
        }).catch((e) => {
            console.error('[SW] Install cache failed:', e);
        })
    );
    self.skipWaiting();
});

// ──── ACTIVATE: delete old caches ────
self.addEventListener('activate', (event) => {
    event.waitUntil(
        caches.keys().then((keys) =>
            Promise.all(
                keys.filter((k) => k !== CACHE_VER).map((k) => {
                    console.log('[SW] Deleting old cache:', k);
                    return caches.delete(k);
                })
            )
        )
    );
    self.clients.claim();
});

// ──── FETCH: Cache-first strategy ────
self.addEventListener('fetch', (event) => {
    const url = event.request.url;

    // Never intercept non-GET requests or Firebase API writes — let those go through
    if (event.request.method !== 'GET') return;

    // Never cache Firebase Firestore/Auth data API calls (dynamic data)
    if (
        url.includes('firestore.googleapis.com') ||
        url.includes('identitytoolkit.googleapis.com') ||
        url.includes('securetoken.googleapis.com') ||
        url.includes('/api/') ||
        url.includes('script.google.com') ||  // Google Apps Script (Sheets API)
        url.includes('googleapis.com/auth')
    ) {
        return; // Let browser handle it directly
    }

    event.respondWith(
        caches.match(event.request).then((cached) => {
            if (cached) {
                return cached; // Serve from cache immediately
            }
            // Not in cache — try network, then cache the result
            return fetch(event.request).then((response) => {
                if (!response || response.status !== 200) return response;
                const toCache = response.clone();
                caches.open(CACHE_VER).then((cache) => {
                    try {
                        cache.put(event.request, toCache);
                    } catch (e) {}
                });
                return response;
            }).catch(() => {
                // Offline and not in cache — return the app shell for navigation requests
                if (event.request.mode === 'navigate') {
                    return caches.match('/index.html');
                }
                return new Response('', { status: 503, statusText: 'Offline' });
            });
        })
    );
});
