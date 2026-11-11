// Update old installations and remove obsolete offline/LAN copies. Business data is online only.
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', event => {
    event.waitUntil((async () => {
        const keys = await caches.keys();
        await Promise.all(keys.filter(key => key.startsWith('krimaa-')).map(key => caches.delete(key)));
        await self.clients.claim();
    })());
});
