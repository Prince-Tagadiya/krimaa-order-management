// Business settings live in Firestore. This map is only an in-memory view.
const CloudState = (() => {
    const values = new Map();
    return {
        async load() {
            const snapshot = await FirebaseService.getDb().collection('cloud_state').get();
            values.clear();
            snapshot.forEach(doc => values.set(doc.id, doc.data().value));
        },
        getItem(key) { return values.get(key) ?? null; },
        setItem(key, value) {
            values.set(key, String(value));
            FirebaseService.bufferWrite('state_' + key, () => FirebaseService.getDb()
                .collection('cloud_state').doc(key).set({value: String(value)}));
        },
        removeItem(key) {
            values.delete(key);
            FirebaseService.bufferWrite('state_' + key, () => FirebaseService.getDb()
                .collection('cloud_state').doc(key).delete());
        }
    };
})();
