// CAMBIA ESTE NOMBRE/VERSIÓN PARA FORZAR LA ACTUALIZACIÓN EN LAS PCs DE LOS USUARIOS
const CACHE_NAME = 'mercado-limpio-prov-v1'; 

// Archivos básicos que la app necesita para cargar
const urlsToCache = [
  './',
  './index.html',
  './manifest.json'
];

// Instalación: Descarga y guarda los archivos en caché
self.addEventListener('install', event => {
  self.skipWaiting(); // Fuerza al Service Worker a instalarse de inmediato
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => {
        return cache.addAll(urlsToCache);
      })
  );
});

// Activación: Limpia las versiones viejas de la app (por ejemplo, cuando pasas de v1 a v2)
self.addEventListener('activate', event => {
  event.waitUntil(self.clients.claim()); // Toma el control de la pantalla inmediatamente
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.map(cacheName => {
          if (cacheName !== CACHE_NAME) {
            console.log('Borrando caché antigua:', cacheName);
            return caches.delete(cacheName);
          }
        })
      );
    })
  );
});

// Interceptar peticiones (Fetch)
self.addEventListener('fetch', event => {
  // Ignoramos las llamadas a la API para que siempre busquen datos reales del servidor
  if (event.request.url.includes('/api/')) {
    return;
  }

  // Estrategia Network First (Red primero, si falla va al Caché)
  // Esto asegura que si el usuario tiene internet, siempre vea lo último, 
  // y si se corta, abra la versión guardada.
  event.respondWith(
    fetch(event.request).catch(() => caches.match(event.request))
  );
});
