import {mountSchemaBrowser} from './upstream/index.js';
const status = document.getElementById('status');
const browser = mountSchemaBrowser(document.getElementById('schema-browser'), {
  assetsUrl: new URL('./upstream/assets/', import.meta.url).href,
  catalogUrl: new URL('./schema-catalog.json', import.meta.url).href,
  assetBaseUrl: new URL('./', import.meta.url).href,
  categories: [{id: 'domain', label: 'Ashlar schemas'}, {id: 'local', label: 'Local files'}],
  initialRoute: location.hash.slice(1), hideHero: true, height: '850px',
  onNavigate(selection) { const route = new URLSearchParams(); for (const [key,value] of Object.entries(selection)) if (value != null) route.set(key,value); history.replaceState(null,'','#'+route); },
  onError(error) { status.textContent = error.message; },
});
browser.ready.then(() => {status.textContent = '';}, error => {status.textContent = error.message;});
window.addEventListener('pagehide', event => {if (!event.persisted) browser.destroy();});
window.addEventListener('hashchange', () => { browser.select(Object.fromEntries(new URLSearchParams(location.hash.slice(1)))).catch(error => {status.textContent = error.message;}); });
