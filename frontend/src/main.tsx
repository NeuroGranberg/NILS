import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import '@mantine/core/styles.css';
import '@mantine/notifications/styles.css';
import 'datatables.net-dt/css/dataTables.dataTables.css';
import 'datatables.net-fixedcolumns-dt/css/fixedColumns.dataTables.css';
import './app/styles.css';
import { App } from './app/App';

if (import.meta.env.DEV && import.meta.env.VITE_USE_REAL_FILES !== 'true') {
  const { worker } = await import('./mocks/browser');
  try {
    await worker.start({ onUnhandledRequest: 'bypass' });
  } catch (error) {
    console.warn('[MSW] Failed to start mock worker', error);
  }
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
