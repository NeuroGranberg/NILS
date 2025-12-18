import { BrowserRouter } from 'react-router-dom';
import { AppProviders } from './providers/AppProviders';
import { AppRoutes } from './routes/AppRoutes';

export const App = () => (
  <AppProviders>
    <BrowserRouter>
      <AppRoutes />
    </BrowserRouter>
  </AppProviders>
);
