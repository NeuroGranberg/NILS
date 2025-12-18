import { Navigate, useRoutes } from 'react-router-dom';
import { CohortsListPage } from '../../features/cohorts/pages/CohortsListPage';
import { CohortDetailPage } from '../../features/cohorts/pages/CohortDetailPage';
import { JobsPage } from '../../features/jobs/pages/JobsPage';
import { DatabaseManagementPage } from '../../features/database/pages/DatabaseManagementPage';
import { SettingsPage } from '../../features/settings/pages/SettingsPage';
import { QCPipelinePage } from '../../features/qc/pages/QCPipelinePage';
import { NotFoundPage } from '../../features/shared/pages/NotFoundPage';
import { AppLayout } from '../layout/AppLayout';

export const AppRoutes = () =>
  useRoutes([
    {
      element: <AppLayout />,
      children: [
        { index: true, element: <Navigate to="cohorts" replace /> },
        { path: 'cohorts', element: <CohortsListPage /> },
        { path: 'cohorts/:cohortId', element: <CohortDetailPage /> },
        { path: 'jobs', element: <JobsPage /> },
        { path: 'qc', element: <QCPipelinePage /> },
        { path: 'database', element: <DatabaseManagementPage /> },
        { path: 'settings', element: <SettingsPage /> },
        { path: '*', element: <NotFoundPage /> },
      ],
    },
  ]);
