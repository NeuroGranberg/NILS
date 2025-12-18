import { Stack } from '@mantine/core';
import { useDatabaseSummaryQuery } from '../api';
import DatabaseSummaryCard from './DatabaseSummaryCard';
import ApplicationTableExplorer from './ApplicationTableExplorer';

export const ApplicationDatabaseTab = () => {
  const summaryQuery = useDatabaseSummaryQuery();
  
  const applicationSummary = summaryQuery.data?.find((s) => s.database === 'application');

  return (
    <Stack gap="lg">
      <DatabaseSummaryCard 
        summary={applicationSummary} 
        isLoading={summaryQuery.isLoading} 
      />
      <ApplicationTableExplorer />
    </Stack>
  );
};

export default ApplicationDatabaseTab;
