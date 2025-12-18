import { Stack } from '@mantine/core';
import { useDatabaseSummaryQuery } from '../api';
import DatabaseSummaryCard from './DatabaseSummaryCard';
import MetadataTableExplorer from './MetadataTableExplorer';

export const MetadataDatabaseTab = () => {
  const summaryQuery = useDatabaseSummaryQuery();
  
  const metadataSummary = summaryQuery.data?.find((s) => s.database === 'metadata');

  return (
    <Stack gap="lg">
      <DatabaseSummaryCard 
        summary={metadataSummary} 
        isLoading={summaryQuery.isLoading} 
      />
      <MetadataTableExplorer />
    </Stack>
  );
};

export default MetadataDatabaseTab;
