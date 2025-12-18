import { useQuery } from '@tanstack/react-query';
import { apiClient } from '../../utils/api-client';

export interface IdTypeInfo {
  id: number;
  name: string;
  description?: string | null;
}

interface IdTypeResponse {
  items: IdTypeInfo[];
}

export const useMetadataIdTypes = () =>
  useQuery<IdTypeResponse, Error, IdTypeInfo[]>({
    queryKey: ['metadata-id-types'],
    queryFn: () => apiClient.get<IdTypeResponse>('/metadata/id-types'),
    select: (data) => data.items ?? [],
    staleTime: 5 * 60 * 1000,
    refetchOnWindowFocus: false,
  });
