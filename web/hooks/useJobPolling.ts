'use client';

import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getJobStatus } from '@/lib/api';
import { JobStatusResponse } from '@/lib/types';

interface UseJobPollingOptions {
  jobId: string | null;
  onSuccess?: (data: JobStatusResponse) => void;
  onError?: (error: Error) => void;
}

export function useJobPolling({ jobId, onSuccess, onError }: UseJobPollingOptions) {
  const queryClient = useQueryClient();

  const query = useQuery({
    queryKey: ['job', jobId],
    queryFn: () => getJobStatus(jobId!),
    enabled: !!jobId,
    refetchInterval: (query) => {
      const data = query.state.data;
      // Stop polling when job is finished or failed
      if (data?.status === 'finished' || data?.status === 'failed') {
        return false;
      }
      // Poll every 1 second while processing
      return 1000;
    },
    refetchIntervalInBackground: true,
    staleTime: 500,
    retry: 3,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 10000),
  });

  // Handle success callback
  if (query.data?.status === 'finished' && onSuccess) {
    onSuccess(query.data);
  }

  // Handle error callback
  if (query.error && onError) {
    onError(query.error);
  }

  const cancelJob = () => {
    queryClient.removeQueries({ queryKey: ['job', jobId] });
  };

  const isFinished = query.data?.status === 'finished';
  const isFailed = query.data?.status === 'failed';

  return {
    ...query,
    cancelJob,
    isPolling: query.isFetching && !isFinished && !isFailed,
    isFinished,
    isFailed,
  };
}
