'use client';

import { useState, useCallback, useEffect } from 'react';
import { Sidebar } from '@/components/Sidebar';
import { CodeEditor } from '@/components/CodeEditor';
import { ResultsTabs } from '@/components/ResultsTabs';
import { useJobPolling } from '@/hooks/useJobPolling';
import { submitCode, APIError } from '@/lib/api';
import { CodeExample, JobResult, JobStatusResponse, transformBackendResult } from '@/lib/types';
import { CODE_EXAMPLES } from '@/lib/examples';

type UIJobStatus = 'idle' | 'pending' | 'processing' | 'completed' | 'failed';

export default function Home() {
  // State
  const [code, setCode] = useState(CODE_EXAMPLES[0].code);
  const [jobId, setJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<UIJobStatus>('idle');
  const [result, setResult] = useState<JobResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Polling hook
  const { data: pollingData, isPolling, isFinished, isFailed } = useJobPolling({
    jobId,
    onError: (err: Error) => {
      setError(err.message);
      setJobStatus('failed');
    },
  });

  // Handle polling data changes
  useEffect(() => {
    if (!pollingData) return;

    if (pollingData.status === 'finished' && pollingData.result) {
      const transformed = transformBackendResult(pollingData.result);
      if (transformed) {
        setResult(transformed);
        setJobStatus('completed');
        setError(null);
      }
    } else if (pollingData.status === 'failed') {
      const errorMsg = pollingData.result?.error?.message || 'Analysis failed';
      setError(errorMsg);
      setJobStatus('failed');
    } else if (pollingData.status === 'running' || pollingData.status === 'analysis_complete') {
      setJobStatus('processing');
    } else if (pollingData.status === 'pending') {
      setJobStatus('pending');
    }
  }, [pollingData]);

  // Submit code for analysis
  const handleSubmit = useCallback(async () => {
    if (!code.trim()) return;

    setIsSubmitting(true);
    setError(null);
    setResult(null);
    setJobStatus('pending');
    setJobId(null);

    try {
      const response = await submitCode(code);
      setJobId(response.job_id);

      // Check if already finished (cached)
      if (response.status === 'finished') {
        setJobStatus('completed');
      } else {
        setJobStatus('processing');
      }
    } catch (err) {
      if (err instanceof APIError) {
        setError(err.message);
      } else {
        setError('Failed to submit code');
      }
      setJobStatus('failed');
    } finally {
      setIsSubmitting(false);
    }
  }, [code]);

  // Handle example selection
  const handleSelectExample = useCallback((example: CodeExample) => {
    setCode(example.code);
    setError(null);
  }, []);

  const isLoading = isSubmitting || isPolling || jobStatus === 'processing' || jobStatus === 'pending';

  return (
    <div className="flex h-screen bg-bg-darkest">
      {/* Sidebar */}
      <Sidebar onSelectExample={handleSelectExample} />

      {/* Main content */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Top bar */}
        <header className="h-14 px-6 flex items-center bg-bg-dark border-b border-white/10 shrink-0">
          <span className="text-text-muted text-sm">Code Analysis</span>
        </header>

        {/* Content area */}
        <div className="flex-1 flex min-h-0">
          {/* Code editor panel */}
          <div className="w-1/2 border-r border-white/10 flex flex-col min-h-0">
            <CodeEditor
              value={code}
              onChange={setCode}
              onSubmit={handleSubmit}
              isLoading={isLoading}
              error={error}
            />
          </div>

          {/* Results panel */}
          <div className="w-1/2 flex flex-col min-h-0">
            <ResultsTabs
              result={result}
              isLoading={isLoading}
              loadingMessage={pollingData?.status === 'analysis_complete' ? 'Generating explanation...' : undefined}
              error={error || undefined}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
