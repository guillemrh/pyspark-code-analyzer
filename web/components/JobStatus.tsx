'use client';

import { motion } from 'framer-motion';
import { Loader2, CheckCircle2, XCircle, Clock } from 'lucide-react';
import { cn } from '@/lib/utils';

interface JobStatusProps {
  status: 'idle' | 'pending' | 'processing' | 'completed' | 'failed';
  progress?: number;
  message?: string;
  error?: string;
  jobId?: string;
}

const statusConfig = {
  idle: {
    icon: Clock,
    label: 'Ready',
    color: 'text-text-muted',
    bgColor: 'bg-bg-light',
  },
  pending: {
    icon: Loader2,
    label: 'Queued',
    color: 'text-spark-orange',
    bgColor: 'bg-spark-orange/10',
  },
  processing: {
    icon: Loader2,
    label: 'Processing',
    color: 'text-spark-orange',
    bgColor: 'bg-spark-orange/10',
  },
  completed: {
    icon: CheckCircle2,
    label: 'Completed',
    color: 'text-node-input',
    bgColor: 'bg-node-input/10',
  },
  failed: {
    icon: XCircle,
    label: 'Failed',
    color: 'text-severity-high',
    bgColor: 'bg-severity-high/10',
  },
};

export function JobStatus({
  status,
  progress,
  message,
  error,
  jobId,
}: JobStatusProps) {
  const config = statusConfig[status];
  const Icon = config.icon;
  const isAnimating = status === 'pending' || status === 'processing';

  if (status === 'idle') {
    return null;
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: -10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -10 }}
      className={cn('flex items-center gap-3 px-4 py-3 rounded-lg', config.bgColor)}
    >
      <Icon
        className={cn('w-5 h-5', config.color, isAnimating && 'animate-spin')}
      />

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className={cn('font-medium', config.color)}>{config.label}</span>
          {jobId && (
            <span className="text-xs text-text-muted font-mono">
              {jobId.slice(0, 8)}...
            </span>
          )}
        </div>

        {message && (
          <p className="text-sm text-text-secondary mt-0.5 truncate">{message}</p>
        )}

        {error && (
          <p className="text-sm text-severity-high mt-0.5">{error}</p>
        )}

        {/* Progress bar */}
        {isAnimating && (
          <div className="mt-2 h-1.5 bg-bg-dark rounded-full overflow-hidden">
            <motion.div
              className="h-full bg-spark-orange rounded-full"
              initial={{ width: '0%' }}
              animate={{
                width: progress ? `${progress}%` : '100%',
                x: progress ? 0 : ['-100%', '100%'],
              }}
              transition={
                progress
                  ? { duration: 0.3 }
                  : {
                      repeat: Infinity,
                      duration: 1.5,
                      ease: 'linear',
                    }
              }
            />
          </div>
        )}
      </div>
    </motion.div>
  );
}
