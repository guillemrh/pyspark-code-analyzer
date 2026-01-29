'use client';

import ReactMarkdown from 'react-markdown';
import { AnalysisMetrics } from '@/lib/types';
import { cn } from '@/lib/utils';

interface MetricItemProps {
  label: string;
  value: number | string;
  colorClass: string;
}

function MetricItem({ label, value, colorClass }: MetricItemProps) {
  return (
    <div className="flex-1 flex flex-col items-center gap-1 py-3.5 px-2">
      <p className={cn('text-2xl font-bold tabular-nums font-mono', colorClass)}>{value}</p>
      <p className="text-[10px] uppercase tracking-widest text-text-muted font-medium">{label}</p>
    </div>
  );
}

interface ExplanationTabProps {
  explanation: string;
  metrics: AnalysisMetrics | null;
}

export function ExplanationTab({ explanation, metrics }: ExplanationTabProps) {
  return (
    <div className="h-full overflow-y-auto p-6">
      {/* Metrics Bar */}
      {metrics && (
        <div className="flex items-center rounded-xl bg-bg-light/50 border border-white/5 mb-6 divide-x divide-white/5">
          <MetricItem label="Operations" value={metrics.total_operations} colorClass="text-node-transform" />
          <MetricItem label="Transforms" value={metrics.transformations} colorClass="text-node-input" />
          <MetricItem label="Actions" value={metrics.actions} colorClass="text-node-action" />
          <MetricItem label="Stages" value={metrics.stages} colorClass="text-node-shuffle" />
          <MetricItem label="Issues" value={metrics.anti_patterns_found} colorClass="text-severity-warning" />
        </div>
      )}

      {/* Explanation Content */}
      <div className="prose prose-invert max-w-none">
        <div className="markdown-content">
          <ReactMarkdown>{explanation || 'No explanation available.'}</ReactMarkdown>
        </div>
      </div>
    </div>
  );
}
