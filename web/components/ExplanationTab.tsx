'use client';

import ReactMarkdown from 'react-markdown';
import { AnalysisMetrics } from '@/lib/types';
import { cn } from '@/lib/utils';
import { BarChart3, GitBranch, Zap, AlertTriangle, Layers } from 'lucide-react';

interface MetricCardProps {
  icon: React.ReactNode;
  label: string;
  value: number | string;
  color: string;
}

function MetricCard({ icon, label, value, color }: MetricCardProps) {
  return (
    <div className={cn('flex items-center gap-3 p-3 rounded-lg bg-bg-light/50 border border-white/5')}>
      <div className={cn('p-2 rounded-lg', color)}>{icon}</div>
      <div>
        <p className="text-xs text-text-muted">{label}</p>
        <p className="text-lg font-semibold text-text-primary">{value}</p>
      </div>
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
      {/* Metrics Grid */}
      {metrics && (
        <div className="grid grid-cols-2 lg:grid-cols-5 gap-3 mb-6">
          <MetricCard
            icon={<BarChart3 className="w-4 h-4 text-node-transform" />}
            label="Operations"
            value={metrics.total_operations}
            color="bg-node-transform/20"
          />
          <MetricCard
            icon={<GitBranch className="w-4 h-4 text-node-input" />}
            label="Transformations"
            value={metrics.transformations}
            color="bg-node-input/20"
          />
          <MetricCard
            icon={<Zap className="w-4 h-4 text-node-action" />}
            label="Actions"
            value={metrics.actions}
            color="bg-node-action/20"
          />
          <MetricCard
            icon={<Layers className="w-4 h-4 text-node-shuffle" />}
            label="Stages"
            value={metrics.stages}
            color="bg-node-shuffle/20"
          />
          <MetricCard
            icon={<AlertTriangle className="w-4 h-4 text-severity-warning" />}
            label="Issues"
            value={metrics.anti_patterns_found}
            color="bg-severity-warning/20"
          />
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
