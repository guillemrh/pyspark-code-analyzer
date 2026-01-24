'use client';

import { useState } from 'react';
import * as Tabs from '@radix-ui/react-tabs';
import { motion } from 'framer-motion';
import { cn } from '@/lib/utils';
import { JobResult, TabId } from '@/lib/types';
import { ExplanationTab } from './ExplanationTab';
import { StagesTab } from './StagesTab';
import { AntiPatternsTab } from './AntiPatternsTab';
import { DAGViewer } from './DAGViewer';
import { LineageViewer } from './LineageViewer';
import {
  FileText,
  Layers,
  AlertTriangle,
  GitBranch,
  Network,
} from 'lucide-react';

interface TabConfig {
  id: TabId;
  label: string;
  icon: React.ReactNode;
  badge?: number;
}

interface ResultsTabsProps {
  result: JobResult | null;
  className?: string;
}

export function ResultsTabs({ result, className }: ResultsTabsProps) {
  const [activeTab, setActiveTab] = useState<TabId>('explanation');

  const tabs: TabConfig[] = [
    {
      id: 'explanation',
      label: 'Explanation',
      icon: <FileText className="w-4 h-4" />,
    },
    {
      id: 'dag',
      label: 'DAG',
      icon: <Network className="w-4 h-4" />,
    },
    {
      id: 'lineage',
      label: 'Lineage',
      icon: <GitBranch className="w-4 h-4" />,
    },
    {
      id: 'stages',
      label: 'Stages',
      icon: <Layers className="w-4 h-4" />,
      badge: result?.stages?.length,
    },
    {
      id: 'antipatterns',
      label: 'Issues',
      icon: <AlertTriangle className="w-4 h-4" />,
      badge: result?.anti_patterns?.length,
    },
  ];

  if (!result) {
    return (
      <div className={cn('flex items-center justify-center h-full', className)}>
        <div className="text-center p-8">
          <div className="w-16 h-16 rounded-full bg-bg-light flex items-center justify-center mx-auto mb-4">
            <FileText className="w-8 h-8 text-text-muted" />
          </div>
          <h3 className="text-lg font-medium text-text-primary mb-2">
            No Results Yet
          </h3>
          <p className="text-sm text-text-muted max-w-md">
            Submit your PySpark code to see a detailed analysis with
            explanations, DAG visualization, and performance recommendations.
          </p>
        </div>
      </div>
    );
  }

  return (
    <Tabs.Root
      value={activeTab}
      onValueChange={(v) => setActiveTab(v as TabId)}
      className={cn('flex flex-col h-full', className)}
    >
      {/* Tab list */}
      <Tabs.List className="flex border-b border-white/10 px-4 bg-bg-medium shrink-0">
        {tabs.map((tab) => (
          <Tabs.Trigger
            key={tab.id}
            value={tab.id}
            className={cn(
              'relative flex items-center gap-2 px-4 py-3 text-sm font-medium',
              'text-text-muted hover:text-text-secondary transition-colors',
              'data-[state=active]:text-text-primary',
              'focus:outline-none focus-visible:ring-2 focus-visible:ring-spark-orange/50'
            )}
          >
            {tab.icon}
            <span>{tab.label}</span>
            {tab.badge !== undefined && tab.badge > 0 && (
              <span
                className={cn(
                  'px-1.5 py-0.5 text-xs rounded-full',
                  tab.id === 'antipatterns'
                    ? 'bg-severity-warning/20 text-severity-warning'
                    : 'bg-bg-light text-text-muted'
                )}
              >
                {tab.badge}
              </span>
            )}

            {/* Active indicator */}
            {activeTab === tab.id && (
              <motion.div
                layoutId="tab-indicator"
                className="absolute bottom-0 left-0 right-0 h-0.5 bg-spark-orange"
                transition={{ type: 'spring', bounce: 0.2, duration: 0.4 }}
              />
            )}
          </Tabs.Trigger>
        ))}
      </Tabs.List>

      {/* Tab panels */}
      <div className="flex-1 min-h-0 bg-bg-dark">
        <Tabs.Content value="explanation" className="h-full data-[state=inactive]:hidden">
          <ExplanationTab
            explanation={result.explanation}
            metrics={result.metrics}
          />
        </Tabs.Content>

        <Tabs.Content value="dag" className="h-full data-[state=inactive]:hidden">
          <DAGViewer dagDot={result.dag_dot} />
        </Tabs.Content>

        <Tabs.Content value="lineage" className="h-full data-[state=inactive]:hidden">
          <LineageViewer lineageDot={result.lineage_dot} />
        </Tabs.Content>

        <Tabs.Content value="stages" className="h-full data-[state=inactive]:hidden">
          <StagesTab stages={result.stages} />
        </Tabs.Content>

        <Tabs.Content value="antipatterns" className="h-full data-[state=inactive]:hidden">
          <AntiPatternsTab patterns={result.anti_patterns} />
        </Tabs.Content>
      </div>
    </Tabs.Root>
  );
}
