'use client';

import { motion } from 'framer-motion';
import { AntiPattern } from '@/lib/types';
import { cn, getSeverityColor } from '@/lib/utils';
import {
  AlertTriangle,
  AlertCircle,
  Info,
  Lightbulb,
  ChevronDown,
  ExternalLink,
} from 'lucide-react';
import { useState } from 'react';

const severityIcons = {
  HIGH: AlertTriangle,
  MEDIUM: AlertCircle,
  WARNING: AlertCircle,
  INFO: Info,
};

const severityLabels = {
  HIGH: 'High Impact',
  MEDIUM: 'Medium Impact',
  WARNING: 'Warning',
  INFO: 'Suggestion',
};

interface AntiPatternCardProps {
  pattern: AntiPattern;
  index: number;
}

function AntiPatternCard({ pattern, index }: AntiPatternCardProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const Icon = severityIcons[pattern.severity] || AlertCircle;
  const severityColorClass = getSeverityColor(pattern.severity);

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.05 }}
      className={cn(
        'rounded-lg border overflow-hidden',
        'bg-bg-medium/50',
        severityColorClass.includes('border') ? severityColorClass : 'border-white/10'
      )}
    >
      {/* Header */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-start gap-3 p-4 text-left hover:bg-white/5 transition-colors"
      >
        <div
          className={cn(
            'p-2 rounded-lg shrink-0',
            pattern.severity === 'HIGH' && 'bg-severity-high/20',
            pattern.severity === 'MEDIUM' && 'bg-severity-medium/20',
            pattern.severity === 'WARNING' && 'bg-severity-warning/20',
            pattern.severity === 'INFO' && 'bg-severity-info/20'
          )}
        >
          <Icon
            className={cn(
              'w-4 h-4',
              pattern.severity === 'HIGH' && 'text-severity-high',
              pattern.severity === 'MEDIUM' && 'text-severity-medium',
              pattern.severity === 'WARNING' && 'text-severity-warning',
              pattern.severity === 'INFO' && 'text-severity-info'
            )}
          />
        </div>

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <h3 className="font-medium text-text-primary">{pattern.rule_name}</h3>
            <span
              className={cn(
                'px-2 py-0.5 text-xs rounded-full border',
                severityColorClass
              )}
            >
              {severityLabels[pattern.severity]}
            </span>
          </div>
          <p className="text-sm text-text-secondary line-clamp-2">{pattern.message}</p>
        </div>

        <ChevronDown
          className={cn(
            'w-4 h-4 text-text-muted shrink-0 transition-transform',
            isExpanded && 'rotate-180'
          )}
        />
      </button>

      {/* Expanded content */}
      {isExpanded && (
        <motion.div
          initial={{ height: 0, opacity: 0 }}
          animate={{ height: 'auto', opacity: 1 }}
          exit={{ height: 0, opacity: 0 }}
          className="border-t border-white/10"
        >
          <div className="p-4 space-y-4">
            {/* Suggestion */}
            <div className="flex gap-3">
              <Lightbulb className="w-4 h-4 text-node-action shrink-0 mt-0.5" />
              <div>
                <p className="text-xs text-text-muted mb-1">Suggestion</p>
                <p className="text-sm text-text-secondary">{pattern.suggestion}</p>
              </div>
            </div>

            {/* Affected nodes */}
            {pattern.affected_nodes.length > 0 && (
              <div>
                <p className="text-xs text-text-muted mb-2">Affected Operations</p>
                <div className="flex flex-wrap gap-2">
                  {pattern.affected_nodes.map((node, i) => (
                    <span
                      key={i}
                      className="px-2 py-1 text-xs font-mono bg-bg-dark rounded border border-white/10"
                    >
                      {node}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Line number */}
            {pattern.line_number && (
              <div className="flex items-center gap-2 text-xs text-text-muted">
                <ExternalLink className="w-3 h-3" />
                <span>Line {pattern.line_number}</span>
              </div>
            )}
          </div>
        </motion.div>
      )}
    </motion.div>
  );
}

interface AntiPatternsTabProps {
  patterns: AntiPattern[];
}

export function AntiPatternsTab({ patterns }: AntiPatternsTabProps) {
  if (!patterns || patterns.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-center p-8">
        <div className="w-16 h-16 rounded-full bg-node-input/20 flex items-center justify-center mb-4">
          <AlertCircle className="w-8 h-8 text-node-input" />
        </div>
        <h3 className="text-lg font-medium text-text-primary mb-2">No Issues Found</h3>
        <p className="text-sm text-text-muted max-w-md">
          Great! Your PySpark code follows best practices. No anti-patterns or performance issues were detected.
        </p>
      </div>
    );
  }

  // Group patterns by severity
  const groupedPatterns = {
    HIGH: patterns.filter((p) => p.severity === 'HIGH'),
    MEDIUM: patterns.filter((p) => p.severity === 'MEDIUM'),
    WARNING: patterns.filter((p) => p.severity === 'WARNING'),
    INFO: patterns.filter((p) => p.severity === 'INFO'),
  };

  return (
    <div className="h-full overflow-y-auto p-6">
      {/* Summary */}
      <div className="flex gap-4 mb-6 flex-wrap">
        {Object.entries(groupedPatterns).map(([severity, items]) => {
          if (items.length === 0) return null;
          return (
            <div
              key={severity}
              className={cn(
                'px-3 py-2 rounded-lg border',
                getSeverityColor(severity)
              )}
            >
              <span className="font-medium">{items.length}</span>
              <span className="ml-1 text-sm opacity-80">{severityLabels[severity as keyof typeof severityLabels]}</span>
            </div>
          );
        })}
      </div>

      {/* Pattern cards */}
      <div className="space-y-3">
        {patterns.map((pattern, index) => (
          <AntiPatternCard key={index} pattern={pattern} index={index} />
        ))}
      </div>
    </div>
  );
}
