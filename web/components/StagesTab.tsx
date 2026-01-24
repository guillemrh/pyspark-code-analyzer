'use client';

import { motion } from 'framer-motion';
import { Stage } from '@/lib/types';
import { cn } from '@/lib/utils';
import { Layers, Shuffle, ChevronRight, Info, Zap, Database, Filter, ArrowLeftRight } from 'lucide-react';

// Operation descriptions and icons
const OPERATION_INFO: Record<string, { description: string; category: 'input' | 'transform' | 'shuffle' | 'action' }> = {
  // Input operations
  read_parquet: { description: 'Load data from Parquet files', category: 'input' },
  read_csv: { description: 'Load data from CSV files', category: 'input' },
  read_json: { description: 'Load data from JSON files', category: 'input' },
  read_jdbc: { description: 'Load data from JDBC source', category: 'input' },
  read_table: { description: 'Load data from a table', category: 'input' },
  spark_read: { description: 'Load data from source', category: 'input' },

  // Narrow transformations (no shuffle)
  filter: { description: 'Filter rows based on condition', category: 'transform' },
  select: { description: 'Select specific columns', category: 'transform' },
  withColumn: { description: 'Add or replace a column', category: 'transform' },
  drop: { description: 'Remove columns', category: 'transform' },
  alias: { description: 'Rename DataFrame', category: 'transform' },
  cache: { description: 'Cache DataFrame in memory', category: 'transform' },
  persist: { description: 'Persist DataFrame to storage', category: 'transform' },
  limit: { description: 'Limit number of rows', category: 'transform' },
  sample: { description: 'Sample rows from DataFrame', category: 'transform' },

  // Wide transformations (shuffle)
  join: { description: 'Join with another DataFrame (triggers shuffle)', category: 'shuffle' },
  groupBy: { description: 'Group rows for aggregation (triggers shuffle)', category: 'shuffle' },
  orderBy: { description: 'Sort all rows (triggers shuffle)', category: 'shuffle' },
  sortBy: { description: 'Sort all rows (triggers shuffle)', category: 'shuffle' },
  distinct: { description: 'Remove duplicate rows (triggers shuffle)', category: 'shuffle' },
  repartition: { description: 'Redistribute data (triggers shuffle)', category: 'shuffle' },
  coalesce: { description: 'Reduce partitions', category: 'shuffle' },

  // Actions
  show: { description: 'Display rows to console', category: 'action' },
  collect: { description: 'Collect all data to driver', category: 'action' },
  count: { description: 'Count number of rows', category: 'action' },
  write: { description: 'Write data to storage', category: 'action' },
  save: { description: 'Save data to storage', category: 'action' },
  take: { description: 'Take first N rows', category: 'action' },
  first: { description: 'Get first row', category: 'action' },
};

const CATEGORY_STYLES = {
  input: { bg: 'bg-emerald-500/10', text: 'text-emerald-400', border: 'border-emerald-500/20' },
  transform: { bg: 'bg-blue-500/10', text: 'text-blue-400', border: 'border-blue-500/20' },
  shuffle: { bg: 'bg-orange-500/10', text: 'text-orange-400', border: 'border-orange-500/20' },
  action: { bg: 'bg-amber-500/10', text: 'text-amber-400', border: 'border-amber-500/20' },
};

function getOperationInfo(op: string) {
  // Try exact match first
  if (OPERATION_INFO[op]) return OPERATION_INFO[op];

  // Try lowercase
  const lower = op.toLowerCase();
  if (OPERATION_INFO[lower]) return OPERATION_INFO[lower];

  // Try to match partial (e.g., "read_parquet" from "spark_read_parquet")
  for (const [key, value] of Object.entries(OPERATION_INFO)) {
    if (lower.includes(key.toLowerCase())) return value;
  }

  return { description: 'Transformation operation', category: 'transform' as const };
}

interface StagesTabProps {
  stages: Stage[];
}

export function StagesTab({ stages }: StagesTabProps) {
  if (!stages || stages.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-text-muted">
        <p>No stage information available</p>
      </div>
    );
  }

  return (
    <div className="h-full overflow-y-auto p-6">
      <div className="max-w-3xl mx-auto">
        {/* Explanation header */}
        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          className="mb-6 p-4 rounded-lg bg-bg-medium/50 border border-white/10"
        >
          <div className="flex items-start gap-3">
            <div className="p-2 rounded-lg bg-spark-orange/10">
              <Info className="w-4 h-4 text-spark-orange" />
            </div>
            <div>
              <h4 className="font-medium text-text-primary mb-1">What are Stages?</h4>
              <p className="text-sm text-text-muted">
                Spark divides work into stages separated by <span className="text-orange-400 font-medium">shuffle boundaries</span>.
                Within a stage, operations run in parallel on partitions. Shuffles redistribute data across the cluster,
                which is expensive. Fewer stages = better performance.
              </p>
            </div>
          </div>
        </motion.div>

        {/* Legend */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.1 }}
          className="mb-6 flex flex-wrap gap-4 text-xs"
        >
          {[
            { label: 'Input', category: 'input' },
            { label: 'Transform', category: 'transform' },
            { label: 'Shuffle', category: 'shuffle' },
            { label: 'Action', category: 'action' },
          ].map((item) => (
            <div key={item.category} className="flex items-center gap-1.5">
              <div className={cn('w-2.5 h-2.5 rounded-full', CATEGORY_STYLES[item.category as keyof typeof CATEGORY_STYLES].text.replace('text-', 'bg-'))} />
              <span className="text-text-muted">{item.label}</span>
            </div>
          ))}
        </motion.div>

        {/* Timeline */}
        <div className="relative">
          {/* Timeline line */}
          <div className="absolute left-6 top-0 bottom-0 w-0.5 bg-gradient-to-b from-spark-orange via-spark-orange/50 to-transparent" />

          {stages.map((stage, index) => (
            <motion.div
              key={stage.stage_id}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: index * 0.1 + 0.2 }}
              className="relative pl-16 pb-8"
            >
              {/* Stage indicator */}
              <div
                className={cn(
                  'absolute left-3 w-7 h-7 rounded-full flex items-center justify-center',
                  'bg-bg-medium border-2',
                  stage.has_shuffle
                    ? 'border-spark-orange shadow-glow-orange'
                    : 'border-node-transform'
                )}
              >
                {stage.has_shuffle ? (
                  <Shuffle className="w-3.5 h-3.5 text-spark-orange" />
                ) : (
                  <Layers className="w-3.5 h-3.5 text-node-transform" />
                )}
              </div>

              {/* Stage card */}
              <div
                className={cn(
                  'p-4 rounded-lg border',
                  'bg-bg-medium/50 border-white/10',
                  'hover:border-white/20 transition-colors'
                )}
              >
                <div className="flex items-center justify-between mb-3">
                  <h3 className="font-semibold text-text-primary flex items-center gap-2">
                    <span>Stage {stage.stage_id}</span>
                    {stage.has_shuffle && (
                      <span className="px-2 py-0.5 text-xs rounded-full bg-spark-orange/20 text-spark-orange-light border border-spark-orange/30">
                        Shuffle Boundary
                      </span>
                    )}
                  </h3>
                  <span className="text-sm text-text-muted">
                    {stage.node_count} operation{stage.node_count !== 1 ? 's' : ''}
                  </span>
                </div>

                {/* Operations list with descriptions */}
                <div className="space-y-2">
                  {stage.operations.map((op, opIndex) => {
                    const info = getOperationInfo(op);
                    const style = CATEGORY_STYLES[info.category];
                    return (
                      <div
                        key={opIndex}
                        className={cn(
                          'flex items-center gap-3 p-2 rounded-md border',
                          style.bg,
                          style.border
                        )}
                      >
                        <code className={cn('font-mono text-sm font-medium', style.text)}>{op}</code>
                        <span className="text-xs text-text-muted">{info.description}</span>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Connector arrow for shuffle stages */}
              {index < stages.length - 1 && stage.has_shuffle && (
                <div className="absolute left-6 -translate-x-1/2 bottom-2 text-text-muted">
                  <div className="text-[10px] px-2 py-0.5 bg-bg-dark rounded border border-white/10 flex items-center gap-1">
                    <ArrowLeftRight className="w-3 h-3" />
                    <span>data shuffle</span>
                  </div>
                </div>
              )}
            </motion.div>
          ))}
        </div>
      </div>
    </div>
  );
}
