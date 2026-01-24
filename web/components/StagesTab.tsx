'use client';

import { motion } from 'framer-motion';
import { Stage } from '@/lib/types';
import { cn } from '@/lib/utils';
import { Layers, Shuffle, ChevronRight } from 'lucide-react';

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
        {/* Timeline */}
        <div className="relative">
          {/* Timeline line */}
          <div className="absolute left-6 top-0 bottom-0 w-0.5 bg-gradient-to-b from-spark-orange via-spark-orange/50 to-transparent" />

          {stages.map((stage, index) => (
            <motion.div
              key={stage.stage_id}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: index * 0.1 }}
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
                        Shuffle
                      </span>
                    )}
                  </h3>
                  <span className="text-sm text-text-muted">
                    {stage.node_count} operation{stage.node_count !== 1 ? 's' : ''}
                  </span>
                </div>

                {/* Operations list */}
                <div className="space-y-2">
                  {stage.operations.map((op, opIndex) => (
                    <div
                      key={opIndex}
                      className="flex items-center gap-2 text-sm text-text-secondary"
                    >
                      <ChevronRight className="w-3 h-3 text-text-muted" />
                      <code className="font-mono text-spark-orange-light">{op}</code>
                    </div>
                  ))}
                </div>
              </div>

              {/* Connector arrow for shuffle stages */}
              {index < stages.length - 1 && stage.has_shuffle && (
                <div className="absolute left-6 -translate-x-1/2 bottom-2 text-text-muted">
                  <div className="text-[10px] px-2 py-0.5 bg-bg-dark rounded border border-white/10">
                    shuffle
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
