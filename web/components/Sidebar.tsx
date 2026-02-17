'use client';

import { useState } from 'react';
import { Code2, History, ChevronRight, Zap, PanelLeftClose, PanelLeft } from 'lucide-react';
import { cn } from '@/lib/utils';
import { CODE_EXAMPLES } from '@/lib/examples';
import { CodeExample } from '@/lib/types';

interface SidebarProps {
  onSelectExample: (example: CodeExample) => void;
  recentJobs?: { id: string; timestamp: Date; preview: string }[];
  isCollapsed?: boolean;
  onToggleCollapse?: () => void;
}

export function Sidebar({ onSelectExample, recentJobs = [], isCollapsed = false, onToggleCollapse }: SidebarProps) {
  const [expandedSection, setExpandedSection] = useState<'examples' | 'history' | null>('examples');

  if (isCollapsed) {
    return (
      <aside className="w-12 h-full bg-bg-dark border-r border-white/10 flex flex-col items-center py-4">
        <button
          onClick={onToggleCollapse}
          className="p-2 rounded-lg hover:bg-bg-medium/50 transition-colors text-text-muted hover:text-text-primary"
          title="Expand sidebar"
        >
          <PanelLeft className="w-5 h-5" />
        </button>
      </aside>
    );
  }

  return (
    <aside className="w-72 h-full bg-bg-dark border-r border-white/10 flex flex-col">
      {/* Logo */}
      <div className="px-5 h-[60px] flex items-center border-b border-white/10">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Zap className="w-5 h-5 text-spark-orange" />
            <span className="text-text-primary font-semibold tracking-tight">
              Spark<span className="text-spark-orange">Lens</span>
            </span>
          </div>
          {onToggleCollapse && (
            <button
              onClick={onToggleCollapse}
              className="p-1.5 rounded-lg hover:bg-bg-medium/50 transition-colors text-text-muted hover:text-text-primary"
              title="Collapse sidebar"
            >
              <PanelLeftClose className="w-4 h-4" />
            </button>
          )}
        </div>
      </div>

      {/* Examples Section */}
      <div className="flex-1 overflow-y-auto">
        <div className="border-b border-white/5">
          <button
            onClick={() => setExpandedSection(expandedSection === 'examples' ? null : 'examples')}
            className="w-full flex items-center justify-between p-4 text-left hover:bg-bg-medium/50 transition-colors"
          >
            <div className="flex items-center gap-2">
              <Code2 className="w-4 h-4 text-spark-orange" />
              <span className="text-text-primary font-medium">Examples</span>
            </div>
            <ChevronRight
              className={cn(
                'w-4 h-4 text-text-muted transition-transform',
                expandedSection === 'examples' && 'rotate-90'
              )}
            />
          </button>

          {expandedSection === 'examples' && (
            <div className="px-2 pb-2 animate-fade-in">
              {CODE_EXAMPLES.map((example) => (
                <button
                  key={example.name}
                  onClick={() => onSelectExample(example)}
                  className={cn(
                    'w-full text-left p-3 rounded-lg mb-1 transition-all',
                    'hover:bg-bg-light/50 group'
                  )}
                >
                  <p className="text-sm text-text-primary font-medium group-hover:text-spark-orange-light transition-colors">
                    {example.name}
                  </p>
                  <p className="text-xs text-text-muted mt-1 line-clamp-2">
                    {example.description}
                  </p>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* History Section */}
        <div>
          <button
            onClick={() => setExpandedSection(expandedSection === 'history' ? null : 'history')}
            className="w-full flex items-center justify-between p-4 text-left hover:bg-bg-medium/50 transition-colors"
          >
            <div className="flex items-center gap-2">
              <History className="w-4 h-4 text-spark-orange" />
              <span className="text-text-primary font-medium">Recent</span>
            </div>
            <ChevronRight
              className={cn(
                'w-4 h-4 text-text-muted transition-transform',
                expandedSection === 'history' && 'rotate-90'
              )}
            />
          </button>

          {expandedSection === 'history' && (
            <div className="px-2 pb-2 animate-fade-in">
              {recentJobs.length === 0 ? (
                <p className="text-text-muted text-xs p-3">No recent analyses</p>
              ) : (
                recentJobs.map((job) => (
                  <div
                    key={job.id}
                    className="p-3 rounded-lg hover:bg-bg-light/50 cursor-pointer transition-all"
                  >
                    <p className="text-xs text-text-muted">
                      {job.timestamp.toLocaleTimeString()}
                    </p>
                    <p className="text-sm text-text-secondary mt-1 line-clamp-2 font-mono">
                      {job.preview}
                    </p>
                  </div>
                ))
              )}
            </div>
          )}
        </div>
      </div>

      {/* Footer */}
      <div className="p-4 border-t border-white/10">
        <p className="text-xs text-text-muted text-center">
          <a href="https://temujinlabs.com" target="_blank" rel="noopener noreferrer" className="text-orange-400 hover:text-orange-300 transition-colors">Temujin Labs</a>
          {" | Built by "}
          <a href="https://github.com/guillemrh" target="_blank" rel="noopener noreferrer" className="text-orange-400 hover:text-orange-300 transition-colors">Guillem Rovira</a>
        </p>
      </div>
    </aside>
  );
}
