'use client';

import { useRef, useCallback } from 'react';
import Editor, { OnMount, OnChange } from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import { Play, Loader2 } from 'lucide-react';
import { cn } from '@/lib/utils';

interface CodeEditorProps {
  value: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
  isLoading?: boolean;
  error?: string | null;
}

export function CodeEditor({
  value,
  onChange,
  onSubmit,
  isLoading = false,
  error = null,
}: CodeEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);

  const handleMount: OnMount = useCallback((editor) => {
    editorRef.current = editor;

    // Add keyboard shortcut for submit
    editor.addCommand(
      // Ctrl/Cmd + Enter
      2048 | 3, // KeyMod.CtrlCmd | KeyCode.Enter
      () => {
        onSubmit();
      }
    );
  }, [onSubmit]);

  const handleChange: OnChange = useCallback(
    (value) => {
      onChange(value || '');
    },
    [onChange]
  );

  return (
    <div className="flex flex-col h-full">
      {/* Editor header */}
      <div className="flex items-center justify-between px-4 h-12 bg-bg-medium border-b border-white/10">
        <div className="flex items-center gap-2">
          <span className="text-text-primary font-medium">PySpark Code</span>
          <span className="text-text-muted text-sm">Python</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-text-muted text-xs">
            {isLoading ? 'Processing...' : 'Ctrl+Enter to analyze'}
          </span>
          <button
            onClick={onSubmit}
            disabled={isLoading || !value.trim()}
            className={cn(
              'flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-all',
              'bg-spark-orange hover:bg-spark-orange-light text-white',
              'disabled:opacity-50 disabled:cursor-not-allowed',
              isLoading && 'animate-pulse-glow'
            )}
          >
            {isLoading ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                Analyzing
              </>
            ) : (
              <>
                <Play className="w-4 h-4" />
                Analyze
              </>
            )}
          </button>
        </div>
      </div>

      {/* Error message */}
      {error && (
        <div className="px-4 py-2 bg-severity-high/10 border-b border-severity-high/30 text-severity-high text-sm">
          {error}
        </div>
      )}

      {/* Monaco Editor */}
      <div className="flex-1 min-h-0">
        <Editor
          height="100%"
          language="python"
          value={value}
          onChange={handleChange}
          onMount={handleMount}
          theme="vs-dark"
          options={{
            minimap: { enabled: false },
            fontSize: 14,
            fontFamily: "'JetBrains Mono', monospace",
            lineNumbers: 'on',
            scrollBeyondLastLine: false,
            automaticLayout: true,
            tabSize: 4,
            wordWrap: 'on',
            padding: { top: 16, bottom: 16 },
            renderLineHighlight: 'line',
            cursorBlinking: 'smooth',
            smoothScrolling: true,
            bracketPairColorization: { enabled: true },
            guides: {
              bracketPairs: true,
              indentation: true,
            },
          }}
        />
      </div>
    </div>
  );
}
