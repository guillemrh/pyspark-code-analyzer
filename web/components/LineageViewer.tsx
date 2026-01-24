'use client';

import { useMemo, useEffect, memo } from 'react';
import ReactFlow, {
  Node,
  Edge,
  Controls,
  Background,
  MiniMap,
  useNodesState,
  useEdgesState,
  MarkerType,
  Position,
  Panel,
  Handle,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { motion } from 'framer-motion';
import { parseLineageDot, calculateLayout, ParsedLineageNode } from '@/lib/dotParser';
import { cn } from '@/lib/utils';
import { Database, Table2, ArrowRight } from 'lucide-react';

// Custom Lineage Node Component with Handles
const LineageNodeComponent = memo(({ data }: { data: ParsedLineageNode & { index: number } }) => {
  const isSource = data.type === 'source';

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.8, y: 20 }}
      animate={{ opacity: 1, scale: 1, y: 0 }}
      transition={{ delay: data.index * 0.08, duration: 0.4, type: 'spring' }}
      className={cn(
        'px-5 py-4 rounded-2xl border-2 min-w-[130px]',
        'bg-gradient-to-br backdrop-blur-sm',
        'transition-all duration-300 cursor-pointer',
        'hover:scale-110 hover:z-10',
        isSource
          ? 'from-emerald-500/25 to-teal-600/15 border-emerald-500/60 shadow-[0_0_35px_rgba(16,185,129,0.4)]'
          : 'from-violet-500/25 to-purple-600/15 border-violet-500/60 shadow-[0_0_30px_rgba(139,92,246,0.35)]'
      )}
    >
      {/* Input Handle (left side) */}
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !border-2 !border-gray-700"
        style={{ background: isSource ? '#10B981' : '#8B5CF6' }}
      />

      <div className="flex items-center gap-3">
        <div
          className={cn(
            'p-2 rounded-xl',
            isSource ? 'bg-emerald-500/30' : 'bg-violet-500/30'
          )}
        >
          {isSource ? (
            <Database className="w-5 h-5 text-emerald-400" />
          ) : (
            <Table2 className="w-5 h-5 text-violet-400" />
          )}
        </div>
        <div>
          <span className="font-bold text-white text-sm block">{data.label}</span>
          <span
            className={cn(
              'text-[10px] font-medium uppercase tracking-wider',
              isSource ? 'text-emerald-400' : 'text-violet-400'
            )}
          >
            {isSource ? 'Source' : 'Derived'}
          </span>
        </div>
      </div>

      {/* Output Handle (right side) */}
      <Handle
        type="source"
        position={Position.Right}
        className="!w-3 !h-3 !border-2 !border-gray-700"
        style={{ background: isSource ? '#10B981' : '#8B5CF6' }}
      />
    </motion.div>
  );
});

LineageNodeComponent.displayName = 'LineageNodeComponent';

const nodeTypes = {
  lineageNode: LineageNodeComponent,
};

interface LineageViewerProps {
  lineageDot?: string;
  className?: string;
}

export function LineageViewer({ lineageDot, className }: LineageViewerProps) {
  // Parse lineage DOT and create React Flow nodes/edges
  const { initialNodes, initialEdges, sourceCount, derivedCount } = useMemo(() => {
    if (!lineageDot) {
      return { initialNodes: [], initialEdges: [], sourceCount: 0, derivedCount: 0 };
    }

    const parsed = parseLineageDot(lineageDot);

    if (parsed.nodes.length === 0) {
      return { initialNodes: [], initialEdges: [], sourceCount: 0, derivedCount: 0 };
    }

    const positions = calculateLayout(parsed, 180, 100, 140, 50);

    // Count sources and derived
    const sourceNodes = parsed.nodes.filter((n) => n.type === 'source');
    const derivedNodes = parsed.nodes.filter((n) => n.type === 'derived');

    // Create React Flow nodes
    const nodes: Node[] = parsed.nodes.map((node, index) => {
      const pos = positions.get(node.id) || { x: index * 200, y: 150 };

      return {
        id: node.id,
        type: 'lineageNode',
        position: pos,
        data: { ...node, index },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      };
    });

    // Create React Flow edges with gradient
    const edges: Edge[] = parsed.edges.map((edge) => ({
      id: `e-${edge.source}-${edge.target}`,
      source: edge.source,
      target: edge.target,
      type: 'smoothstep',
      animated: true,
      style: {
        stroke: '#8B5CF6',
        strokeWidth: 3,
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: '#8B5CF6',
        width: 16,
        height: 16,
      },
    }));

    return {
      initialNodes: nodes,
      initialEdges: edges,
      sourceCount: sourceNodes.length,
      derivedCount: derivedNodes.length,
    };
  }, [lineageDot]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  useEffect(() => {
    setNodes(initialNodes);
    setEdges(initialEdges);
  }, [initialNodes, initialEdges, setNodes, setEdges]);

  if (!lineageDot || initialNodes.length === 0) {
    return (
      <div className={cn('flex flex-col items-center justify-center h-full text-center p-8', className)}>
        <div className="w-20 h-20 rounded-2xl bg-gradient-to-br from-violet-500/20 to-purple-500/10 flex items-center justify-center mb-4 shadow-[0_0_40px_rgba(139,92,246,0.3)]">
          <ArrowRight className="w-10 h-10 text-violet-400" />
        </div>
        <h3 className="text-lg font-semibold text-white mb-2">Data Lineage</h3>
        <p className="text-sm text-gray-400 max-w-md">
          Submit PySpark code to visualize how data flows between DataFrames.
        </p>
      </div>
    );
  }

  return (
    <div className={cn('h-full w-full', className)}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.4, maxZoom: 1.5 }}
        minZoom={0.1}
        maxZoom={2}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#1E2633" gap={24} size={1} />
        <Controls
          showInteractive={false}
          className="!bg-bg-medium !border-white/10 !rounded-xl overflow-hidden !shadow-xl"
        />
        <MiniMap
          nodeColor={(node) => {
            const type = (node.data as ParsedLineageNode)?.type;
            return type === 'source' ? '#10B981' : '#8B5CF6';
          }}
          maskColor="rgba(0, 0, 0, 0.8)"
          className="!bg-bg-dark !border-white/10 !rounded-xl overflow-hidden"
        />

        {/* Legend Panel */}
        <Panel position="top-left" className="!m-4">
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-bg-medium/90 backdrop-blur-sm rounded-xl border border-white/10 p-3 shadow-xl"
          >
            <div className="text-xs font-semibold text-gray-400 mb-2 uppercase tracking-wider">
              {sourceCount} Source{sourceCount !== 1 ? 's' : ''} → {derivedCount} Derived
            </div>
            <div className="flex gap-4">
              <div className="flex items-center gap-1.5">
                <div className="w-3 h-3 rounded-full bg-emerald-500" />
                <span className="text-[10px] text-gray-400">Source DataFrame</span>
              </div>
              <div className="flex items-center gap-1.5">
                <div className="w-3 h-3 rounded-full bg-violet-500" />
                <span className="text-[10px] text-gray-400">Derived DataFrame</span>
              </div>
            </div>
          </motion.div>
        </Panel>
      </ReactFlow>
    </div>
  );
}
