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
  ConnectionMode,
  Panel,
  Handle,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { motion } from 'framer-motion';
import { parseDot, calculateLayout, ParsedNode } from '@/lib/dotParser';
import { cn } from '@/lib/utils';
import { Layers, Zap, Database, GitBranch, Play, Info } from 'lucide-react';

// Node type colors and styles
const nodeConfig = {
  input: {
    bg: 'from-emerald-500/20 to-emerald-600/10',
    border: 'border-emerald-500/60',
    glow: 'shadow-[0_0_30px_rgba(16,185,129,0.4)]',
    icon: Database,
    iconColor: 'text-emerald-400',
    handleColor: '#10B981',
  },
  transform: {
    bg: 'from-blue-500/20 to-blue-600/10',
    border: 'border-blue-500/50',
    glow: 'shadow-[0_0_20px_rgba(59,130,246,0.3)]',
    icon: GitBranch,
    iconColor: 'text-blue-400',
    handleColor: '#3B82F6',
  },
  shuffle: {
    bg: 'from-orange-500/30 to-red-600/20',
    border: 'border-orange-500/70',
    glow: 'shadow-[0_0_40px_rgba(249,115,22,0.5)]',
    icon: Zap,
    iconColor: 'text-orange-400',
    handleColor: '#F97316',
  },
  action: {
    bg: 'from-amber-500/25 to-yellow-600/15',
    border: 'border-amber-500/60',
    glow: 'shadow-[0_0_35px_rgba(245,158,11,0.45)]',
    icon: Play,
    iconColor: 'text-amber-400',
    handleColor: '#F59E0B',
  },
};

// Custom DAG Node Component with Handles
const DAGNodeComponent = memo(({ data }: { data: ParsedNode & { index: number } }) => {
  const config = nodeConfig[data.nodeType] || nodeConfig.transform;
  const Icon = config.icon;

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.8 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ delay: data.index * 0.05, duration: 0.3 }}
      className={cn(
        'px-4 py-3 rounded-xl border-2 min-w-[140px]',
        'bg-gradient-to-br backdrop-blur-sm',
        'transition-all duration-300 cursor-pointer',
        'hover:scale-105 hover:z-10',
        config.bg,
        config.border,
        config.glow
      )}
    >
      {/* Input Handle (left side) - invisible, just for edge connection */}
      <Handle
        type="target"
        position={Position.Left}
        className="!w-2 !h-2 !border-0 !opacity-0"
      />

      <div className="flex items-center gap-2 mb-1">
        <div className={cn('p-1.5 rounded-lg bg-black/30', config.iconColor)}>
          <Icon className="w-3.5 h-3.5" />
        </div>
        <span className="font-semibold text-white text-sm truncate max-w-[100px]">
          {data.label}
        </span>
      </div>

      {data.stage !== undefined && (
        <div className="flex items-center gap-1.5 mt-2">
          <Layers className="w-3 h-3 text-gray-400" />
          <span className="text-[11px] text-gray-400 font-medium">
            Stage {data.stage}
          </span>
          {data.isShuffleNode && (
            <span className="ml-1 px-1.5 py-0.5 text-[9px] font-bold rounded bg-orange-500/30 text-orange-300 uppercase tracking-wider">
              Shuffle
            </span>
          )}
        </div>
      )}

      {/* Output Handle (right side) - invisible, just for edge connection */}
      <Handle
        type="source"
        position={Position.Right}
        className="!w-2 !h-2 !border-0 !opacity-0"
      />
    </motion.div>
  );
});

DAGNodeComponent.displayName = 'DAGNodeComponent';

const nodeTypes = {
  dagNode: DAGNodeComponent,
};

interface DAGViewerProps {
  dagDot?: string;
  className?: string;
}

export function DAGViewer({ dagDot, className }: DAGViewerProps) {
  // Parse DOT and create React Flow nodes/edges
  const { initialNodes, initialEdges, stageCount } = useMemo(() => {
    if (!dagDot) {
      return { initialNodes: [], initialEdges: [], stageCount: 0 };
    }

    const parsed = parseDot(dagDot);
    const positions = calculateLayout(parsed, 180, 90, 120, 60);

    // Calculate stage count
    const stages = new Set(parsed.nodes.map(n => n.stage).filter(s => s !== undefined));

    const nodes: Node[] = parsed.nodes.map((node, index) => {
      const pos = positions.get(node.id) || { x: index * 200, y: 100 };

      return {
        id: node.id,
        type: 'dagNode',
        position: pos,
        data: { ...node, index },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      };
    });

    const edges: Edge[] = parsed.edges.map((edge) => {
      // Determine edge style based on whether it crosses a stage boundary
      const sourceNode = parsed.nodes.find(n => n.id === edge.source);
      const targetNode = parsed.nodes.find(n => n.id === edge.target);
      const crossesStage = sourceNode?.stage !== targetNode?.stage &&
                           sourceNode?.stage !== undefined &&
                           targetNode?.stage !== undefined;

      return {
        id: `e-${edge.source}-${edge.target}`,
        source: edge.source,
        target: edge.target,
        type: 'smoothstep',
        animated: crossesStage,
        style: {
          stroke: crossesStage ? '#F97316' : '#6B7A94',
          strokeWidth: crossesStage ? 3 : 2,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: crossesStage ? '#F97316' : '#6B7A94',
          width: 12,
          height: 12,
        },
      };
    });

    return { initialNodes: nodes, initialEdges: edges, stageCount: stages.size };
  }, [dagDot]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Update nodes and edges when dagDot changes
  useEffect(() => {
    setNodes(initialNodes);
    setEdges(initialEdges);
  }, [initialNodes, initialEdges, setNodes, setEdges]);

  if (!dagDot || initialNodes.length === 0) {
    return (
      <div className={cn('flex flex-col items-center justify-center h-full text-center p-8', className)}>
        <div className="w-20 h-20 rounded-2xl bg-gradient-to-br from-orange-500/20 to-red-500/10 flex items-center justify-center mb-4 shadow-[0_0_40px_rgba(249,115,22,0.3)]">
          <GitBranch className="w-10 h-10 text-orange-400" />
        </div>
        <h3 className="text-lg font-semibold text-white mb-2">Operation DAG</h3>
        <p className="text-sm text-gray-400 max-w-md">
          Submit PySpark code to visualize the operation execution graph with stage boundaries and shuffle operations.
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
        connectionMode={ConnectionMode.Loose}
        fitView
        fitViewOptions={{ padding: 0.3, maxZoom: 1.5 }}
        minZoom={0.1}
        maxZoom={2}
        proOptions={{ hideAttribution: true }}
      >
        <Background
          color="#1E2633"
          gap={24}
          size={1}
        />
        <Controls
          showInteractive={false}
          className="!bg-bg-medium !border-white/10 !rounded-xl overflow-hidden !shadow-xl"
        />
        <MiniMap
          nodeColor={(node) => {
            const type = (node.data as ParsedNode)?.nodeType || 'transform';
            switch (type) {
              case 'input': return '#10B981';
              case 'shuffle': return '#F97316';
              case 'action': return '#F59E0B';
              default: return '#3B82F6';
            }
          }}
          maskColor="rgba(0, 0, 0, 0.8)"
          className="!bg-bg-dark !border-white/10 !rounded-xl overflow-hidden"
        />

        {/* Info + Legend Panel */}
        <Panel position="top-left" className="!m-4">
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-bg-medium/90 backdrop-blur-sm rounded-xl border border-white/10 p-3 shadow-xl max-w-xs"
          >
            <div className="flex items-start gap-2 mb-3">
              <Info className="w-4 h-4 text-spark-orange shrink-0 mt-0.5" />
              <p className="text-[11px] text-gray-400 leading-relaxed">
                <span className="text-white font-medium">Operation DAG</span> shows how Spark executes your code.
                <span className="text-orange-400"> Orange edges</span> cross stage boundaries (shuffles).
              </p>
            </div>
            <div className="text-[10px] font-semibold text-gray-500 mb-2 uppercase tracking-wider">
              {stageCount} Stage{stageCount !== 1 ? 's' : ''} • {nodes.length} Operations
            </div>
            <div className="flex flex-wrap gap-3">
              {[
                { type: 'input', label: 'Input', color: 'bg-emerald-500' },
                { type: 'transform', label: 'Transform', color: 'bg-blue-500' },
                { type: 'shuffle', label: 'Shuffle', color: 'bg-orange-500' },
                { type: 'action', label: 'Action', color: 'bg-amber-500' },
              ].map(item => (
                <div key={item.type} className="flex items-center gap-1.5">
                  <div className={cn('w-2.5 h-2.5 rounded-full', item.color)} />
                  <span className="text-[10px] text-gray-400">{item.label}</span>
                </div>
              ))}
            </div>
          </motion.div>
        </Panel>
      </ReactFlow>
    </div>
  );
}
