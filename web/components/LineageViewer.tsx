'use client';

import { useMemo, useEffect } from 'react';
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
} from 'reactflow';
import 'reactflow/dist/style.css';
import { motion } from 'framer-motion';
import { LineageGraph, LineageNode as LineageNodeType } from '@/lib/types';
import { cn } from '@/lib/utils';
import { Database, Table2, ArrowRight } from 'lucide-react';

// Custom Lineage Node Component
function LineageNodeComponent({ data }: { data: LineageNodeType & { index: number } }) {
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
    </motion.div>
  );
}

const nodeTypes = {
  lineageNode: LineageNodeComponent,
};

interface LineageViewerProps {
  lineage: LineageGraph | null;
  lineageDot?: string;
  className?: string;
}

export function LineageViewer({ lineage, lineageDot, className }: LineageViewerProps) {
  // Parse lineage graph and create React Flow nodes/edges
  const { initialNodes, initialEdges, sourceCount, derivedCount } = useMemo(() => {
    if (!lineage || lineage.nodes.length === 0) {
      return { initialNodes: [], initialEdges: [], sourceCount: 0, derivedCount: 0 };
    }

    // Separate sources and derived
    const sourceNodes = lineage.nodes.filter((n) => n.type === 'source');
    const derivedNodes = lineage.nodes.filter((n) => n.type === 'derived');

    // Build parent map for layout
    const parentMap = new Map<string, string[]>();
    lineage.nodes.forEach((n) => parentMap.set(n.id, []));
    lineage.edges.forEach((e) => {
      parentMap.get(e.target)?.push(e.source);
    });

    // Calculate layers
    const layers = new Map<string, number>();

    // Sources are layer 0
    sourceNodes.forEach((n) => layers.set(n.id, 0));

    // BFS to assign layers
    const queue = [...sourceNodes.map((n) => n.id)];
    const visited = new Set<string>(queue);

    while (queue.length > 0) {
      const nodeId = queue.shift()!;
      const currentLayer = layers.get(nodeId) || 0;

      // Find children
      lineage.edges
        .filter((e) => e.source === nodeId)
        .forEach((e) => {
          const existingLayer = layers.get(e.target);
          if (existingLayer === undefined || currentLayer + 1 > existingLayer) {
            layers.set(e.target, currentLayer + 1);
          }
          if (!visited.has(e.target)) {
            visited.add(e.target);
            queue.push(e.target);
          }
        });
    }

    // Group by layer
    const layerGroups = new Map<number, string[]>();
    layers.forEach((layer, nodeId) => {
      if (!layerGroups.has(layer)) {
        layerGroups.set(layer, []);
      }
      layerGroups.get(layer)!.push(nodeId);
    });

    // Create nodes with positions
    const nodeWidth = 160;
    const nodeHeight = 90;
    const horizontalSpacing = 120;
    const verticalSpacing = 40;

    const nodes: Node[] = lineage.nodes.map((node, index) => {
      const layer = layers.get(node.id) || 0;
      const layerNodes = layerGroups.get(layer) || [];
      const indexInLayer = layerNodes.indexOf(node.id);
      const totalHeight = layerNodes.length * nodeHeight + (layerNodes.length - 1) * verticalSpacing;

      return {
        id: node.id,
        type: 'lineageNode',
        position: {
          x: layer * (nodeWidth + horizontalSpacing) + 50,
          y: -totalHeight / 2 + indexInLayer * (nodeHeight + verticalSpacing) + 150,
        },
        data: { ...node, index },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      };
    });

    // Create edges
    const edges: Edge[] = lineage.edges.map((edge) => ({
      id: `e-${edge.source}-${edge.target}`,
      source: edge.source,
      target: edge.target,
      type: 'smoothstep',
      animated: true,
      style: {
        stroke: 'url(#lineage-gradient)',
        strokeWidth: 3,
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: '#8B5CF6',
        width: 25,
        height: 25,
      },
    }));

    return {
      initialNodes: nodes,
      initialEdges: edges,
      sourceCount: sourceNodes.length,
      derivedCount: derivedNodes.length,
    };
  }, [lineage]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  useEffect(() => {
    setNodes(initialNodes);
    setEdges(initialEdges);
  }, [initialNodes, initialEdges, setNodes, setEdges]);

  if (!lineage || lineage.nodes.length === 0) {
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
      {/* SVG Gradient Definition */}
      <svg style={{ position: 'absolute', width: 0, height: 0 }}>
        <defs>
          <linearGradient id="lineage-gradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#10B981" />
            <stop offset="100%" stopColor="#8B5CF6" />
          </linearGradient>
        </defs>
      </svg>

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
            const type = (node.data as LineageNodeType)?.type;
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
