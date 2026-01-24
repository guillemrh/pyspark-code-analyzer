// Parse Graphviz DOT format to extract nodes and edges for React Flow

export interface ParsedNode {
  id: string;
  label: string;
  stage?: number;
  nodeType: 'input' | 'transform' | 'shuffle' | 'action';
  isShuffleNode: boolean;
}

export interface ParsedEdge {
  source: string;
  target: string;
}

export interface ParsedGraph {
  nodes: ParsedNode[];
  edges: ParsedEdge[];
}

// Shuffle operations that cause stage boundaries
const SHUFFLE_OPS = new Set(['join', 'groupBy', 'repartition', 'distinct', 'coalesce', 'sortBy', 'orderBy']);
const ACTION_OPS = new Set(['show', 'collect', 'count', 'write', 'save', 'foreach', 'take', 'first', 'head']);
const INPUT_OPS = new Set(['read', 'parquet', 'csv', 'json', 'jdbc', 'table', 'load', 'textFile']);

function detectNodeType(label: string): 'input' | 'transform' | 'shuffle' | 'action' {
  const opName = label.split('\\n')[0].toLowerCase().trim();

  if (INPUT_OPS.has(opName)) return 'input';
  if (ACTION_OPS.has(opName)) return 'action';
  if (SHUFFLE_OPS.has(opName)) return 'shuffle';
  return 'transform';
}

export function parseDot(dot: string): ParsedGraph {
  const nodes: ParsedNode[] = [];
  const edges: ParsedEdge[] = [];
  const nodeSet = new Set<string>();

  if (!dot) {
    return { nodes, edges };
  }

  // Parse node definitions: "node_id" [label="..."];
  const nodeRegex = /"([^"]+)"\s*\[([^\]]*)\]/g;
  let match;

  while ((match = nodeRegex.exec(dot)) !== null) {
    const nodeId = match[1];
    const attrs = match[2];

    // Extract label
    const labelMatch = attrs.match(/label="([^"]+)"/);
    const label = labelMatch ? labelMatch[1].replace(/\\n/g, '\n') : nodeId;

    // Extract stage from label (e.g., "filter\nStage 0")
    const stageMatch = label.match(/Stage\s+(\d+)/i);
    const stage = stageMatch ? parseInt(stageMatch[1], 10) : undefined;

    // Check if it's a shuffle node (has special styling in DOT)
    const isShuffleNode = attrs.includes('color="#C0392B"') ||
                          attrs.includes('fillcolor="#C0392B"') ||
                          SHUFFLE_OPS.has(label.split('\n')[0].toLowerCase());

    // Detect node type
    const nodeType = detectNodeType(label);

    if (!nodeSet.has(nodeId)) {
      nodeSet.add(nodeId);
      nodes.push({
        id: nodeId,
        label: label.split('\n')[0], // Just the operation name
        stage,
        nodeType: isShuffleNode ? 'shuffle' : nodeType,
        isShuffleNode,
      });
    }
  }

  // Parse edge definitions: "source" -> "target";
  const edgeRegex = /"([^"]+)"\s*->\s*"([^"]+)"/g;

  while ((match = edgeRegex.exec(dot)) !== null) {
    const source = match[1];
    const target = match[2];

    // Add nodes if they don't exist (from edge-only definitions)
    if (!nodeSet.has(source)) {
      nodeSet.add(source);
      nodes.push({
        id: source,
        label: source.split('_')[1] || source,
        nodeType: 'transform',
        isShuffleNode: false,
      });
    }
    if (!nodeSet.has(target)) {
      nodeSet.add(target);
      nodes.push({
        id: target,
        label: target.split('_')[1] || target,
        nodeType: 'transform',
        isShuffleNode: false,
      });
    }

    edges.push({ source, target });
  }

  return { nodes, edges };
}

// Calculate node positions using a layered layout algorithm
export function calculateLayout(
  graph: ParsedGraph,
  nodeWidth: number = 160,
  nodeHeight: number = 70,
  horizontalSpacing: number = 80,
  verticalSpacing: number = 40
): Map<string, { x: number; y: number }> {
  const positions = new Map<string, { x: number; y: number }>();

  if (graph.nodes.length === 0) {
    return positions;
  }

  // Build adjacency list
  const children = new Map<string, string[]>();
  const parents = new Map<string, string[]>();

  graph.nodes.forEach(node => {
    children.set(node.id, []);
    parents.set(node.id, []);
  });

  graph.edges.forEach(edge => {
    children.get(edge.source)?.push(edge.target);
    parents.get(edge.target)?.push(edge.source);
  });

  // Find roots (nodes with no parents)
  const roots = graph.nodes.filter(node => parents.get(node.id)?.length === 0);

  // If no roots found, use all nodes (circular dependency case)
  const startNodes = roots.length > 0 ? roots : graph.nodes;

  // Assign layers using BFS
  const layers = new Map<string, number>();
  const queue: string[] = startNodes.map(n => n.id);
  const visited = new Set<string>();

  startNodes.forEach(n => layers.set(n.id, 0));

  while (queue.length > 0) {
    const nodeId = queue.shift()!;
    if (visited.has(nodeId)) continue;
    visited.add(nodeId);

    const currentLayer = layers.get(nodeId) || 0;
    const nodeChildren = children.get(nodeId) || [];

    nodeChildren.forEach(childId => {
      const existingLayer = layers.get(childId);
      const newLayer = currentLayer + 1;

      if (existingLayer === undefined || newLayer > existingLayer) {
        layers.set(childId, newLayer);
      }

      if (!visited.has(childId)) {
        queue.push(childId);
      }
    });
  }

  // Handle any unvisited nodes
  graph.nodes.forEach(node => {
    if (!layers.has(node.id)) {
      layers.set(node.id, 0);
    }
  });

  // Group nodes by layer
  const layerGroups = new Map<number, string[]>();
  layers.forEach((layer, nodeId) => {
    if (!layerGroups.has(layer)) {
      layerGroups.set(layer, []);
    }
    layerGroups.get(layer)!.push(nodeId);
  });

  // Calculate positions
  const maxLayer = Math.max(...Array.from(layers.values()));

  layerGroups.forEach((nodeIds, layer) => {
    const totalHeight = nodeIds.length * nodeHeight + (nodeIds.length - 1) * verticalSpacing;
    const startY = -totalHeight / 2;

    nodeIds.forEach((nodeId, index) => {
      positions.set(nodeId, {
        x: layer * (nodeWidth + horizontalSpacing) + 50,
        y: startY + index * (nodeHeight + verticalSpacing) + 100,
      });
    });
  });

  return positions;
}
