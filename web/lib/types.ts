// API Types

export interface SubmitCodeRequest {
  code: string;
}

export interface SubmitCodeResponse {
  job_id: string;
  status: string;
  message?: string;
}

export interface DAGNode {
  id: string;
  label: string;
  op_type: 'input' | 'transformation' | 'shuffle' | 'action';
  operation: string;
  df_name: string;
  args?: Record<string, unknown>;
  stage_id?: number;
}

export interface DAGEdge {
  source: string;
  target: string;
}

export interface OperationDAG {
  nodes: DAGNode[];
  edges: DAGEdge[];
}

export interface LineageNode {
  id: string;
  label: string;
  type: 'source' | 'derived';
}

export interface LineageEdge {
  source: string;
  target: string;
  relationship?: string;
}

export interface LineageGraph {
  nodes: LineageNode[];
  edges: LineageEdge[];
}

export interface Stage {
  stage_id: number;
  operations: string[];
  node_count: number;
  has_shuffle: boolean;
}

export interface AntiPattern {
  rule_name: string;
  severity: 'HIGH' | 'MEDIUM' | 'WARNING' | 'INFO';
  message: string;
  affected_nodes: string[];
  suggestion: string;
  line_number?: number;
}

export interface AnalysisMetrics {
  total_operations: number;
  transformations: number;
  actions: number;
  shuffles: number;
  stages: number;
  anti_patterns_found: number;
}

// Backend response structures
export interface BackendDAGSummary {
  json: {
    total_operations: number;
    transformations: number;
    actions: number;
    wide_operations: number;
    stages: number;
    roots: string[];
    leaves: string[];
  };
  markdown: string;
}

export interface BackendStageSummary {
  json: {
    total_stages: number;
    stages: Array<{
      stage_id: number;
      operations: string[];
      node_count: number;
      has_shuffle_boundary: boolean;
    }>;
  };
  markdown: string;
}

export interface BackendLineageSummary {
  json: {
    total_dataframes: number;
    source_dataframes: string[];
    derived_dataframes: string[];
    edges: Array<{ from: string; to: string }>;
  };
  markdown: string;
}

export interface BackendAntipatternSummary {
  json: {
    total_findings: number;
    findings: Array<{
      rule_name: string;
      severity: 'HIGH' | 'MEDIUM' | 'WARNING' | 'INFO';
      message: string;
      affected_nodes: string[];
      suggestion: string;
      line_number?: number;
    }>;
    by_severity: Record<string, number>;
  };
  markdown: string;
}

export interface BackendAnalysisResult {
  dag_dot: string;
  lineage_dot: string;
  dag_summary: BackendDAGSummary;
  stage_summary: BackendStageSummary;
  lineage_summary: BackendLineageSummary;
  antipatterns: BackendAntipatternSummary;
}

export interface BackendLLMResult {
  explanation?: string;
  latency_ms?: number;
  tokens_used?: number;
  error?: string;
}

export interface BackendJobResult {
  analysis?: BackendAnalysisResult;
  llm?: BackendLLMResult;
  error?: {
    type: string;
    message: string;
  };
}

// Frontend processed result
export interface JobResult {
  explanation: string;
  operation_dag: OperationDAG;
  lineage_graph: LineageGraph;
  stages: Stage[];
  anti_patterns: AntiPattern[];
  metrics: AnalysisMetrics;
  dag_dot?: string;
  lineage_dot?: string;
}

export interface JobStatusResponse {
  job_id: string;
  status: 'pending' | 'running' | 'analysis_complete' | 'finished' | 'failed';
  result?: BackendJobResult;
  job_duration_ms?: number;
  cached?: boolean;
}

export interface CodeExample {
  name: string;
  description: string;
  code: string;
}

// UI Types

export type TabId = 'explanation' | 'stages' | 'antipatterns' | 'dag' | 'lineage';

export interface Tab {
  id: TabId;
  label: string;
  icon?: string;
}

// Helper function to transform backend response to frontend format
export function transformBackendResult(backendResult: BackendJobResult): JobResult | null {
  if (!backendResult.analysis) {
    return null;
  }

  const { analysis, llm } = backendResult;

  // Transform stages
  const stages: Stage[] = (analysis.stage_summary?.json?.stages || []).map((s) => ({
    stage_id: s.stage_id,
    operations: s.operations,
    node_count: s.node_count,
    has_shuffle: s.has_shuffle_boundary,
  }));

  // Transform anti-patterns
  const antiPatterns: AntiPattern[] = (analysis.antipatterns?.json?.findings || []).map((f) => ({
    rule_name: f.rule_name,
    severity: f.severity,
    message: f.message,
    affected_nodes: f.affected_nodes,
    suggestion: f.suggestion,
    line_number: f.line_number,
  }));

  // Build metrics
  const dagJson = analysis.dag_summary?.json;
  const metrics: AnalysisMetrics = {
    total_operations: dagJson?.total_operations || 0,
    transformations: dagJson?.transformations || 0,
    actions: dagJson?.actions || 0,
    shuffles: dagJson?.wide_operations || 0,
    stages: dagJson?.stages || 0,
    anti_patterns_found: analysis.antipatterns?.json?.total_findings || 0,
  };

  // Transform lineage graph from DOT or summary
  const lineageSummary = analysis.lineage_summary?.json;
  const lineageNodes: LineageNode[] = [];
  const lineageEdges: LineageEdge[] = [];

  if (lineageSummary) {
    // Add source nodes
    (lineageSummary.source_dataframes || []).forEach((name) => {
      lineageNodes.push({ id: name, label: name, type: 'source' });
    });

    // Add derived nodes
    (lineageSummary.derived_dataframes || []).forEach((name) => {
      if (!lineageNodes.find((n) => n.id === name)) {
        lineageNodes.push({ id: name, label: name, type: 'derived' });
      }
    });

    // Add edges
    (lineageSummary.edges || []).forEach((edge) => {
      lineageEdges.push({ source: edge.from, target: edge.to });
    });
  }

  return {
    explanation: llm?.explanation || 'No explanation available.',
    operation_dag: { nodes: [], edges: [] }, // DAG will be rendered from DOT
    lineage_graph: { nodes: lineageNodes, edges: lineageEdges },
    stages,
    anti_patterns: antiPatterns,
    metrics,
    dag_dot: analysis.dag_dot,
    lineage_dot: analysis.lineage_dot,
  };
}
