import { SubmitCodeResponse, JobStatusResponse } from './types';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8050';

class APIError extends Error {
  constructor(
    message: string,
    public status: number,
    public details?: unknown
  ) {
    super(message);
    this.name = 'APIError';
  }
}

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let details;
    try {
      details = await response.json();
    } catch {
      details = await response.text();
    }

    if (response.status === 429) {
      throw new APIError('Rate limit exceeded. Please wait a moment.', 429, details);
    }
    if (response.status === 400) {
      throw new APIError(details?.detail || 'Invalid request', 400, details);
    }
    throw new APIError(
      details?.detail || `Request failed: ${response.statusText}`,
      response.status,
      details
    );
  }

  return response.json();
}

export async function submitCode(code: string): Promise<SubmitCodeResponse> {
  const response = await fetch(`${API_BASE_URL}/explain/pyspark`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ code }),
  });

  return handleResponse<SubmitCodeResponse>(response);
}

export async function getJobStatus(jobId: string): Promise<JobStatusResponse> {
  const response = await fetch(`${API_BASE_URL}/status/${jobId}`);
  return handleResponse<JobStatusResponse>(response);
}

export async function checkHealth(): Promise<{ status: string }> {
  const response = await fetch(`${API_BASE_URL}/health`);
  return handleResponse<{ status: string }>(response);
}

export { APIError };
