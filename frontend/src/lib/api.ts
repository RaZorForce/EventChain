/**
 * API client for EventChainTrader backend
 */

const API_BASE = 'http://localhost:8000';

// Types
export interface BackendStatus {
  status: 'running' | 'stopped' | 'not_initialized';
  equity: number;
  holdings: Record<string, number>;
}

export interface UniverseSymbol {
  ticker: string;
  name: string;
  sector?: string;
  marketCap?: string;
}

export interface Universe {
  id: string;
  name: string;
  description: string;
  created_at: string;
  format: 'symbol_list' | 'ohlcv';
  symbol_count?: number;
  symbols?: UniverseSymbol[];
  symbol?: string;
  file_path?: string;
}

export interface UploadUniverseResponse {
  status: 'success';
  universe_id: string;
  name: string;
  format: 'symbol_list' | 'ohlcv';
  path: string;
  symbols_count?: number;
  symbols?: UniverseSymbol[];
  symbol?: string;
  ohlcv_path?: string;
  message?: string;
}

export interface ListUniversesResponse {
  universes: Universe[];
}

// API functions
export const api = {
  // Trading system status
  getStatus: async (): Promise<BackendStatus> => {
    const response = await fetch(`${API_BASE}/status`);
    if (!response.ok) throw new Error('Failed to get status');
    return response.json();
  },

  start: async (): Promise<{ message: string }> => {
    const response = await fetch(`${API_BASE}/start`, { method: 'POST' });
    if (!response.ok) throw new Error('Failed to start');
    return response.json();
  },

  stop: async (): Promise<{ message: string }> => {
    const response = await fetch(`${API_BASE}/stop`, { method: 'POST' });
    if (!response.ok) throw new Error('Failed to stop');
    return response.json();
  },

  // Universe management
  uploadUniverse: async (
    file: File,
    name: string,
    description: string = ''
  ): Promise<UploadUniverseResponse> => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('name', name);
    formData.append('description', description);

    const response = await fetch(`${API_BASE}/api/data/upload-universe`, {
      method: 'POST',
      body: formData,
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Upload failed' }));
      throw new Error(error.detail || 'Failed to upload universe');
    }

    return response.json();
  },

  listUniverses: async (): Promise<ListUniversesResponse> => {
    const response = await fetch(`${API_BASE}/api/data/universes`);
    if (!response.ok) throw new Error('Failed to list universes');
    return response.json();
  },

  deleteUniverse: async (universeId: string): Promise<{ status: string; message: string }> => {
    const response = await fetch(`${API_BASE}/api/data/universe/${universeId}`, {
      method: 'DELETE',
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Delete failed' }));
      throw new Error(error.detail || 'Failed to delete universe');
    }

    return response.json();
  },
};

// Helper to check if backend is available
export async function isBackendAvailable(): Promise<boolean> {
  try {
    const response = await fetch(`${API_BASE}/status`, {
      method: 'GET',
      signal: AbortSignal.timeout(2000),
    });
    return response.ok;
  } catch {
    return false;
  }
}
