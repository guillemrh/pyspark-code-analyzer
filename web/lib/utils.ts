import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function getSeverityColor(severity: string): string {
  switch (severity) {
    case 'HIGH':
      return 'text-severity-high bg-severity-high/10 border-severity-high/30';
    case 'MEDIUM':
      return 'text-severity-medium bg-severity-medium/10 border-severity-medium/30';
    case 'WARNING':
      return 'text-severity-warning bg-severity-warning/10 border-severity-warning/30';
    case 'INFO':
      return 'text-severity-info bg-severity-info/10 border-severity-info/30';
    default:
      return 'text-text-secondary bg-bg-light border-bg-light';
  }
}

export function getNodeColor(opType: string): string {
  switch (opType) {
    case 'input':
      return 'node-input';
    case 'transformation':
      return 'node-transform';
    case 'shuffle':
      return 'node-shuffle';
    case 'action':
      return 'node-action';
    default:
      return 'node-transform';
  }
}

export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}

export function truncateText(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text;
  return text.slice(0, maxLength - 3) + '...';
}
