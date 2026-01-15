/**
 * Server-side configuration storage
 * This stores Snowflake config in memory for API routes to access
 */

import { SnowflakeConfig } from './snowflake';

// In-memory storage for server-side config
// In production, you might want to use Redis or a database
let serverConfig: SnowflakeConfig | null = null;

export function getServerConfig(): SnowflakeConfig | null {
  return serverConfig;
}

export function setServerConfig(config: SnowflakeConfig | null): void {
  serverConfig = config;
  console.log('Server config updated:', config ? 'Config stored' : 'Config cleared');
}

export function hasServerConfig(): boolean {
  return serverConfig !== null;
}

