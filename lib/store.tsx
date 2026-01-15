'use client';

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { SnowflakeConfig } from './snowflake';

interface AppStoreContextType {
  snowflakeConfig: SnowflakeConfig | null;
  setSnowflakeConfig: (config: SnowflakeConfig | null) => void;
  isConnected: boolean;
  setIsConnected: (connected: boolean) => void;
}

const AppStoreContext = createContext<AppStoreContextType | undefined>(undefined);

export function AppStoreProvider({ children }: { children: ReactNode }) {
  const [snowflakeConfig, setSnowflakeConfigState] = useState<SnowflakeConfig | null>(null);
  const [isConnected, setIsConnected] = useState(false);

  // Load config from localStorage on mount
  useEffect(() => {
    const savedConfig = localStorage.getItem('snowflakeConfig');
    if (savedConfig) {
      try {
        const config = JSON.parse(savedConfig);
        setSnowflakeConfigState(config);
      } catch (error) {
        console.error('Failed to load saved config:', error);
      }
    }
  }, []);

  const setSnowflakeConfig = (config: SnowflakeConfig | null) => {
    setSnowflakeConfigState(config);
    if (config) {
      localStorage.setItem('snowflakeConfig', JSON.stringify(config));
    } else {
      localStorage.removeItem('snowflakeConfig');
      setIsConnected(false);
    }
  };

  return (
    <AppStoreContext.Provider
      value={{
        snowflakeConfig,
        setSnowflakeConfig,
        isConnected,
        setIsConnected,
      }}
    >
      {children}
    </AppStoreContext.Provider>
  );
}

export function useAppStore() {
  const context = useContext(AppStoreContext);
  if (context === undefined) {
    throw new Error('useAppStore must be used within an AppStoreProvider');
  }
  return context;
}

