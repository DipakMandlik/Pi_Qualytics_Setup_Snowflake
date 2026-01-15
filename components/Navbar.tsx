'use client';

import { useState } from 'react';
import Image from 'next/image';
import { ConnectionDialog } from './ConnectionDialog';
import { Database } from 'lucide-react';
import { Button } from './ui/button';

export function Navbar() {
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  return (
    <>
      <nav className="border-b border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900">
        <div className="container mx-auto px-4">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center">
              <Image
                src="/Logo2.png"
                alt="Pi-Qualytics Logo"
                width={120}
                height={40}
                className="h-28 w-50 object-contain"
                priority
              />
            </div>
            <div className="flex items-center">
              <Button
                variant="outline"
                onClick={() => setIsDialogOpen(true)}
                className="flex items-center gap-2"
              >
                <Database className="size-4" />
                Connect
              </Button>
            </div>
          </div>
        </div>
      </nav>
      <ConnectionDialog
        isOpen={isDialogOpen}
        onClose={() => setIsDialogOpen(false)}
      />
    </>
  );
}

