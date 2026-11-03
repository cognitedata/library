/**
 * Error Recovery Dialog Component.
 * Provides recovery options when operations fail while preserving user configuration.
 */

import { AlertTriangle, RefreshCw, RotateCcw, Settings, X } from "lucide-react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

interface ErrorRecoveryDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  error: string;
  canRetry: boolean;
  onRetry: () => Promise<void>;
  onResetToIdle: () => void;
  onClose: () => void;
  isRetrying?: boolean;
  operationType?: string;
  preservedConfig?: {
    dateRange?: string;
    dataTypes?: string[];
    agent?: string;
  };
}

export function ErrorRecoveryDialog({
  open,
  onOpenChange,
  error,
  canRetry,
  onRetry,
  onResetToIdle,
  onClose,
  isRetrying = false,
  operationType = "operation",
  preservedConfig,
}: ErrorRecoveryDialogProps) {
  const handleRetry = async () => {
    try {
      await onRetry();
      onClose();
    } catch {
      // Error handling is done in the hook
    }
  };

  const handleResetToIdle = () => {
    onResetToIdle();
    onClose();
  };

  const handleClose = () => {
    onClose();
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-amber-600" />
            {operationType.charAt(0).toUpperCase() +
              operationType.slice(1)}{" "}
            Failed
          </DialogTitle>
        </DialogHeader>

        <Alert variant="warning" className="mt-0">
          <AlertDescription className="text-left">{error}</AlertDescription>
        </Alert>

        <div className="space-y-4">
          {/* Preserved Configuration Info */}
          {preservedConfig && (
            <div className="rounded-md bg-muted/50 p-3 border">
              <div className="flex items-center gap-2 text-sm font-medium text-foreground mb-2">
                <Settings className="h-4 w-4" />
                Your configuration has been preserved
              </div>
              <div className="text-sm text-muted-foreground space-y-1">
                {preservedConfig.dateRange && (
                  <div>Date range: {preservedConfig.dateRange}</div>
                )}
                {preservedConfig.dataTypes &&
                  preservedConfig.dataTypes.length > 0 && (
                    <div>
                      Data types: {preservedConfig.dataTypes.join(", ")}
                    </div>
                  )}
                {preservedConfig.agent && (
                  <div>Agent: {preservedConfig.agent}</div>
                )}
              </div>
            </div>
          )}

          {/* Recovery Options */}
          <div className="space-y-2">
            <div className="text-sm text-muted-foreground">
              What would you like to do?
            </div>

            {canRetry && (
              <Button
                variant="default"
                onClick={handleRetry}
                disabled={isRetrying}
                className="w-full justify-start"
              >
                {isRetrying ? (
                  <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <RefreshCw className="h-4 w-4 mr-2" />
                )}
                Try again with same settings
              </Button>
            )}

            <Button
              variant="outline"
              onClick={handleResetToIdle}
              className="w-full justify-start"
            >
              <RotateCcw className="h-4 w-4 mr-2" />
              Reset and modify settings
            </Button>

            <Button
              variant="ghost"
              onClick={handleClose}
              className="w-full justify-start"
            >
              <X className="h-4 w-4 mr-2" />
              Cancel and continue
            </Button>
          </div>

          {/* Additional Info */}
          <div className="pt-2 border-t border-border">
            <div className="text-xs text-muted-foreground">
              Your date range, data types, and other settings remain unchanged
              and ready for the next attempt.
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
