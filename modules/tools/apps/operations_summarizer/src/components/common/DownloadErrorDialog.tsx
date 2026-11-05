/**
 * Download Error Dialog Component.
 * Provides recovery options when download operations fail.
 */

import { AlertCircle, FileText, RefreshCw, X } from "lucide-react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import type { SummaryInstance } from "@/types/operations";

interface DownloadErrorDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  error: string;
  summary: SummaryInstance | null;
  onRetry: () => Promise<void>;
  onDownloadMarkdown: () => Promise<void>;
  onClose: () => void;
  isRetrying?: boolean;
  canRetry?: boolean;
}

export function DownloadErrorDialog({
  open,
  onOpenChange,
  error,
  summary,
  onRetry,
  onDownloadMarkdown,
  onClose,
  isRetrying = false,
  canRetry = true,
}: DownloadErrorDialogProps) {
  const handleRetry = async () => {
    try {
      await onRetry();
      onClose();
    } catch {
      // Error handling is done in the hook
    }
  };

  const handleDownloadMarkdown = async () => {
    try {
      await onDownloadMarkdown();
      onClose();
    } catch {
      // Error handling is done in the hook
    }
  };

  const handleClose = () => {
    onClose();
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <AlertCircle className="h-5 w-5 text-destructive" />
            Download Failed
          </DialogTitle>
        </DialogHeader>

        <Alert variant="error" className="mt-0">
          <AlertDescription className="text-left">{error}</AlertDescription>
        </Alert>

        <div className="space-y-3">
          <div className="text-sm text-muted-foreground">
            What would you like to do?
          </div>

          <div className="flex flex-col gap-2">
            {canRetry && (
              <Button
                variant="default"
                onClick={handleRetry}
                disabled={isRetrying}
                className="justify-start"
              >
                {isRetrying ? (
                  <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <RefreshCw className="h-4 w-4 mr-2" />
                )}
                Try PDF download again
              </Button>
            )}

            <Button
              variant="outline"
              onClick={handleDownloadMarkdown}
              className="justify-start"
            >
              <FileText className="h-4 w-4 mr-2" />
              Download as Markdown instead
            </Button>

            <Button
              variant="ghost"
              onClick={handleClose}
              className="justify-start"
            >
              <X className="h-4 w-4 mr-2" />
              Cancel and continue browsing
            </Button>
          </div>

          {summary && (
            <div className="pt-2 border-t border-border">
              <div className="text-xs text-muted-foreground">
                Failed to download: {summary.externalId}
              </div>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
