/**
 * Static "How to Use the Testing Feature" dialog content.
 * Shared help content for the Tests page.
 */

import { Info } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { TestingInfoSections } from "./testing-info-sections";

export interface TestingInfoDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function TestingInfoDialog({
  open,
  onOpenChange,
}: TestingInfoDialogProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl max-h-[85vh] overflow-hidden p-0">
        <div className="bg-muted/50 border-b border-border px-6 py-4">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-lg">
              <Info className="h-5 w-5 text-primary" />
              How to Use the Testing Feature
            </DialogTitle>
            <p className="text-sm text-muted-foreground mt-1">
              A guide to validating AI summary consistency and quality
            </p>
          </DialogHeader>
        </div>

        <TestingInfoSections />
      </DialogContent>
    </Dialog>
  );
}
