/**
 * Standardized Confirmation Dialog Component.
 * Provides consistent confirmation patterns across all destructive actions in the app.
 */

import { FileX, Save, Settings, Trash2, UserX } from "lucide-react";
import { Loader } from "@/components/Loader";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

type ActionType =
  "delete" | "remove" | "save" | "replace" | "reset" | "archive" | "disconnect";

interface ConfirmationDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description: string;
  actionType: ActionType;
  actionLabel?: string;
  onConfirm: () => Promise<void> | void;
  onCancel?: () => void;
  isProcessing?: boolean;
  processingText?: string;
  variant?: "destructive" | "warning" | "default";
  itemName?: string;
  consequences?: string[];
}

const ACTION_CONFIG = {
  delete: {
    icon: Trash2,
    defaultLabel: "Delete",
    variant: "destructive" as const,
    iconColor: "text-destructive-foreground",
  },
  remove: {
    icon: UserX,
    defaultLabel: "Remove",
    variant: "destructive" as const,
    iconColor: "text-destructive-foreground",
  },
  save: {
    icon: Save,
    defaultLabel: "Save",
    variant: "default" as const,
    iconColor: "text-foreground-link",
  },
  replace: {
    icon: FileX,
    defaultLabel: "Replace",
    variant: "warning" as const,
    iconColor: "text-warning-foreground",
  },
  reset: {
    icon: Settings,
    defaultLabel: "Reset",
    variant: "warning" as const,
    iconColor: "text-warning-foreground",
  },
  archive: {
    icon: FileX,
    defaultLabel: "Archive",
    variant: "warning" as const,
    iconColor: "text-warning-foreground",
  },
  disconnect: {
    icon: UserX,
    defaultLabel: "Disconnect",
    variant: "destructive" as const,
    iconColor: "text-destructive-foreground",
  },
} as const;

function ConfirmationDialog({
  open,
  onOpenChange,
  title,
  description,
  actionType,
  actionLabel,
  onConfirm,
  onCancel,
  isProcessing = false,
  processingText,
  variant,
  itemName,
  consequences = [],
}: ConfirmationDialogProps) {
  const config = ACTION_CONFIG[actionType];
  const finalActionLabel = actionLabel || config.defaultLabel;
  const finalVariant = variant || config.variant;
  const finalProcessingText =
    processingText || `${finalActionLabel.toLowerCase()}ing...`;

  const IconComponent = config.icon;

  const handleConfirm = async () => {
    try {
      await onConfirm();
      // Don't close dialog here - let parent handle success/error states
    } catch (error) {
      console.error("Confirmation action failed:", error);
      // Error handling will be done by parent component
    }
  };

  const handleCancel = () => {
    if (onCancel) {
      onCancel();
    }
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[calc(100%-2rem)] max-w-md sm:w-full">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <IconComponent className={`h-5 w-5 ${config.iconColor}`} />
            {title}
          </DialogTitle>
          <DialogDescription className="sr-only">
            {[
              description,
              itemName,
              consequences.length > 0
                ? `This action will: ${consequences.join(", ")}`
                : "",
              finalVariant === "destructive"
                ? "This action cannot be undone."
                : "",
            ]
              .filter(Boolean)
              .join(" ")}
          </DialogDescription>
          <div className="text-left space-y-2 text-muted-foreground text-base font-normal leading-5 tracking-tightest pt-2">
            <p>{description}</p>

            {itemName && (
              <p className="font-medium text-foreground">{itemName}</p>
            )}

            {consequences.length > 0 && (
              <div className="space-y-1">
                <p className="font-medium text-foreground">This action will:</p>
                <ul className="list-disc list-inside space-y-1 text-sm">
                  {consequences.map((consequence) => (
                    <li key={consequence}>{consequence}</li>
                  ))}
                </ul>
              </div>
            )}

            {finalVariant === "destructive" && (
              <p className="font-medium text-destructive-foreground">
                This action cannot be undone.
              </p>
            )}
          </div>
        </DialogHeader>

        <DialogFooter className="gap-2 flex-col sm:flex-row sm:justify-end">
          <Button
            type="button"
            variant="outline"
            onClick={handleCancel}
            disabled={isProcessing}
          >
            Cancel
          </Button>
          <Button
            type="button"
            variant={finalVariant === "destructive" ? "destructive" : "default"}
            onClick={handleConfirm}
            disabled={isProcessing}
            className="min-w-20"
          >
            {isProcessing ? (
              <>
                <Loader className="mr-2" size={16} />
                {finalProcessingText}
              </>
            ) : (
              finalActionLabel
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// Pre-configured confirmation dialogs for common scenarios

interface DeleteConfirmationProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  itemType: string;
  itemName?: string;
  onConfirm: () => Promise<void> | void;
  onCancel?: () => void;
  isProcessing?: boolean;
  additionalConsequences?: string[];
}

export function DeleteConfirmation({
  open,
  onOpenChange,
  itemType,
  itemName,
  onConfirm,
  onCancel,
  isProcessing = false,
  additionalConsequences = [],
}: DeleteConfirmationProps) {
  return (
    <ConfirmationDialog
      open={open}
      onOpenChange={onOpenChange}
      title={`Delete ${itemType}`}
      description={`Are you sure you want to delete this ${itemType.toLowerCase()}?`}
      actionType="delete"
      onConfirm={onConfirm}
      onCancel={onCancel}
      isProcessing={isProcessing}
      itemName={itemName}
      consequences={[
        "Remove all data permanently",
        "Cannot be recovered later",
        ...additionalConsequences,
      ]}
    />
  );
}
