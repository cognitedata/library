import {
  DialogContent as AuraDialogContent,
  Dialog,
  DialogClose,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogOverlay,
  DialogPortal,
  DialogTitle,
  DialogTrigger,
} from "@cognite/aura/components";
import type { ComponentProps } from "react";
import { cn } from "@/lib/utils";

type DialogContentProps = ComponentProps<typeof AuraDialogContent>;

function DialogContent({ className, ...props }: DialogContentProps) {
  return (
    <AuraDialogContent
      className={cn(
        // Aura uses top-5 + right-4 while the shell uses px-5 — shift looks off vs the corner.
        "[&_[data-slot=dialog-close]]:!top-4 [&_[data-slot=dialog-close]]:!right-4",
        className,
        // Base UI applies Floating UI inline position/transform, which overrides Tailwind centering.
        // Important utilities win over those inline styles so modals stay viewport-centered.
        "!left-1/2 !top-1/2 !right-auto !bottom-auto !-translate-x-1/2 !-translate-y-1/2",
      )}
      {...props}
    />
  );
}

export {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogOverlay,
  DialogPortal,
  DialogTitle,
  DialogTrigger,
};
