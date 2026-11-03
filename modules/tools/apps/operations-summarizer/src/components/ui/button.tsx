import { Button as AuraButton, buttonVariants } from "@cognite/aura/components";
import {
  type ComponentPropsWithoutRef,
  type ElementRef,
  forwardRef,
} from "react";
import { cn, PRIMARY_CTA_BUTTON_CLASSNAME } from "@/lib/utils";

type ButtonProps = ComponentPropsWithoutRef<typeof AuraButton>;

const Button = forwardRef<ElementRef<typeof AuraButton>, ButtonProps>(
  ({ className, variant, ...props }, ref) => {
    const isDefaultVariant = variant === undefined || variant === "default";
    return (
      <AuraButton
        ref={ref}
        variant={variant}
        className={cn(
          isDefaultVariant && PRIMARY_CTA_BUTTON_CLASSNAME,
          className,
        )}
        {...props}
      />
    );
  },
);
Button.displayName = "Button";

export { Button, buttonVariants };
