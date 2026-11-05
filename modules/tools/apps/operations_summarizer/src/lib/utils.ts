import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/** Primary filled actions: embedded hosts can inherit body `color` onto <button>. */
export const PRIMARY_CTA_BUTTON_CLASSNAME =
  "!bg-[var(--primary)] !text-[var(--primary-foreground)] hover:!bg-[var(--primary-hover)] [&_svg]:!text-[var(--primary-foreground)]";

/** Icon-only delete: clear background, red icon/text; red fill + white glyph on hover (contrast). */
const DELETE_BUTTON_BASE =
  "border-0 bg-transparent text-red-700 shadow-none hover:bg-red-600 hover:text-white dark:bg-transparent dark:text-red-500 dark:hover:bg-red-600 dark:hover:text-white [&_svg]:text-red-700 hover:[&_svg]:text-white dark:[&_svg]:text-red-500 dark:hover:[&_svg]:text-white focus-visible:shadow-focus-ring-destructive";

/** Icon-only delete: clear background, red icon/text; red fill + white glyph on hover (contrast). */
export const DELETE_ICON_BUTTON_CLASSNAME = cn(
  "inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-md p-0",
  DELETE_BUTTON_BASE,
);

/** Text delete (e.g. Delete Agent): same pattern as icon delete. */
export const DELETE_TEXT_BUTTON_CLASSNAME = cn(
  "font-semibold",
  DELETE_BUTTON_BASE,
);
