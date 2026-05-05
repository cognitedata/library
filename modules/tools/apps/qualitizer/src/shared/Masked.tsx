import { usePrivateMode } from "./PrivateModeContext";

/**
 * Wraps text content and applies CSS blur in private mode.
 * Use for IDs, external IDs, space names, project names, user identifiers,
 * and any other customer-specific data that should not be visible on screen recordings.
 */
export function Masked({
  children,
  as: Tag = "span",
  className = "",
}: {
  children: React.ReactNode;
  as?: "span" | "div" | "td" | "code" | "pre";
  className?: string;
}) {
  const { isPrivateMode } = usePrivateMode();
  return (
    <Tag className={`${isPrivateMode ? "private-mask" : ""} ${className}`.trim()}>
      {children}
    </Tag>
  );
}
