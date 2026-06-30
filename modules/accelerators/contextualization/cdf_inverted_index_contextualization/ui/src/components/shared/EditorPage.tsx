import type { ReactNode } from "react";
import { PaneHeader } from "./PaneHeader";

type Props = {
  title: string;
  hint?: string;
  badge?: ReactNode;
  children: ReactNode;
  className?: string;
};

export function EditorPage({ title, hint, badge, children, className }: Props) {
  return (
    <div className={className ? `idx-pane idx-editor-page ${className}` : "idx-pane idx-editor-page"}>
      <PaneHeader title={title} hint={hint} badge={badge} />
      {children}
    </div>
  );
}
