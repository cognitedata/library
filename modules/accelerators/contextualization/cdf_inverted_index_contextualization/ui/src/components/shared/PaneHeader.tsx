import type { ReactNode } from "react";

type Props = {
  title: string;
  hint?: string;
  badge?: ReactNode;
};

export function PaneHeader({ title, hint, badge }: Props) {
  return (
    <header className="idx-pane-header">
      <div className="idx-pane-header__row">
        <div>
          <h2 className="idx-pane__title">{title}</h2>
          {hint ? <p className="idx-pane__hint">{hint}</p> : null}
        </div>
        {badge}
      </div>
    </header>
  );
}
