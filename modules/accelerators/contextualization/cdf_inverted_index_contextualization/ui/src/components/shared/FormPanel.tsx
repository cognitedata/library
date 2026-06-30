import type { ReactNode } from "react";

type Props = {
  title: string;
  hint?: string;
  children: ReactNode;
  className?: string;
  variant?: "default" | "compact";
};

export function FormPanel({ title, hint, children, className, variant = "default" }: Props) {
  const panelClass = [
    "idx-panel",
    variant === "compact" ? "idx-panel--compact" : "",
    className ?? "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <section className={panelClass}>
      <header className="idx-panel__header">
        <h3 className="idx-panel__title">{title}</h3>
        {hint ? <p className="idx-panel__hint">{hint}</p> : null}
      </header>
      <div className="idx-panel__body">{children}</div>
    </section>
  );
}
