interface MessageBoxProps {
  text: string;
  variant: "info" | "success" | "warning" | "error";
  className?: string;
}

export function MessageBox({ text, variant, className }: MessageBoxProps) {
  const base = "rounded-lg border px-3 py-2 text-[11px]";
  const styles: Record<MessageBoxProps["variant"], string> = {
    info: "border-slate-200 bg-slate-50 text-slate-700",
    success: "border-emerald-200 bg-emerald-50/70 text-emerald-700",
    warning: "border-amber-200 bg-amber-50 text-amber-800",
    error: "border-rose-200 bg-rose-50 text-rose-700",
  };

  const classes = `${base} ${styles[variant]}${className ? ` ${className}` : ""}`;

  return <div className={classes}>{text}</div>;
}
