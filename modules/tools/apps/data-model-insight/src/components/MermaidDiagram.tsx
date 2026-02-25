import { useEffect, useState } from "react";
import mermaid from "mermaid";

mermaid.initialize({
  startOnLoad: false,
  theme: "dark",
  securityLevel: "loose",
  flowchart: { useMaxWidth: true },
});

/** Sanitize SVG string before innerHTML to avoid parse errors and XSS. Removes script and event handlers. */
function sanitizeSvgForDom(svg: string): string {
  if (typeof svg !== "string") return "";
  return svg
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, "")
    .replace(/\s+on\w+\s*=\s*["'][^"']*["']/gi, "")
    .replace(/\s+on\w+\s*=\s*[^\s>]+/gi, "");
}

interface MermaidDiagramProps {
  source: string;
  className?: string;
  /** If true, always show raw source below (for CDF when render may be blocked) */
  showSourceFallback?: boolean;
  /** When Mermaid fails, show this (e.g. SVG diagram for Level 1) */
  renderErrorFallback?: React.ReactNode;
}

export function MermaidDiagram({ source, className = "", showSourceFallback = true, renderErrorFallback }: MermaidDiagramProps) {
  const [error, setError] = useState<string | null>(null);
  const [svg, setSvg] = useState<string | null>(null);

  useEffect(() => {
    if (!source.trim()) {
      setSvg(null);
      setError(null);
      return;
    }
    setError(null);
    setSvg(null);

    const id = `mermaid-${Math.random().toString(36).slice(2, 9)}`;

    mermaid
      .render(id, source)
      .then(({ svg: s }) => {
        setSvg(sanitizeSvgForDom(s));
      })
      .catch((err) => {
        setSvg(null);
        const msg = err?.message ?? "Diagram render failed.";
        setError(msg.includes("security") ? "Diagram render failed (restricted in this environment). See source below." : msg);
      });
  }, [source]);

  if (!source.trim()) {
    return (
      <div className={`rounded-lg bg-slate-800 border border-slate-600 p-4 text-slate-500 text-sm ${className}`}>
        No diagram source
      </div>
    );
  }

  if (error) {
    return (
      <div className={`space-y-3 ${className}`}>
        {renderErrorFallback}
        <div className="rounded-lg bg-amber-950/50 border border-amber-700 p-3 text-amber-200 text-sm">
          Diagram could not be rendered (e.g. restricted in this environment): {error}
        </div>
        {showSourceFallback && (
          <div className="rounded-lg bg-slate-800 border border-slate-600 overflow-hidden">
            <div className="px-3 py-2 text-xs font-medium text-slate-400 border-b border-slate-600 bg-slate-900/50">
              Mermaid source (copy to render elsewhere)
            </div>
            <pre className="p-4 text-slate-300 text-sm overflow-auto max-h-[400px] whitespace-pre font-mono">
              {source}
            </pre>
          </div>
        )}
      </div>
    );
  }

  if (!svg) {
    return (
      <div className={`space-y-3 ${className}`}>
        <div className="rounded-lg bg-slate-800 border border-slate-600 p-4 text-slate-500 text-sm">
          Rendering diagram…
        </div>
        {showSourceFallback && (
          <pre className="rounded-lg bg-slate-800 border border-slate-600 p-4 text-slate-400 text-xs overflow-auto max-h-[200px] whitespace-pre font-mono">
            {source}
          </pre>
        )}
      </div>
    );
  }

  return (
    <div className={`space-y-3 ${className}`}>
      <div className="rounded-lg bg-slate-800 border border-slate-600 overflow-auto p-2 min-h-[180px] flex items-center justify-center">
        <div className="mermaid-svg-wrapper [&_svg]:max-w-full [&_svg]:h-auto" dangerouslySetInnerHTML={{ __html: svg }} />
      </div>
      {showSourceFallback && (
        <details className="rounded-lg bg-slate-800/80 border border-slate-600 overflow-hidden">
          <summary className="px-3 py-2 text-xs font-medium text-slate-400 cursor-pointer hover:bg-slate-700/50">
            Show Mermaid source
          </summary>
          <pre className="p-4 text-slate-400 text-xs overflow-auto max-h-[300px] whitespace-pre font-mono border-t border-slate-600">
            {source}
          </pre>
        </details>
      )}
    </div>
  );
}
