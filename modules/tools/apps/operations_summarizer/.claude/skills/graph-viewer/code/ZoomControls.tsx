function ZoomOutIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 20 20"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      role="img"
      aria-label="Zoom out"
    >
      <circle cx="9" cy="9" r="6" stroke="currentColor" strokeWidth="1.5" />
      <path d="M13.5 13.5L17 17" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      <path d="M6 9H12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

function ZoomInIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 20 20"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      role="img"
      aria-label="Zoom in"
    >
      <circle cx="9" cy="9" r="6" stroke="currentColor" strokeWidth="1.5" />
      <path d="M13.5 13.5L17 17" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      <path d="M9 6V12M6 9H12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

function FitViewIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 20 20"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      role="img"
      aria-label="Fit view"
    >
      <path
        d="M2 7V3.5C2 2.67 2.67 2 3.5 2H7"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M13 2H16.5C17.33 2 18 2.67 18 3.5V7"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M18 13V16.5C18 17.33 17.33 18 16.5 18H13"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M7 18H3.5C2.67 18 2 17.33 2 16.5V13"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <rect
        x="8"
        y="8"
        width="4"
        height="4"
        rx="0.5"
        stroke="currentColor"
        strokeWidth="1.5"
        transform="rotate(0 10 10)"
      />
    </svg>
  );
}

interface ZoomControlsProps {
  onZoomIn: () => void;
  onZoomOut: () => void;
  onFitView: () => void;
}

export function ZoomControls({ onZoomIn, onZoomOut, onFitView }: ZoomControlsProps) {
  const btnClass =
    "p-2 text-muted-foreground hover:text-foreground hover:bg-accent dark:hover:bg-accent/50 rounded-lg active:scale-95 transition-colors";

  return (
    <div className="absolute bottom-4 right-4 flex items-center gap-0.5 bg-background/95 backdrop-blur-sm border border-border rounded-xl shadow-lg p-1.5 z-10">
      <button type="button" onClick={onZoomOut} className={btnClass} title="Zoom Out">
        <ZoomOutIcon className="w-4 h-4" />
      </button>
      <button type="button" onClick={onZoomIn} className={btnClass} title="Zoom In">
        <ZoomInIcon className="w-4 h-4" />
      </button>
      <div className="w-px h-5 bg-border mx-1" />
      <button type="button" onClick={onFitView} className={btnClass} title="Fit to View">
        <FitViewIcon className="w-4 h-4" />
      </button>
    </div>
  );
}
