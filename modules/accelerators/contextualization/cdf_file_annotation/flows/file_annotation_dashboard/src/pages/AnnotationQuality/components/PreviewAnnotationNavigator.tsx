import { CheckCircle2, ChevronLeft, ChevronRight, Search, Sparkles, X } from "lucide-react";
import { useEffect, useState } from "react";
import { Badge } from "@/shared/components/ui/badge";
import { Button } from "@/shared/components/ui/button";
import { Input } from "@/shared/components/ui/input";

interface NavigatorAnnotationItem {
  id: string;
  text: string;
  resourceType?: string;
  isActual: boolean;
}

interface PreviewAnnotationNavigatorProps {
  counts: {
    actual: number;
    potential: number;
  };
  activeCategory: "all" | "actual" | "potential";
  onCategoryChange: (category: "all" | "actual" | "potential") => void;
  searchQuery: string;
  onSearchChange: (value: string) => void;
  onSearchKeyDown: (event: React.KeyboardEvent<HTMLInputElement>) => void;
  onClearSearch: () => void;
  matches: NavigatorAnnotationItem[];
  activeMatchIndex: number;
  onPrevious: () => void;
  onNext: () => void;
  onResultClick: (index: number) => void;
  onResultDoubleClick: (index: number) => void;
  onResultHover: (id: string | null) => void;
  resultsListRef: React.RefObject<HTMLDivElement>;
  hoveredAnnotationId?: string | null;
  openSignal?: number | null;
  displayMatchIndex?: number;
  selectedAnnotationId?: string | null;
}

export function PreviewAnnotationNavigator(props: PreviewAnnotationNavigatorProps) {
  const {
    counts,
    activeCategory,
    onCategoryChange,
    searchQuery,
    onSearchChange,
    onSearchKeyDown,
    onClearSearch,
    matches,
    activeMatchIndex,
    onPrevious,
    onNext,
    onResultClick,
    onResultDoubleClick,
    onResultHover,
    resultsListRef,
    hoveredAnnotationId,
    openSignal,
    displayMatchIndex,
    selectedAnnotationId,
  } = props;

  const [isOpen, setIsOpen] = useState(false);

  useEffect(() => {
    if (openSignal == null) return;
    setIsOpen(true);
  }, [openSignal]);

  const effectiveMatchIndex = matches.length === 0
    ? 0
    : Math.max(0, Math.min(matches.length - 1, displayMatchIndex ?? activeMatchIndex));

  const selectedMatchIndex = selectedAnnotationId
    ? matches.findIndex((annotation) => annotation.id === selectedAnnotationId)
    : -1;

  return (
    <div className="rounded-md border bg-muted/20 p-2 space-y-2">
      <div className="flex items-center justify-between gap-2">
        <p className="text-xs font-medium text-foreground">Annotation Navigator</p>
        <Button
          variant="outline"
          size="sm"
          className="h-7 px-2 text-[10px]"
          onClick={() => setIsOpen((current) => !current)}
        >
          {isOpen ? "Hide" : `Show (${counts.actual + counts.potential})`}
        </Button>
      </div>

      {!isOpen ? null : (
        <>
          <div className="flex items-center gap-2 flex-wrap">
            <Button
              type="button"
              variant="outline"
              size="sm"
              className={`h-7 w-[112px] text-[10px] justify-center ${activeCategory === "all" ? "bg-primary/10 ring-1 ring-primary font-semibold" : ""}`}
              onClick={() => onCategoryChange("all")}
            >
              All ({counts.actual + counts.potential})
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className={`h-7 w-[112px] text-[10px] justify-center gap-1 ${activeCategory === "actual" ? "bg-primary/10 ring-1 ring-primary font-semibold" : ""}`}
              onClick={() => onCategoryChange("actual")}
            >
              <CheckCircle2 className="h-3 w-3" />
              Actual ({counts.actual})
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className={`h-7 w-[112px] text-[10px] justify-center gap-1 ${activeCategory === "potential" ? "bg-primary/10 ring-1 ring-primary font-semibold" : ""}`}
              onClick={() => onCategoryChange("potential")}
            >
              <Sparkles className="h-3 w-3" />
              Potential ({counts.potential})
            </Button>
          </div>

          <div className="flex items-center gap-1.5 min-w-[260px]">
            <div className="relative flex-1 min-w-[180px]">
              <Search className="absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={searchQuery}
                onChange={(event) => onSearchChange(event.target.value)}
                onKeyDown={onSearchKeyDown}
                className="h-7 pl-7 text-xs"
                placeholder="Search annotations..."
              />
              {searchQuery && (
                <button
                  type="button"
                  aria-label="Clear annotation search"
                  className="absolute right-2 top-1/2 -translate-y-1/2"
                  onClick={onClearSearch}
                >
                  <X className="h-3.5 w-3.5 text-muted-foreground" />
                </button>
              )}
            </div>
            <Button
              variant="outline"
              size="icon"
              className="h-7 w-7"
              disabled={matches.length === 0}
              onClick={onPrevious}
            >
              <ChevronLeft className="h-3.5 w-3.5" />
            </Button>
            <Button
              variant="outline"
              size="icon"
              className="h-7 w-7"
              disabled={matches.length === 0}
              onClick={onNext}
            >
              <ChevronRight className="h-3.5 w-3.5" />
            </Button>
            <span className="text-[10px] text-muted-foreground min-w-[70px] text-right">
              {matches.length === 0 ? "0 matches" : `${effectiveMatchIndex + 1}/${matches.length}`}
            </span>
          </div>

          <div ref={resultsListRef} className="rounded-md border bg-background max-h-[160px] overflow-y-auto p-1">
            {matches.length === 0 ? (
              <div className="h-[70px] flex items-center justify-center text-[11px] text-muted-foreground">
                No annotations found
              </div>
            ) : (
              <div className="space-y-1">
                {matches.map((annotation, index) => {
                  const isSelected = index === selectedMatchIndex;
                  const isCanvasHovered = hoveredAnnotationId === annotation.id;
                  return (
                    <button
                      key={`navigator-match-${annotation.id}-${index}`}
                      type="button"
                      data-match-index={index}
                      className={`w-full text-left rounded px-2 py-1.5 border transition-colors ${
                        isSelected
                          ? "border-sky-300 bg-sky-50"
                          : isCanvasHovered
                            ? "border-sky-200 bg-sky-50/70"
                          : "border-transparent hover:border-border hover:bg-muted/60"
                      }`}
                      onMouseEnter={() => onResultHover(annotation.id)}
                      onMouseLeave={() => onResultHover(null)}
                      onClick={() => onResultClick(index)}
                      onDoubleClick={() => onResultDoubleClick(index)}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <span className="text-xs font-medium truncate">{annotation.text}</span>
                        <Badge variant={annotation.isActual ? "success" : "warning"} className="text-[9px]">
                          {annotation.isActual ? "Actual" : "Potential"}
                        </Badge>
                      </div>
                      <div className="text-[10px] text-muted-foreground truncate">
                        {annotation.resourceType || "No type"}
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}
