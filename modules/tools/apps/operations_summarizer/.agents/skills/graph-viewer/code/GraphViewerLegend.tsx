import * as LucideIcons from "lucide-react";
import type { NodeTypeInfo } from "./types";
import { getIconForType } from "./types";

type LucideIconComponent = React.ComponentType<{
  size?: number;
  color?: string;
  strokeWidth?: number;
  className?: string;
}>;

function getLucideIcon(iconName: string): LucideIconComponent {
  const icons = LucideIcons as unknown as Record<string, LucideIconComponent>;
  const icon = icons[iconName];
  if (!icon) {
    console.warn(`[getLucideIcon] Icon "${iconName}" not found, using Circle`);
  }
  return icon || icons.Circle;
}

const POSITION_CLASSES = {
  "bottom-left": "bottom-4 left-4",
  "bottom-right": "bottom-4 right-4",
  "top-left": "top-4 left-4",
  "top-right": "top-4 right-4",
};

export interface LegendProps {
  nodeTypes: NodeTypeInfo[];
  selectedNodeType: string | null;
  onNodeTypeClick: (typeKey: string) => void;
  onClearSelection: () => void;
  maxVisibleTypes?: number;
  position?: keyof typeof POSITION_CLASSES;
}

export function GraphViewerLegend({
  nodeTypes,
  selectedNodeType,
  onNodeTypeClick,
  onClearSelection,
  maxVisibleTypes = 12,
  position = "bottom-left",
}: LegendProps) {
  if (!nodeTypes || nodeTypes.length === 0) {
    return null;
  }

  const visibleTypes = nodeTypes.slice(0, maxVisibleTypes);
  const remainingCount = nodeTypes.length - maxVisibleTypes;

  return (
    <div
      className={`absolute ${POSITION_CLASSES[position]} bg-white/95 dark:bg-gray-900/95 backdrop-blur-sm border border-gray-200 dark:border-gray-700 rounded-xl shadow-lg dark:shadow-gray-950/50 p-3 max-w-sm z-10`}
    >
      <p className="text-xs font-semibold text-gray-700 dark:text-gray-200 mb-2 flex items-center gap-2">
        <span className="w-1.5 h-1.5 rounded-full bg-[#00205B] dark:bg-primary animate-pulse" />
        Node Types ({nodeTypes.length})
        {selectedNodeType && (
          <button
            type="button"
            onClick={onClearSelection}
            className="ml-auto text-gray-400 hover:text-gray-600 dark:text-gray-500 dark:hover:text-gray-300 transition-colors"
            title="Clear filter"
          >
            ✕
          </button>
        )}
      </p>
      <div className="flex flex-wrap gap-2">
        {visibleTypes.map((nodeType) => {
          const iconName = getIconForType(nodeType.externalId);
          const IconComponent = getLucideIcon(iconName);
          const typeKey = `${nodeType.space}:${nodeType.externalId}`;
          const isSelected = selectedNodeType === typeKey;

          return (
            <button
              type="button"
              key={typeKey}
              onClick={() => onNodeTypeClick(typeKey)}
              className={`flex items-center gap-1.5 px-2 py-1.5 rounded-lg transition-all cursor-pointer border ${
                isSelected
                  ? "bg-[#00205B] border-[#00205B] dark:bg-primary dark:border-primary shadow-md"
                  : "bg-gray-50 border-gray-200 hover:bg-gray-100 hover:border-gray-300 dark:bg-gray-800 dark:border-gray-600 dark:hover:bg-gray-700 dark:hover:border-gray-500"
              }`}
              title={`Click to highlight ${nodeType.externalId} nodes`}
            >
              <div className="w-5 h-5 relative shrink-0">
                <div
                  className="absolute inset-0 rounded-full shadow-sm"
                  style={{ backgroundColor: nodeType.color }}
                />
                <div className="absolute inset-0 flex items-center justify-center">
                  <IconComponent size={11} className="shrink-0" color="#ffffff" strokeWidth={2.5} />
                </div>
              </div>
              <span
                className={`text-xs truncate max-w-[80px] font-medium ${
                  isSelected ? "text-white" : "text-gray-700 dark:text-gray-200"
                }`}
                title={nodeType.externalId}
              >
                {nodeType.externalId}
              </span>
              <span
                className={`text-xs tabular-nums ${isSelected ? "text-gray-300 dark:text-white/70" : "text-gray-400 dark:text-gray-500"}`}
              >
                {nodeType.count}
              </span>
            </button>
          );
        })}
        {remainingCount > 0 && (
          <span className="text-xs text-gray-400 dark:text-gray-500 px-2 py-1">
            +{remainingCount} more
          </span>
        )}
      </div>
    </div>
  );
}
