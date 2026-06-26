import type { ReactNode } from "react";
import type { TreeNode } from "../types/discoveryNodes";
import {
  DATA_ROOT,
  EXTRACT_ROOT,
  FUSION_ROOT,
  GOVERNANCE_ROOT,
  MONITOR_ROOT,
  TRANSFORM_ROOT,
} from "../utils/treeNodeIds";

type Props = {
  node: Pick<TreeNode, "id" | "kind" | "has_children">;
  className?: string;
};

function Svg({ className, children }: { className?: string; children: ReactNode }) {
  return (
    <svg
      className={className}
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden
    >
      {children}
    </svg>
  );
}

function iconForNode(node: Pick<TreeNode, "id" | "kind" | "has_children">) {
  switch (node.id) {
    case DATA_ROOT:
      return (
        <Svg>
          <ellipse cx="12" cy="5" rx="8" ry="3" />
          <path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5" />
          <path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6" />
        </Svg>
      );
    case FUSION_ROOT:
      return (
        <Svg>
          <path d="M18 10h-4V6a2 2 0 0 0-4 0v4H6a2 2 0 0 0 0 4h4v4a2 2 0 0 0 4 0v-4h4a2 2 0 0 0 0-4z" />
        </Svg>
      );
    case GOVERNANCE_ROOT:
      return (
        <Svg>
          <path d="M12 3 4 7v6c0 4.4 3.4 8.5 8 9.5 4.6-1 8-5.1 8-9.5V7l-8-4z" />
        </Svg>
      );
    case EXTRACT_ROOT:
      return (
        <Svg>
          <path d="M12 3v12" />
          <path d="m7 10 5 5 5-5" />
          <path d="M5 21h14" />
        </Svg>
      );
    case TRANSFORM_ROOT:
      return (
        <Svg>
          <circle cx="6" cy="6" r="2.5" />
          <circle cx="18" cy="6" r="2.5" />
          <circle cx="12" cy="18" r="2.5" />
          <path d="M8.2 7.5 10.8 15" />
          <path d="M15.8 7.5 13.2 15" />
        </Svg>
      );
    case MONITOR_ROOT:
      return (
        <Svg>
          <path d="M4 19h16" />
          <path d="M7 15V9" />
          <path d="M12 15V5" />
          <path d="M17 15v-4" />
        </Svg>
      );
    default:
      break;
  }

  switch (node.kind) {
    case "saved_query":
      return (
        <Svg>
          <path d="M8 6h8" />
          <path d="M8 10h8" />
          <path d="M8 14h5" />
          <rect x="4" y="4" width="16" height="16" rx="2" />
        </Svg>
      );
    case "workflow":
    case "etl_pipeline":
    case "etl_workflow_yaml":
      return (
        <Svg>
          <rect x="3" y="5" width="6" height="6" rx="1" />
          <rect x="15" y="5" width="6" height="6" rx="1" />
          <rect x="9" y="13" width="6" height="6" rx="1" />
          <path d="M9 8h6" />
          <path d="M12 11v2" />
        </Svg>
      );
    case "transformation":
    case "function":
      return (
        <Svg>
          <path d="M8 4 4 8l4 4" />
          <path d="M4 8h10a4 4 0 0 1 0 8H8" />
        </Svg>
      );
    case "dm_data_model":
    case "dm_view":
    case "fusion_dm_view":
      return (
        <Svg>
          <path d="M4 7h16" />
          <path d="M6 7V5h12v2" />
          <rect x="6" y="9" width="12" height="10" rx="1" />
        </Svg>
      );
    case "raw_database":
    case "raw_table":
      return (
        <Svg>
          <ellipse cx="12" cy="6" rx="7" ry="2.5" />
          <path d="M5 6v4c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5V6" />
          <path d="M5 10v4c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5v-4" />
        </Svg>
      );
    case "classic_resource":
      return (
        <Svg>
          <path d="M4 6h16v12H4z" />
          <path d="M8 10h8" />
          <path d="M8 14h5" />
        </Svg>
      );
    case "record_stream":
      return (
        <Svg>
          <path d="M4 7h16" />
          <path d="M4 12h10" />
          <path d="M4 17h14" />
        </Svg>
      );
    case "etl_template":
      return (
        <Svg>
          <rect x="5" y="4" width="14" height="16" rx="2" />
          <path d="M9 9h6" />
          <path d="M9 13h6" />
        </Svg>
      );
    case "gov_artifact_file":
    case "gov_space":
    case "gov_group":
      return (
        <Svg>
          <path d="M7 4h7l3 3v13H7z" />
          <path d="M14 4v4h4" />
        </Svg>
      );
    default:
      if (node.has_children) {
        return (
          <Svg>
            <path d="M4 7h16" />
            <path d="M4 7v11h16V7" />
            <path d="M9 7V5h6v2" />
          </Svg>
        );
      }
      return (
        <Svg>
          <path d="M8 4h8l2 2v14H6V4z" />
          <path d="M8 4v4h8" />
        </Svg>
      );
  }
}

export function TreeNavIcon({ node, className }: Props) {
  return <span className={className}>{iconForNode(node)}</span>;
}
