import type { MessageKey } from "../../i18n";
import {
  FlowToolbarGroup,
  FlowToolbarIconButton,
  FlowToolbarOpenExternalIcon,
  FlowToolbarRefreshIcon,
} from "./FlowToolbarIcons";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  refreshLabelKey: MessageKey;
  onRefresh: () => void;
  refreshDisabled?: boolean;
  openInTransform?: {
    labelKey: MessageKey;
    busyLabelKey: MessageKey;
    hintKey: MessageKey;
    busy: boolean;
    disabled?: boolean;
    onClick: () => void;
  };
};

/** Icon actions for data model / workflow document toolbars. */
export function FlowDocToolbarActions({
  t,
  refreshLabelKey,
  onRefresh,
  refreshDisabled = false,
  openInTransform,
}: Props) {
  return (
    <FlowToolbarGroup label={t("flow.toolbar.docActions")} className="disc-flow-doc-actions">
      <FlowToolbarIconButton
        label={t(refreshLabelKey)}
        disabled={refreshDisabled}
        onClick={onRefresh}
      >
        <FlowToolbarRefreshIcon />
      </FlowToolbarIconButton>
      {openInTransform ? (
        <FlowToolbarIconButton
          label={
            openInTransform.busy
              ? t(openInTransform.busyLabelKey)
              : t(openInTransform.labelKey)
          }
          primary
          disabled={openInTransform.disabled || openInTransform.busy}
          onClick={openInTransform.onClick}
        >
          <FlowToolbarOpenExternalIcon />
        </FlowToolbarIconButton>
      ) : null}
    </FlowToolbarGroup>
  );
}
