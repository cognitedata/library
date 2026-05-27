export const DOCUMENT_TAB_PANEL_ID = "disc-document-tabpanel";

export function documentTabButtonId(tabId: string): string {
  return `disc-tab-${tabId}`;
}

export function documentTabPanelIdForTab(tabId: string): string {
  return `${DOCUMENT_TAB_PANEL_ID}-${tabId}`;
}
