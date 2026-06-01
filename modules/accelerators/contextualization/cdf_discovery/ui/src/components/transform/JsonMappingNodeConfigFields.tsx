import { json } from "@codemirror/lang-json";
import { javascript } from "@codemirror/lang-javascript";
import { oneDark } from "@codemirror/theme-one-dark";
import type { EditorView } from "@codemirror/view";
import CodeMirror, { type ReactCodeMirrorRef } from "@uiw/react-codemirror";
import { useCallback, useMemo, useRef, useState, type ReactNode } from "react";
import type { Edge, Node } from "@xyflow/react";
import { QueryEditorTabs, useQueryEditorTabState, type QueryEditorTabDef } from "../query/QueryEditorTabs";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n";
import type { JsonObject } from "../../types/jsonConfig";
import {
  listDataPredecessorTasks,
  workflowOutputRef,
  type CanvasPredecessorContext,
} from "../../utils/canvasPredecessorTasks";
import {
  applyJsonMappingTemplate,
  applyMapperKindTemplate,
  JSON_MAPPING_TEMPLATES,
  parseJsonMappingInputText,
  readJsonMappingExpression,
  readJsonMappingInput,
  readMapperKind,
  validateJsonMappingConfig,
  type JsonMappingMapperKind,
  type JsonMappingValidationIssue,
} from "../../utils/jsonMappingNodeConfigModel";

const JSON_MAPPING_DOCS_URL =
  "https://docs.cognite.com/cdf/data_workflows/task_types#json-mapping-tasks";

const TAB_SETUP = "setup";
const TAB_INPUT = "input";
const TAB_EXPRESSION = "expression";

const JSON_MAPPING_TABS: QueryEditorTabDef[] = [
  { id: TAB_SETUP, labelKey: "transform.jsonMapping.tabSetup" },
  { id: TAB_INPUT, labelKey: "transform.jsonMapping.tabInput" },
  { id: TAB_EXPRESSION, labelKey: "transform.jsonMapping.tabExpression" },
];

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  nodeId: string;
  flowNodes: readonly Node[];
  flowEdges: readonly Edge[];
};

function insertAtView(view: EditorView | undefined, text: string) {
  if (!view) return;
  const { from, to } = view.state.selection.main;
  view.dispatch({
    changes: { from, to, insert: text },
    selection: { anchor: from + text.length },
  });
  view.focus();
}

function validationMessageKey(issue: JsonMappingValidationIssue): MessageKey {
  if (issue === "expressionRequired") return "transform.jsonMapping.errorExpressionRequired";
  return "transform.jsonMapping.errorInputNotObject";
}

export function JsonMappingNodeConfigFields({ value, onChange, nodeId, flowNodes, flowEdges }: Props) {
  const { t, theme } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });

  const graphCtx = useMemo<CanvasPredecessorContext>(
    () => ({ nodes: flowNodes, edges: flowEdges, nodeId }),
    [flowNodes, flowEdges, nodeId]
  );
  const predecessors = useMemo(() => listDataPredecessorTasks(graphCtx), [graphCtx]);
  const firstPredecessorId = predecessors[0]?.taskId ?? null;

  const inputRef = useRef<ReactCodeMirrorRef>(null);
  const exprRef = useRef<ReactCodeMirrorRef>(null);

  const storedInput = readJsonMappingInput(value as Record<string, unknown>);
  const inputText = useMemo(() => JSON.stringify(storedInput, null, 2), [storedInput]);
  const expression = readJsonMappingExpression(value as Record<string, unknown>);

  const [inputParseError, setInputParseError] = useState<MessageKey | null>(null);
  const [activeTab, setActiveTab] = useQueryEditorTabState(nodeId, TAB_SETUP);

  const validationIssues = validateJsonMappingConfig(value as Record<string, unknown>);
  const cmTheme = theme === "dark" ? oneDark : "light";

  const commitInputText = useCallback(
    (text: string) => {
      const result = parseJsonMappingInputText(text);
      if (!result.ok) {
        if (result.error === "notObject") {
          setInputParseError("transform.jsonMapping.errorInputNotObject");
        } else {
          setInputParseError("transform.jsonMapping.errorInputInvalidJson");
        }
        return;
      }
      setInputParseError(null);
      patch({ input: result.value });
    },
    [patch]
  );

  const formatInput = () => {
    commitInputText(inputText);
  };

  const applyTemplate = (templateId: (typeof JSON_MAPPING_TEMPLATES)[number]["id"]) => {
    const tpl = JSON_MAPPING_TEMPLATES.find((x) => x.id === templateId);
    if (!tpl) return;
    const applied = applyJsonMappingTemplate(tpl, firstPredecessorId);
    setInputParseError(null);
    patch({ input: applied.input, expression: applied.expression });
  };

  const mapperKind = readMapperKind(value as Record<string, unknown>);

  const onMapperKindChange = (next: JsonMappingMapperKind) => {
    if (next === "custom") {
      patch({ mapper_kind: "custom" });
      return;
    }
    const applied = applyMapperKindTemplate(next, firstPredecessorId);
    setInputParseError(null);
    patch(applied.config);
  };

  let validationStrip: ReactNode = null;
  if (inputParseError || validationIssues.length > 0) {
    validationStrip = (
      <div className="transform-json-mapping-fields__errors" role="alert">
        {inputParseError ? <p>{t(inputParseError)}</p> : null}
        {validationIssues.map((issue) => (
          <p key={issue}>{t(validationMessageKey(issue))}</p>
        ))}
      </div>
    );
  }

  return (
    <div className="transform-json-mapping-fields">
      <p className="transform-json-mapping-fields__notice">
        {t("transform.jsonMapping.earlyAdopterNotice")}{" "}
        <a href={JSON_MAPPING_DOCS_URL} target="_blank" rel="noopener noreferrer">
          {t("transform.jsonMapping.docsLink")}
        </a>
      </p>

      {validationStrip}

      <QueryEditorTabs
        tabs={JSON_MAPPING_TABS}
        activeTab={activeTab}
        onActiveTabChange={setActiveTab}
        ariaLabel={t("transform.query.editorTabsAria")}
        panelIdPrefix={`json-mapping-${nodeId}`}
      >
        {activeTab === TAB_SETUP ? (
          <>
            <label className="gov-label gov-label--block">
              {t("transform.jsonMapping.mapperKind")}
              <select
                className="gov-input"
                style={{ marginTop: "0.35rem" }}
                value={mapperKind}
                onChange={(e) => onMapperKindChange(e.target.value as JsonMappingMapperKind)}
              >
                <option value="custom">{t("transform.jsonMapping.mapperCustom")}</option>
                <option value="diagram_detect_to_dm">{t("transform.jsonMapping.mapperDiagramDetectToDm")}</option>
                <option value="diagram_detect_to_classic">
                  {t("transform.jsonMapping.mapperDiagramDetectToClassic")}
                </option>
              </select>
            </label>

            <label className="gov-label gov-label--block">
              {t("transform.config.description")}
              <input
                className="gov-input"
                style={{ marginTop: "0.35rem" }}
                value={String(value.description ?? "")}
                onChange={(e) => patch({ description: e.target.value })}
                spellCheck={false}
                autoComplete="off"
              />
            </label>

            <fieldset className="transform-node-editor-fields__section">
              <legend>{t("transform.jsonMapping.predecessorsTitle")}</legend>
              <p className="transform-json-mapping-fields__hint">{t("transform.jsonMapping.predecessorsHint")}</p>
              {predecessors.length === 0 ? (
                <p className="transform-json-mapping-fields__empty">{t("transform.jsonMapping.noPredecessors")}</p>
              ) : (
                <ul className="transform-json-mapping-fields__predecessors">
                  {predecessors.map((p) => {
                    const ref = workflowOutputRef(p.taskId);
                    return (
                      <li key={p.taskId} className="transform-json-mapping-fields__predecessor">
                        <span className="transform-json-mapping-fields__predecessor-label" title={p.taskId}>
                          {p.label}
                          <code className="transform-json-mapping-fields__predecessor-id">{p.taskId}</code>
                        </span>
                        <span className="transform-json-mapping-fields__predecessor-actions">
                          <button
                            type="button"
                            className="gov-btn gov-btn--secondary gov-btn--small"
                            onClick={() => insertAtView(inputRef.current?.view, ref)}
                          >
                            {t("transform.jsonMapping.insertIntoInput")}
                          </button>
                          <button
                            type="button"
                            className="gov-btn gov-btn--secondary gov-btn--small"
                            onClick={() => insertAtView(exprRef.current?.view, ref)}
                          >
                            {t("transform.jsonMapping.insertIntoExpression")}
                          </button>
                        </span>
                      </li>
                    );
                  })}
                </ul>
              )}
            </fieldset>

            <div className="transform-json-mapping-fields__templates">
              <span className="transform-json-mapping-fields__templates-label">
                {t("transform.jsonMapping.templatesLabel")}
              </span>
              <div className="transform-json-mapping-fields__template-btns">
                {JSON_MAPPING_TEMPLATES.map((tpl) => (
                  <button
                    key={tpl.id}
                    type="button"
                    className="gov-btn gov-btn--secondary gov-btn--small"
                    onClick={() => applyTemplate(tpl.id)}
                  >
                    {t(tpl.labelKey)}
                  </button>
                ))}
              </div>
            </div>
          </>
        ) : null}

        {activeTab === TAB_INPUT ? (
          <fieldset className="transform-node-editor-fields__section">
            <legend>{t("transform.jsonMapping.inputTitle")}</legend>
            <p className="transform-json-mapping-fields__hint">{t("transform.jsonMapping.inputHint")}</p>
            <div className="transform-json-mapping-fields__toolbar">
              <button type="button" className="gov-btn gov-btn--secondary gov-btn--small" onClick={formatInput}>
                {t("transform.jsonMapping.formatInput")}
              </button>
            </div>
            <CodeMirror
              ref={inputRef}
              className="transform-json-mapping-fields__editor"
              value={inputText}
              height="min(28vh, 14rem)"
              theme={cmTheme}
              extensions={[json()]}
              basicSetup={{ lineNumbers: true, foldGutter: true }}
              onChange={(next) => commitInputText(next)}
            />
          </fieldset>
        ) : null}

        {activeTab === TAB_EXPRESSION ? (
          <fieldset className="transform-node-editor-fields__section">
            <legend>{t("transform.jsonMapping.expressionTitle")}</legend>
            <p className="transform-json-mapping-fields__hint">{t("transform.jsonMapping.expressionHint")}</p>
            <CodeMirror
              ref={exprRef}
              className="transform-json-mapping-fields__editor"
              value={expression}
              height="min(22vh, 11rem)"
              theme={cmTheme}
              extensions={[javascript()]}
              basicSetup={{ lineNumbers: false, foldGutter: false }}
              onChange={(next) => patch({ expression: next })}
            />
          </fieldset>
        ) : null}
      </QueryEditorTabs>
    </div>
  );
}
