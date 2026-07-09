import type { ReactNode } from "react";

type Props = {
  parameters: ReactNode;
  results: ReactNode;
  between?: ReactNode;
};

export function EditorWorkflowLayout({ parameters, results, between }: Props) {
  return (
    <div className="idx-editor-workflow">
      <div className="idx-editor-workflow__stage idx-editor-workflow__stage--input">{parameters}</div>
      {between ? <div className="idx-editor-workflow__between">{between}</div> : null}
      <div className="idx-editor-workflow__stage idx-editor-workflow__stage--output">{results}</div>
    </div>
  );
}
