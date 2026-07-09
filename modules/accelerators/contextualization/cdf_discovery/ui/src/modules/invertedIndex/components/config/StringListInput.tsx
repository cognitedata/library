import { useEffect, useRef, useState } from "react";

type Props = {
  value: string[];
  onChange: (next: string[]) => void;
  placeholder?: string;
  id?: string;
  mono?: boolean;
};

function parseList(text: string): string[] {
  return text
    .split(/[,;\n]/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function formatList(value: string[]): string {
  return value.join(", ");
}

export function StringListInput({ value, onChange, placeholder, id, mono }: Props) {
  const committedText = formatList(value);
  const [draft, setDraft] = useState(committedText);
  const focusedRef = useRef(false);

  useEffect(() => {
    if (!focusedRef.current) {
      setDraft(committedText);
    }
  }, [committedText]);

  const commit = (text: string) => {
    const parsed = parseList(text);
    onChange(parsed);
    setDraft(formatList(parsed));
  };

  return (
    <input
      id={id}
      className={`idx-input${mono ? " idx-input--mono" : ""}`}
      value={draft}
      placeholder={placeholder}
      onFocus={() => {
        focusedRef.current = true;
      }}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={(e) => {
        focusedRef.current = false;
        commit(e.target.value);
      }}
      onKeyDown={(e) => {
        if (e.key === "Enter") {
          e.currentTarget.blur();
        }
      }}
    />
  );
}
