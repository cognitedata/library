import {
  useEffect,
  useRef,
  useState,
  type FocusEvent,
  type InputHTMLAttributes,
  type TextareaHTMLAttributes,
} from "react";

type DeferredBase = {
  committedValue: string;
  /** Called on blur when the value differs from `committedValue`. */
  onCommit: (next: string) => void;
  /** When this changes, sync the draft from `committedValue` if the field is not focused. */
  syncKey?: string | number;
};

function useDeferredTextState(committedValue: string, syncKey: string | number | undefined) {
  const [draft, setDraft] = useState(committedValue);
  const focusedRef = useRef(false);
  useEffect(() => {
    if (!focusedRef.current) {
      setDraft(committedValue);
    }
  }, [committedValue, syncKey]);
  return { draft, setDraft, focusedRef };
}

export function DeferredCommitInput({
  committedValue,
  onCommit,
  syncKey,
  onFocus,
  onBlur,
  ...rest
}: DeferredBase &
  Omit<InputHTMLAttributes<HTMLInputElement>, "value" | "onChange" | "onBlur" | "onFocus"> & {
    onFocus?: (e: FocusEvent<HTMLInputElement>) => void;
    onBlur?: (e: FocusEvent<HTMLInputElement>) => void;
  }) {
  const { draft, setDraft, focusedRef } = useDeferredTextState(committedValue, syncKey);

  return (
    <input
      {...rest}
      value={draft}
      onFocus={(e) => {
        focusedRef.current = true;
        onFocus?.(e);
      }}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={(e) => {
        focusedRef.current = false;
        const next = e.target.value;
        setDraft(next);
        if (next !== committedValue) {
          onCommit(next);
        }
        onBlur?.(e);
      }}
    />
  );
}

export function DeferredCommitTextarea({
  committedValue,
  onCommit,
  syncKey,
  onFocus,
  onBlur,
  ...rest
}: DeferredBase &
  Omit<TextareaHTMLAttributes<HTMLTextAreaElement>, "value" | "onChange" | "onBlur" | "onFocus"> & {
    onFocus?: (e: FocusEvent<HTMLTextAreaElement>) => void;
    onBlur?: (e: FocusEvent<HTMLTextAreaElement>) => void;
  }) {
  const { draft, setDraft, focusedRef } = useDeferredTextState(committedValue, syncKey);

  return (
    <textarea
      {...rest}
      value={draft}
      onFocus={(e) => {
        focusedRef.current = true;
        onFocus?.(e);
      }}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={(e) => {
        focusedRef.current = false;
        const next = e.target.value;
        setDraft(next);
        if (next !== committedValue) {
          onCommit(next);
        }
        onBlur?.(e);
      }}
    />
  );
}
