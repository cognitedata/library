type Props = {
  /** Shown when `value` is empty (technical / nav name). */
  fallbackName: string;
  value: string;
  onChange: (next: string) => void;
  readOnly?: boolean;
  ariaLabel: string;
};

/**
 * Optional top-level `name`: reads like static text until focused. When unset, shows `fallbackName`.
 */
export function DisplayNameField({ fallbackName, value, onChange, readOnly, ariaLabel }: Props) {
  return (
    <div className="discovery-config-display-name">
      <input
        type="text"
        className="discovery-input discovery-input--display-name"
        aria-label={ariaLabel}
        placeholder={fallbackName}
        value={value}
        readOnly={readOnly}
        aria-readonly={readOnly || undefined}
        onChange={(e) => onChange(e.target.value)}
      />
    </div>
  );
}
