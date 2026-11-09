export const FILTER_ALL = '__all__';

type SelectOption = {
  id: string;
  label: string;
};

export function toFilterSelectValue(value?: string): string {
  return value ?? FILTER_ALL;
}

export function fromFilterSelectValue(value: string): string | undefined {
  return value === FILTER_ALL ? undefined : value;
}

export function formatFilterSelectLabel(
  value: string,
  options: readonly SelectOption[],
  allLabel: string
): string {
  if (value === FILTER_ALL) {
    return allLabel;
  }

  const match = options.find((option) => option.id === value);
  return match?.label ?? allLabel;
}
