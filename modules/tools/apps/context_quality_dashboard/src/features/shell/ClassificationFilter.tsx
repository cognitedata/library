import { FilterSelect } from '../issues/IssuePatternFilters';

import {
  classificationKindLabel,
  classificationKinds,
  classificationValues,
  type Classified,
  type ClassificationFilterState,
} from './classificationFilterModel';

type ClassificationFilterProps = {
  items: readonly Classified[];
  filter: ClassificationFilterState;
  onChange: (filter: ClassificationFilterState) => void;
  t: (key: string) => string;
};

/** Pick a classification kind, then one of its values. Renders nothing when no item is classified. */
export function ClassificationFilter({ items, filter, onChange, t }: ClassificationFilterProps) {
  const kinds = classificationKinds(items);
  if (kinds.length === 0) {
    return null;
  }
  const values = filter.kind === undefined ? [] : classificationValues(items, filter.kind);

  return (
    <div className="flex flex-wrap items-end gap-3" data-testid="classification-filter">
      <FilterSelect
        label={t('classificationFilter.kind')}
        value={filter.kind ?? '__all__'}
        options={kinds.map((kind) => ({ id: kind, label: classificationKindLabel(kind, t) }))}
        allLabel={t('filter.all')}
        onValueChange={(next) => onChange({ kind: next === '__all__' ? undefined : next, value: undefined })}
      />
      {filter.kind === undefined ? null : (
        <FilterSelect
          label={t('classificationFilter.value')}
          value={filter.value ?? '__all__'}
          options={values.map((option) => ({ id: option.value, label: `${option.value} (${option.count})` }))}
          allLabel={t('filter.all')}
          onValueChange={(next) => onChange({ kind: filter.kind, value: next === '__all__' ? undefined : next })}
        />
      )}
    </div>
  );
}
