/** Sample run configuration shipped with the dashboard; must stay valid per `parseRunConfigFile`. */
export const RUN_CONFIG_FILE_EXAMPLE_FILENAME = 'context-quality-run-config.example.yaml';

export const RUN_CONFIG_FILE_EXAMPLE_YAML = `# List of data models and View.property links to measure
- model: cdf_cdm:CogniteCore:v1
  links:
    - CogniteAsset.type
    - CogniteEquipment.equipmentType
- model: my_space:MyModel:v2
  links:
    - MyView.owner
`;

export function downloadRunConfigExampleFile(): void {
  const blob = new Blob([RUN_CONFIG_FILE_EXAMPLE_YAML], { type: 'text/yaml;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = RUN_CONFIG_FILE_EXAMPLE_FILENAME;
  link.rel = 'noopener';
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}
