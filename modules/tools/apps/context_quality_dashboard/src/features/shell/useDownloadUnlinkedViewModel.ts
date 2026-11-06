import { useMutation } from '@tanstack/react-query';

import { useExportService } from '../../services/useServices';

/** Fetches the workbook link of the latest run and starts the browser download. `data === false` means no workbook exists. */
export function useDownloadUnlinkedViewModel() {
  const exportService = useExportService();
  return useMutation({
    mutationFn: async () => {
      const url = await exportService.getUnlinkedWorkbookUrl();
      if (url === null) {
        return false;
      }
      const link = document.createElement('a');
      link.href = url;
      link.rel = 'noopener';
      document.body.appendChild(link);
      link.click();
      link.remove();
      return true;
    },
  });
}
