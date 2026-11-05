export function buildFilesUploadLinkPath(project: string): string {
  return `/api/v1/projects/${project}/files/uploadlink`;
}
