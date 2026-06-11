import { useQuery } from "@tanstack/react-query";
import type { CogniteClient } from "@cognite/sdk";
import { isLocalMockMode } from "@/runtime/authMode";
import {
  getLocalFileInfo,
  getLocalFilePageCount,
  getLocalFilePreview,
} from "@/mocks/mockData";

interface FileInfo {
  id: number;
  externalId?: string;
  name?: string;
  mimeType?: string;
}

/**
 * Fetch file info from CDF
 * @param sdk - Cognite SDK client
 * @param fileExternalId - The DMS external ID of the file
 * @param fileName - Optional: the actual file name (if known, skips DMS lookup)
 */
async function fetchFileInfo(
  sdk: CogniteClient,
  fileExternalId: string,
  fileName?: string
): Promise<FileInfo | null> {
  console.log("Looking up file:", fileExternalId, "fileName:", fileName);

  // Strategy 1: Try to get file directly by external ID from Files API
  try {
    const files = await sdk.files.retrieve([{ externalId: fileExternalId }]);
    if (files.length > 0) {
      console.log("Found file by externalId:", files[0].id);
      return {
        id: files[0].id,
        externalId: files[0].externalId,
        name: files[0].name,
        mimeType: files[0].mimeType,
      };
    }
  } catch {
    console.log("File not found by externalId, trying name search...");
  }

  // Strategy 2: Search by fileName if provided (from DMS data we already have)
  if (fileName) {
    try {
      console.log("Searching for file by name:", fileName);
      const searchResults = await sdk.files.search({
        filter: { name: fileName },
        limit: 5,
      });
      // Find exact match
      const exactMatch = searchResults.find(f => f.name === fileName);
      if (exactMatch) {
        console.log("Found file by exact name match:", exactMatch.id, "name:", exactMatch.name);
        const result = {
          id: exactMatch.id,
          externalId: exactMatch.externalId,
          name: exactMatch.name,
          mimeType: exactMatch.mimeType,
        };
        console.log("Returning fileInfo:", result);
        return result;
      }
      // Fall back to first result
      if (searchResults.length > 0) {
        console.log("Found file by name search (first result):", searchResults[0].id);
        return {
          id: searchResults[0].id,
          externalId: searchResults[0].externalId,
          name: searchResults[0].name,
          mimeType: searchResults[0].mimeType,
        };
      }
    } catch (e) {
      console.warn("File search by name failed:", e);
    }
  }

  // Strategy 3: Try searching by the externalId as name (last resort)
  try {
    console.log("Last resort: searching by externalId as name:", fileExternalId);
    const searchResults = await sdk.files.search({
      filter: { name: fileExternalId },
      limit: 1,
    });
    if (searchResults.length > 0) {
      console.log("Found file by externalId name search:", searchResults[0].id);
      return {
        id: searchResults[0].id,
        externalId: searchResults[0].externalId,
        name: searchResults[0].name,
        mimeType: searchResults[0].mimeType,
      };
    }
  } catch {
    console.warn("File search by externalId failed");
  }

  console.warn("Could not find file:", fileExternalId);
  return null;
}

/**
 * Fetch file preview/download URL
 */
async function fetchFilePreviewUrl(
  sdk: CogniteClient,
  fileInfo: FileInfo,
  page: number = 1
): Promise<{ url: string; type: "image" | "pdf" | "download" } | null> {
  const isPdf = fileInfo.mimeType?.includes("pdf");

  // Strategy 1: Try to find the document by name and use documentAsImage (handles auth)
  console.log("fileInfo for preview:", fileInfo);
  const searchName = fileInfo.name;
  
  if (searchName) {
    try {
      console.log("Looking up document by source file name:", searchName);
      const documentsResult = await sdk.documents.search({
        filter: {
          equals: {
            property: ["sourceFile", "name"],
            value: searchName,
          },
        },
        limit: 1,
      });

      if (documentsResult.items.length > 0) {
        // DocumentSearchItem structure may vary - access id from the item
        const docItem = documentsResult.items[0] as unknown as { id?: number; document?: { id: number } };
        const documentId = docItem.id ?? docItem.document?.id;
        
        if (documentId) {
          console.log("Found document:", documentId, "for file:", searchName);

          // Use documentAsImage which handles auth - returns ArrayBuffer
          const imageData = await sdk.documents.preview.documentAsImage(documentId, page);
          const blob = new Blob([imageData], { type: "image/png" });
          const url = URL.createObjectURL(blob);
          console.log("Created blob URL from documentAsImage");
          return { url, type: "image" };
        } else {
          console.log("Document found but no ID available:", documentsResult.items[0]);
        }
      } else {
        console.log("No document found for file name:", searchName);
      }
    } catch (error) {
      console.log("Document search/preview failed:", error);
    }
  }

  // Strategy 1b: Try direct documentAsImage with file ID (in case file ID = document ID)
  try {
    console.log("Trying documentAsImage with file ID:", fileInfo.id);
    const imageData = await sdk.documents.preview.documentAsImage(fileInfo.id, page);
    const blob = new Blob([imageData], { type: "image/png" });
    const url = URL.createObjectURL(blob);
    console.log("Created blob URL from documentAsImage (direct file ID)");
    return { url, type: "image" };
  } catch (error) {
    console.log("documentAsImage failed, trying file download...", error);
  }

  // Strategy 2: For PDFs, fetch as blob and create object URL to avoid download
  if (isPdf) {
    try {
      const downloadUrls = await sdk.files.getDownloadUrls([{ id: fileInfo.id }]);
      if (downloadUrls.length > 0 && downloadUrls[0].downloadUrl) {
        console.log("Fetching PDF as blob...");
        const response = await fetch(downloadUrls[0].downloadUrl);
        if (response.ok) {
          const arrayBuffer = await response.arrayBuffer();
          // Create blob with explicit PDF MIME type
          const blob = new Blob([arrayBuffer], { type: "application/pdf" });
          const url = URL.createObjectURL(blob);
          console.log("Created blob URL for PDF with explicit MIME type");
          return { url, type: "pdf" };
        }
      }
    } catch (error) {
      console.warn("Failed to fetch PDF as blob:", error);
    }
  }

  // Strategy 3: For images, try to fetch and display directly
  if (fileInfo.mimeType?.startsWith("image/")) {
    try {
      const downloadUrls = await sdk.files.getDownloadUrls([{ id: fileInfo.id }]);
      if (downloadUrls.length > 0 && downloadUrls[0].downloadUrl) {
        console.log("Fetching image as blob...");
        const response = await fetch(downloadUrls[0].downloadUrl);
        if (response.ok) {
          const blob = await response.blob();
          const url = URL.createObjectURL(blob);
          console.log("Created blob URL for image");
          return { url, type: "image" };
        }
      }
    } catch (error) {
      console.warn("Failed to fetch image as blob:", error);
    }
  }

  // Strategy 4: Fallback - return download URL (will trigger download)
  try {
    const downloadUrls = await sdk.files.getDownloadUrls([{ id: fileInfo.id }]);
    if (downloadUrls.length > 0 && downloadUrls[0].downloadUrl) {
      console.log("Falling back to download URL");
      return {
        url: downloadUrls[0].downloadUrl,
        type: "download",
      };
    }
  } catch (error) {
    console.warn("Failed to get download URL:", error);
  }

  return null;
}

/**
 * Get page count for a document
 * Note: Page count detection is limited - we return 1 by default
 */
async function fetchFilePageCount(
  _sdk: CogniteClient,
  _fileId: number
): Promise<number> {
  // Page count detection would require document content parsing
  // For now, return 1 as a default
  return 1;
}

/**
 * Hook to fetch file info from CDF
 * @param sdk - Cognite SDK client
 * @param fileExternalId - The DMS external ID of the file
 * @param fileName - Optional: the actual file name (if known, used for search)
 */
export function useFileCdfId(
  sdk: CogniteClient | null,
  fileExternalId: string | null,
  fileName?: string
) {
  return useQuery({
    queryKey: ["fileInfo", fileExternalId, fileName],
    queryFn: async () => {
      if (isLocalMockMode) {
        return Promise.resolve(fileExternalId ? getLocalFileInfo(fileExternalId, fileName) : null);
      }
      if (!sdk || !fileExternalId) return null;
      return fetchFileInfo(sdk, fileExternalId, fileName);
    },
    enabled: (isLocalMockMode && !!fileExternalId) || (!!sdk && !!fileExternalId),
    staleTime: 30 * 60 * 1000,
  });
}

/**
 * Hook to fetch file preview
 */
export function useFilePreview(
  sdk: CogniteClient | null,
  fileId: number | null,
  page: number = 1,
  mimeType?: string
) {
  return useQuery({
    queryKey: ["filePreview", fileId, page],
    queryFn: async () => {
      if (isLocalMockMode) {
        return Promise.resolve(getLocalFilePreview(fileId, page));
      }
      if (!sdk || !fileId) return null;
      return fetchFilePreviewUrl(sdk, { id: fileId, mimeType }, page);
    },
    enabled: (isLocalMockMode && !!fileId) || (!!sdk && !!fileId),
    staleTime: 10 * 60 * 1000,
  });
}

/**
 * Hook to fetch file page count
 */
export function useFilePageCount(
  sdk: CogniteClient | null,
  fileId: number | null
) {
  return useQuery({
    queryKey: ["filePageCount", fileId],
    queryFn: async () => {
      if (isLocalMockMode) {
        return Promise.resolve(getLocalFilePageCount(fileId));
      }
      if (!sdk || !fileId) return 1;
      return fetchFilePageCount(sdk, fileId);
    },
    enabled: (isLocalMockMode && !!fileId) || (!!sdk && !!fileId),
    staleTime: 30 * 60 * 1000,
  });
}
