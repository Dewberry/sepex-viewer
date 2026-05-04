export const STORAGE_KEY = "sepex-viewer:builder-templates:v1";

export function loadSavedTemplates() {
  if (typeof window === "undefined") return [];
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
  } catch {
    return [];
  }
}

export function persistSavedTemplates(templates) {
  if (typeof window === "undefined") return;
  localStorage.setItem(STORAGE_KEY, JSON.stringify(templates));
}
