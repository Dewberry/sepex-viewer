import { useState } from "react";
import {
  loadSavedTemplates,
  persistSavedTemplates
} from "@/app/(dashboard)/builder/_utils/storage";

function generateTemplateId() {
  if (typeof crypto !== "undefined" && crypto.randomUUID) {
    return `tpl_${crypto.randomUUID()}`;
  }
  return `tpl_${Math.random().toString(36).slice(2)}`;
}

export default function useSavedTemplates() {
  const [templates, setTemplates] = useState(() => loadSavedTemplates());

  const save = ({ name, processID, inputs, tags }) => {
    if (templates.some((t) => t.name === name)) {
      throw new Error(
        "A template with that name already exists. Pick a different name."
      );
    }
    const template = {
      id: generateTemplateId(),
      name,
      processID: processID || "",
      inputs: { ...inputs },
      tags: [...(tags || [])],
      savedAt: new Date().toISOString()
    };
    const next = [template, ...templates];
    setTemplates(next);
    persistSavedTemplates(next);
    return template;
  };

  const remove = (id) => {
    const next = templates.filter((t) => t.id !== id);
    setTemplates(next);
    persistSavedTemplates(next);
  };

  return { templates, save, remove };
}
