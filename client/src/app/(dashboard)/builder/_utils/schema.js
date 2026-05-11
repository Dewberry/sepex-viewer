import { z } from "zod";

// Sepex uses "value" as a catch-all for any literal; in practice all the
// processes that use it (fdt, pyecho) expect a free-form string. Treat it
// the same as "string" so the user gets a text input instead of a JSON
// textarea demanding quoted strings.
export const isTextLike = (dataType) =>
  dataType === "string" || dataType === "value";

export const isMultiOccurrence = (input) => (input.maxOccurs ?? 1) > 1;

function fieldFor(input) {
  const dataType = input.input?.literalDataDomain?.dataType;
  const required = (input.minOccurs || 0) > 0;
  const multi = isMultiOccurrence(input);

  if (multi) {
    // Multi-occurrence is rendered as a multi-line textarea (one value per
    // line); the form stores string[]. Integer arrays get coerced in
    // buildPayload at the submission boundary.
    const arr = z.array(z.string().min(1));
    return required ? arr.min(1) : arr.optional();
  }

  if (dataType === "integer") {
    const numeric = z.coerce.number();
    return required ? numeric : numeric.optional();
  }

  if (isTextLike(dataType)) {
    const str = z.string();
    return required ? str.min(1) : str.optional();
  }

  // object / unknown — kept as JSON textarea, validated leniently
  return required
    ? z.any().refine((v) => v != null && v !== "")
    : z.any().optional();
}

export function buildZodSchema(processInputs) {
  if (!processInputs?.length) return z.object({});
  const shape = {};
  for (const input of processInputs) {
    shape[input.id] = fieldFor(input);
  }
  return z.object(shape);
}

export function buildPayload(processDetail, values, tags) {
  const payload = { inputs: {} };
  const inputs = processDetail?.inputs || [];
  const inputMap = new Map(inputs.map((i) => [i.id, i]));
  const sourceIds = inputs.length
    ? inputs.map((i) => i.id)
    : Object.keys(values || {});

  for (const id of sourceIds) {
    const v = values?.[id];
    if (v === undefined || v === null || v === "") continue;
    if (Array.isArray(v) && v.length === 0) continue;

    const meta = inputMap.get(id);
    const dataType = meta?.input?.literalDataDomain?.dataType;
    const multi = meta ? isMultiOccurrence(meta) : false;

    if (multi && dataType === "integer" && Array.isArray(v)) {
      payload.inputs[id] = v.map((x) => Number(x));
    } else {
      payload.inputs[id] = v;
    }
  }
  if (tags?.length) payload.tags = tags;
  return payload;
}

export function buildYaml(payload) {
  const lines = ["inputs:"];
  for (const [k, v] of Object.entries(payload.inputs || {})) {
    lines.push(`  ${k}: ${JSON.stringify(v)}`);
  }
  if (payload.tags?.length) {
    lines.push("tags:");
    for (const t of payload.tags) lines.push(`  - ${t}`);
  }
  return lines.join("\n");
}
