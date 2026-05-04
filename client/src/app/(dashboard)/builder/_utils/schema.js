import { z } from "zod";

// TODO: deeper schema derivation. For v1 we coerce strings/integers and treat
// object/list as freeform — JSON parsing in the textarea is the only validation.
export function buildZodSchema(processInputs) {
  if (!processInputs?.length) return z.object({});
  const shape = {};
  for (const input of processInputs) {
    const dataType = input.input?.literalDataDomain?.dataType;
    const required = (input.minOccurs || 0) > 0;
    let field;
    if (dataType === "string") {
      field = z.string();
      field = required ? field.min(1) : field.optional();
    } else if (dataType === "integer") {
      const numeric = z.coerce.number();
      field = required ? numeric : numeric.optional();
    } else {
      field = required
        ? z.any().refine((v) => v != null && v !== "")
        : z.any().optional();
    }
    shape[input.id] = field;
  }
  return z.object(shape);
}

export function buildPayload(processDetail, values, tags) {
  const payload = { inputs: {} };
  const sourceInputs = processDetail?.inputs?.length
    ? processDetail.inputs.map((i) => i.id)
    : Object.keys(values || {});
  for (const id of sourceInputs) {
    const v = values?.[id];
    if (v === undefined || v === null || v === "") continue;
    payload.inputs[id] = v;
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
