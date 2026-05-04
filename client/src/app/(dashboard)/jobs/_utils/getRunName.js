// "Run name" displayed in the Jobs table is derived client-side because the
// OGC API has no dedicated run-name field. Convention: a deployment may use
// the first tag as a category prefix (project code, region, etc.) and the
// next tag as the run identifier. Populate KNOWN_PREFIXES per deployment if
// you have such prefixes; otherwise the helper just returns the first tag.

const KNOWN_PREFIXES = new Set([]);

export default function getRunName(tags) {
  if (!Array.isArray(tags) || tags.length === 0) return "—";
  const runTag = tags.find((tag) => !KNOWN_PREFIXES.has(tag.toLowerCase()));
  return runTag || "—";
}
