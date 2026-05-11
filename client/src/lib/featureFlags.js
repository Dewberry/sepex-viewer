// Read by both server-side route handlers and client components.
// Set NEXT_PUBLIC_ENABLE_PROPOSED_API="true" to enable UI surfaces that
// depend on Sepex API additions from research/proposed-next-steps.md
// (items A, C, D, H, I). Defaults to "false" so the design branch reflects
// what the current Sepex API can support today.
export const PROPOSED_API_ENABLED =
  process.env.NEXT_PUBLIC_ENABLE_PROPOSED_API === "true";
