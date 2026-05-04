export const isDevBypass =
  process.env.NODE_ENV === "development" &&
  process.env.NEXT_PUBLIC_DEV_BYPASS_AUTH === "true";

export const devBypassUser = {
  email: process.env.NEXT_PUBLIC_SEPEX_USER_EMAIL || "dev@dewberry.local",
  name: "Dev User"
};
