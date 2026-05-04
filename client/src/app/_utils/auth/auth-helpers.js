import { auth } from "@/app/_utils/auth/auth";
import { devBypassUser, isDevBypass } from "@/app/_utils/auth/devBypass";

export async function getAuthUser() {
  if (isDevBypass) return devBypassUser;
  const session = await auth();
  if (!session?.user?.email) return null;
  return session.user;
}

export async function requireAuthUser() {
  const user = await getAuthUser();
  if (!user) {
    throw new Response(JSON.stringify({ error: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" }
    });
  }
  return user;
}
