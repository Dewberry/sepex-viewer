import NextAuth from "next-auth";
import KeycloakProvider from "next-auth/providers/keycloak";

const providers = [];

if (process.env.KEYCLOAK_ISSUER) {
  providers.push(
    KeycloakProvider({
      clientId: process.env.KEYCLOAK_CLIENT_ID,
      clientSecret: process.env.KEYCLOAK_CLIENT_SECRET,
      issuer: process.env.KEYCLOAK_ISSUER
    })
  );
}

export const { handlers, signIn, signOut, auth } = NextAuth({ providers });
