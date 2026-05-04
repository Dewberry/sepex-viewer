import { defineConfig, globalIgnores } from "eslint/config";
import nextVitals from "eslint-config-next/core-web-vitals";
import prettier from "eslint-config-prettier";
import importPlugin from "eslint-plugin-import";
import prettierPlugin from "eslint-plugin-prettier";

const eslintConfig = defineConfig([
  ...(Array.isArray(nextVitals) ? nextVitals : [nextVitals]),
  prettier,
  {
    plugins: {
      import: importPlugin,
      prettier: prettierPlugin
    },
    rules: {
      "no-unused-vars": [
        "warn",
        {
          args: "all",
          argsIgnorePattern: "^_"
        }
      ],
      "import/no-unresolved": "off",
      "react-hooks/set-state-in-effect": "off",
      "react-hooks/incompatible-library": "off",
      "import/order": [
        "error",
        {
          groups: ["builtin", "external", "internal", "index"],
          pathGroups: [
            {
              pattern: "react",
              group: "builtin",
              position: "before"
            },
            {
              pattern: "next/**",
              group: "external",
              position: "before"
            },
            {
              pattern: "**/_components/**",
              group: "internal",
              position: "before"
            },
            {
              pattern: "**/_hooks/**",
              group: "internal",
              position: "after"
            },
            {
              pattern: "**/_utils/**",
              group: "internal",
              position: "after"
            },
            {
              pattern: "**/styles/*!(.css)",
              group: "index",
              position: "before"
            },
            {
              pattern: "**/*.css",
              group: "index",
              position: "after"
            }
          ],
          pathGroupsExcludedImportTypes: ["builtin"],
          "newlines-between": "never",
          alphabetize: {
            order: "asc",
            caseInsensitive: true
          }
        }
      ],
      "prettier/prettier": "error"
    }
  },
  globalIgnores([".next/**", "out/**", "build/**", "next-env.d.ts"])
]);

export default eslintConfig;
