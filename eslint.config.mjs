import eslint from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  {
    ignores: ["dist/**", "node_modules/**"]
  },
  {
    files: ["src/**/*.ts", "test/**/*.ts"],
    extends: [
      eslint.configs.recommended,
      ...tseslint.configs.recommendedTypeChecked
    ],
    rules: {
      '@typescript-eslint/require-await': 'off',
    },
    languageOptions: {
      parserOptions: {
        project: "./tsconfig.json",
        tsconfigRootDir: "."
      }
    }
  }
);
