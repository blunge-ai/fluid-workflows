import typescriptEslint from "@typescript-eslint/eslint-plugin";
import unusedImports from "eslint-plugin-unused-imports";
import tsParser from "@typescript-eslint/parser";
import path from "node:path";
import { fileURLToPath } from "node:url";
import js from "@eslint/js";
import { FlatCompat } from "@eslint/eslintrc";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// eslint-disable-next-line
const compat = new FlatCompat({
    baseDirectory: __dirname,
    recommendedConfig: js.configs.recommended,
    allConfig: js.configs.all
});
// eslint-disable-next-line
export default [...compat.extends("plugin:@typescript-eslint/recommended-type-checked", "plugin:@typescript-eslint/stylistic-type-checked"), {
        plugins: {
            "@typescript-eslint": typescriptEslint,
            "unused-imports": unusedImports,
        },
        languageOptions: {
            parser: tsParser,
            ecmaVersion: 5,
            sourceType: "script",
            parserOptions: {
                project: true,
            },
        },
        rules: {
            "react/display-name": "off",
            "@typescript-eslint/prefer-nullish-coalescing": "off",
            "@typescript-eslint/array-type": "off",
            "@typescript-eslint/consistent-type-definitions": "off",
            "@typescript-eslint/no-empty-function": "off",
            "@typescript-eslint/unbound-method": "off",
            "@typescript-eslint/consistent-type-imports": "off",
            "@typescript-eslint/require-await": "off",
            "@typescript-eslint/no-misused-promises": ["error", {
                    checksVoidReturn: {
                        attributes: false,
                    },
                }],
            "@typescript-eslint/no-unsafe-member-access": "warn",
            "@typescript-eslint/no-unsafe-call": "warn",
            "@typescript-eslint/no-unsafe-argument": "off",
            "@typescript-eslint/no-unsafe-assignment": "off",
            "@typescript-eslint/no-unused-vars": "off",
            "react-hooks/exhaustive-deps": "off",
            "unused-imports/no-unused-imports": "off",
        },
    }];
