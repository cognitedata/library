import { auraEslintPlugin } from '@cognite/aura/eslint';
import js from '@eslint/js';
import importPlugin from 'eslint-plugin-import-x';
import noOnlyTestsPlugin from 'eslint-plugin-no-only-tests';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import globals from 'globals';
import tseslint from 'typescript-eslint';

const sharedRules = {
  'import/first': 'error',
  'import/no-duplicates': 'error',
  'import/order': [
    'error',
    {
      groups: ['builtin', 'external', 'internal', 'parent', ['sibling', 'index']],
      'newlines-between': 'always',
      alphabetize: {
        order: 'asc',
        caseInsensitive: true,
      },
    },
  ],
  'no-only-tests/no-only-tests': 'error',
  quotes: ['error', 'single', { avoidEscape: true }],
};

const noUnusedVarsOptions = {
  argsIgnorePattern: '^_',
  caughtErrorsIgnorePattern: '^_',
  varsIgnorePattern: '^_',
  ignoreRestSiblings: true,
};

export default tseslint.config(
  {
    ignores: [
      'dist',
      'build',
      '.next',
      'coverage',
      '*.min.js',
      '*.config.d.ts',
      '.agents',
      '.cursor',
      '.claude',
      '.worktrees',
      '.vendor',
    ],
  },
  {
    files: ['**/*.{js,mjs,cjs,ts,tsx}'],
    plugins: {
      import: importPlugin,
      'no-only-tests': noOnlyTestsPlugin,
    },
    rules: sharedRules,
  },
  {
    files: ['**/*.{js,mjs,cjs}'],
    extends: [js.configs.recommended],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: {
        ...globals.browser,
        ...globals.node,
      },
    },
    rules: {
      'no-unused-vars': ['error', noUnusedVarsOptions],
    },
  },
  {
    files: ['**/*.cjs'],
    languageOptions: {
      sourceType: 'commonjs',
      globals: {
        ...globals.node,
      },
    },
  },
  {
    files: ['**/*.{ts,tsx}'],
    extends: [js.configs.recommended, ...tseslint.configs.recommended],
    languageOptions: {
      ecmaVersion: 'latest',
      globals: {
        ...globals.browser,
      },
    },
    plugins: {
      aura: auraEslintPlugin,
      'react-hooks': reactHooks,
      'react-refresh': reactRefresh,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      '@typescript-eslint/consistent-type-imports': 'error',
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/no-unused-vars': ['error', noUnusedVarsOptions],
      'aura/no-overriding-styles': 'warn',
      'no-unused-vars': 'off',
      'react-refresh/only-export-components': ['error', { allowConstantExport: true }],
    },
  }
);
