import pluginJs from '@eslint/js'
import globals from 'globals'

// See more configuration options:
// https://eslint.org/docs/latest/use/configure/configuration-files

/** @type {import('eslint').Linter.Config[]} */
export default [
    { languageOptions: { globals: { ...globals.browser, ...globals.node } } },
    pluginJs.configs.recommended,
    {
        ignores: ['types/*'],
    },
]
