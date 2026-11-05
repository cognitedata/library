---
name: design
description: Simplified Aura guidance for selecting primitives, keeping token usage consistent, and applying reliable layout/copy/state patterns in Flows and Fusion apps.
allowed-tools: Read, Glob, Grep, Edit, Write
---

## Role

Use Aura as the default UI system for customer-facing product work. Prefer decision-level guidance over exhaustive rules:
- choose the right primitive first,
- apply semantic tokens (no raw values),
- keep layouts and UX states consistent,
- write concise, action-oriented copy.

> **Note:** Make sure the project is using the latest version of Aura (`@cognite/aura`) to access the most up-to-date guidance and components.

Everything you need to know is:
- In the node_modeules/@cognite/aura folder
- Aura Design Guidlines can be found at: `./node_modules/@cognite/aura/DESIGN.md`
- At the storybook https://master--695bb4b1b8041ae09768950a.chromatic.com/?path=/docs/primitives
- On the docs site https://docs.cognite.com/aura-design-system/primitives


<when-to-reference>

Consult this skill whenever you are:

- Creating or migrating interactive UI, forms, tables, navigation, or data display
- Writing or modifying styles, colors, spacing, or typography
- Choosing components, tokens, or layout patterns
- Creating or restructuring pages and responsive layouts
- Writing or editing any user-facing text
- Building forms, handling API responses, async actions, confirmations, or dynamic content
- Implementing accessibility (keyboard, focus, headings, ARIA, alt text)
- Applying Aura correctly in a Flows or React app

</when-to-reference>

## Operating principles

1. Use Aura primitives before custom UI.
2. Follow foundations through semantic tokens and Aura defaults; do not hardcode raw values.
3. If a primitive almost fits, do not override visuals to force it; check variants/props first, then document the gap.
4. Keep behavior predictable and accessible: keyboard support, visible focus, and clear feedback for loading/success/error.
5. Use publicly reachable links — Aura design system docs, Storybook, and Figma.
