import { Type } from '@sinclair/typebox';
import type { AtlasTool } from '../atlas-agent/types';

import type { TodoList } from './types';

const parameters = Type.Object({
  todos: Type.Array(
    Type.Object({
      content: Type.String({ description: 'Imperative form, e.g. "Fix authentication bug"' }),
      status: Type.Unsafe<'pending' | 'in_progress' | 'completed'>({
        type: 'string',
        enum: ['pending', 'in_progress', 'completed'],
        description: 'Task status',
      }),
      activeForm: Type.String({
        description: 'Present continuous form, e.g. "Fixing authentication bug"',
      }),
    }),
    { description: 'The complete, updated todo list. Must include ALL items — do not omit any.' }
  ),
});

const DESCRIPTION = `Use this tool to create and manage a structured task list when answering questions about industrial assets, equipment, maintenance orders, files, time series, and related data in CDF. This helps you track progress across multi-step queries, and helps the user see what you are doing and why.

## When to Use This Tool
Use this tool proactively in these scenarios:

1. Fetching related data across types - Any question that involves traversing from one type to another (e.g., "files for an asset", "time series for a pump", "operations on a maintenance order", "notifications for an asset")
2. Reverse list-relation traversals - Finding CogniteFile, CogniteTimeSeries, or CogniteActivity instances that reference a known asset or equipment requires a separate /search step with containsAny — this cannot be done in a single /query call
3. Multi-level traversals - When the question requires stepping through more than one relation (e.g., asset → equipment → time series)
4. User asks multiple questions - When the user asks for several different pieces of data at once
5. After receiving new instructions - Immediately capture user requirements as todos
6. When you start working on a task - Mark it as in_progress BEFORE beginning work. Ideally you should only have one todo as in_progress at a time
7. After completing a task - Mark it as completed and add any new follow-up tasks discovered during execution

## When NOT to Use This Tool

Skip using this tool when:
1. The question targets a single type with no relation traversal (e.g., "list all assets", "find the maintenance order with ID X")
2. The question is purely conversational or informational (e.g., "what is a functional location?")
3. The answer requires only one tool call
4. You are clarifying what the user wants before starting work

NOTE that you should not use this tool if there is only one trivial step to do. In this case you are better off just doing the task directly.

## Why Multi-Step Decomposition Is Needed for Cross-Type Queries

In CDF Data Modeling, data is organized into separate views (CogniteAsset, CogniteFile, CogniteEquipment, CogniteTimeSeries, CogniteActivity, CogniteMaintenanceOrder, CogniteOperation, etc.). Relations between views are stored as direct relation properties on one side of the relationship.

### Reverse Direct Relation Constraint

A critical constraint governs how relations can be traversed:

**Single-target relations (targetsList=false)** — the property points to exactly one instance. Reverse traversal CAN be done via /query using \`through\` + \`direction: inwards\`.
- CogniteAsset.children traverses backward through CogniteAsset.parent (single) — works with /query
- CogniteAsset.equipment traverses backward through CogniteEquipment.asset (single) — works with /query
- CogniteMaintenanceOrder.operations traverses backward through CogniteOperation.maintenanceOrder (single) — works with /query

**List-target relations (targetsList=true)** — the property holds a list of references. Reverse traversal CANNOT be done via /query. You MUST use /search with a containsAny filter instead.
- CogniteAsset.files — reverse through CogniteFile.assets (list) — needs /search + containsAny
- CogniteAsset.timeSeries — reverse through CogniteTimeSeries.assets (list) — needs /search + containsAny
- CogniteAsset.activities — reverse through CogniteActivity.assets (list) — needs /search + containsAny
- CogniteEquipment.timeSeries — reverse through CogniteTimeSeries.equipment (list) — needs /search + containsAny
- CogniteEquipment.activities — reverse through CogniteActivity.equipment (list) — needs /search + containsAny
- CogniteFile.equipment — reverse through CogniteEquipment.files (list) — needs /search + containsAny

This means that many natural user questions ("show me files for pump X") require AT LEAST two steps: (1) find the asset/equipment instance, (2) search the related type using containsAny. This is why task decomposition is essential.

### Hierarchy / Path Interpretation

Use the asset \`path\` property to read and interpret the ancestry of a known node. Do not infer hierarchy from alphabetical ordering or from an item's position in a result list.

- \`path[-1]\` = current node
- \`path[-2]\` = parent
- \`path[-3]\` = grandparent
- \`path[-4]\` = ancestor three levels above the current node

Example:
- Path: \`WMT:VAL -> WMT:23 -> WMT:230900 -> WMT:23-1ST STAGE COMPRESSION-PH -> WMT:23-XX-9105\`
- \`path[-4] = WMT:23\`, \`path[-3] = WMT:230900\`, \`path[-2] = WMT:23-1ST STAGE COMPRESSION-PH\`, \`path[-1] = WMT:23-XX-9105\`

To **find descendants** N levels below a given node, do NOT filter on the \`path\` array. Instead, traverse the \`parent\` relation one level at a time, with **one todo item per level**:
- Level 1 (children): query assets where \`parent == A\`
- Level 2 (grandchildren): query assets where \`parent\` is any of the level-1 results
- Level 3: query assets where \`parent\` is any of the level-2 results
- …and so on until the target depth

Each level must be its own todo item — do not collapse multiple levels into a single task.

### Subtree + Maintenance Queries

A very common industrial pattern is "what maintenance work exists for section X and everything below it?" This combines hierarchy traversal with maintenance order (or notification) lookups and always requires multiple steps:

1. Traverse the hierarchy level by level to collect all descendant asset IDs (one todo per level, as above)
2. Search for maintenance orders referencing those assets — use \`mainAsset\` if looking for the primary asset on the order (single field, filter directly); use \`assets\` containsAny if looking for any association (list field, needs /search)
3. Optionally fetch operations or notifications for the found maintenance orders

Note: \`mainAsset\` and \`assets\` on \`CogniteMaintenanceOrder\` serve different purposes. \`mainAsset\` is the primary functional location the order is about; \`assets\` is the broader list of associated assets. For most "what work is planned for asset X" questions, querying \`mainAsset\` is the right starting point.

## Examples of When to Use the Todo List

<example>
User: Please give me all the files for 1st stage compressor
Assistant: *Creates todo list with the following items:*
1. Discover relevant views for files and assets in the data model
2. Find the asset instance for "1st stage compressor"
3. Search for files related to the identified asset (using containsAny on CogniteFile.assets)
4. Summarize the files with their names, space, and external IDs
*Begins working on the first task*

<reasoning>
The assistant used the todo list because:
1. This is a cross-type query spanning CogniteAsset and CogniteFile
2. CogniteFile.assets is a list relation (targetsList=true), so reverse traversal requires a separate /search step with containsAny — it cannot be done in a single /query call
3. The asset must be identified first before files can be searched
4. Multiple sequential tool calls are needed, making task tracking valuable
</reasoning>
</example>

<example>
User: Show me the time series and recent activities for pump P-101
Assistant: *Searches for the equipment instance P-101*
Found equipment "P-101" in the data model.
*Creates todo list with specific items:*
1. Retrieve full details of equipment P-101
2. Search for time series referencing P-101 (CogniteTimeSeries.equipment is a list relation — needs /search + containsAny)
3. Search for activities referencing P-101 (CogniteActivity.equipment is a list relation — needs /search + containsAny)
4. Summarize findings with time series names and activity descriptions
*Begins working on the first task*

<reasoning>
The assistant used the todo list because:
1. First, the assistant identified the equipment to understand the scope
2. Both CogniteTimeSeries.equipment and CogniteActivity.equipment are list relations requiring separate /search + containsAny calls — neither can be resolved with a single /query traversal
3. Multiple independent search steps are needed, making task tracking valuable
</reasoning>
</example>

<example>
User: List all child assets of the "North Platform" and their associated equipment and maintenance orders
Assistant: *Creates todo list breaking down the query:*
1. Find the "North Platform" asset instance
2. Query child assets using the parent relation (CogniteAsset.parent is a single relation — can use /query with through + direction: inwards)
3. Query equipment for each child asset (CogniteEquipment.asset is a single relation — can use /query)
4. Find maintenance orders referencing the child assets
5. Compile and present the asset hierarchy with equipment and maintenance information
*Begins working on the first task*

<reasoning>
The assistant used the todo list because:
1. This involves traversing multiple levels of the data model hierarchy
2. Even though parent/child and asset/equipment are single relations (queryable via /query), the overall task still spans many sequential steps across the hierarchy
3. The user wants a comprehensive view across assets, equipment, and maintenance orders
4. Tracking progress prevents missing any branch of the hierarchy
</reasoning>
</example>

<example>
User: What operations and notifications are there for maintenance order MO-2024-001?
Assistant: *Creates todo list:*
1. Find the maintenance order MO-2024-001
2. Query operations for this maintenance order (CogniteOperation.maintenanceOrder is a single relation — can use /query with through + direction: inwards)
3. Search for notifications referencing this maintenance order (CogniteNotification.maintenanceOrder is a single relation — can use /query)
4. Summarize operations and notifications with their status and descriptions
*Begins working on the first task*

<reasoning>
The assistant used the todo list because:
1. This spans three types: CogniteMaintenanceOrder, CogniteOperation, and CogniteNotification
2. Operations and notifications both require separate query steps after finding the maintenance order
3. Tracking steps ensures both relations are covered and results are organized clearly
</reasoning>
</example>

<example>
User: What is the maintenance backlog for the entire 1st stage compression section?
Assistant: *Creates todo list:*
1. Find the asset instance for "1st stage compression"
2. Query level-1 children (assets where parent == 1st stage compression asset)
3. Query level-2 children (assets where parent is any level-1 result)
4. Query level-3 children (assets where parent is any level-2 result)
5. Search for maintenance orders where mainAsset is any of the collected asset IDs
6. Summarize maintenance orders with their status, priority, and scheduled dates
*Begins working on the first task*

<reasoning>
The assistant used the todo list because:
1. "The entire section" means all descendant assets must be collected first — one todo per hierarchy level
2. Once all descendant asset IDs are known, maintenance orders are found by filtering on mainAsset (single indexed field — no containsAny needed for this use case)
3. The hierarchy traversal and the maintenance lookup are separate dependent phases, making step-by-step tracking essential
</reasoning>
</example>

<example>
User: Give me all files for assets that are two levels below WMT:23
Assistant: *Creates todo list:*
1. Find the asset instance WMT:23
2. Query level-1 children of WMT:23 (assets where parent == WMT:23)
3. Query level-2 children (assets where parent is any of the level-1 results)
4. Search for files referencing the level-2 assets (CogniteFile.assets is a list relation — needs /search + containsAny)
5. Summarize files with their names and external IDs
*Begins working on the first task*

<reasoning>
The assistant used the todo list because:
1. Descendant lookup must be done by stepping through the parent relation one level at a time — each level is its own todo item, never collapsed into one task
2. Two levels of traversal means two separate query steps before any file lookup can happen
3. The final file lookup requires a separate /search + containsAny step because CogniteFile.assets is a list relation
4. The chain of dependent steps makes task tracking essential
</reasoning>
</example>

<example>
User: Please give me the seventh descendant of WMT:VAL
Assistant: *Creates todo list:*
1. Find the asset instance WMT:VAL
2. Query level-1 children (assets where parent == WMT:VAL)
3. Query level-2 children (assets where parent is any level-1 result)
4. Query level-3 children (assets where parent is any level-2 result)
5. Query level-4 children (assets where parent is any level-3 result)
6. Query level-5 children (assets where parent is any level-4 result)
7. Query level-6 children (assets where parent is any level-5 result)
8. Query level-7 children (assets where parent is any level-6 result)
9. Summarize the level-7 descendants with space and external ID
*Begins working on the first task*

<reasoning>
The assistant used the todo list because:
1. Seven levels of parent traversal are required — each level is a separate query and gets its own todo item
2. Collapsing all traversal steps into one task would hide progress and make it impossible to track which level is being processed
3. Each step depends on the results of the previous one, making explicit tracking essential
</reasoning>
</example>

## Examples of When NOT to Use the Todo List

<example>
User: How many assets do we have?
Assistant: *Uses the aggregate tool on the CogniteAsset view*
Based on the aggregation, you have 1,247 assets in your data model.

<reasoning>
The assistant did not use the todo list because this is a single aggregation query against one view. No cross-type traversal or multi-step decomposition is needed.
</reasoning>
</example>

<example>
User: What is a direct relation in CDF?
Assistant: A direct relation in CDF is a property on a node that references another node by its space and externalId. It creates a typed graph connection stored directly on the source instance, without needing a separate edge object.

<reasoning>
The assistant did not use the todo list because this is an informational request with no data query to execute. The user is asking for an explanation, not for the assistant to retrieve data from CDF.
</reasoning>
</example>

<example>
User: Find the asset named "Main Turbine"
Assistant: *Searches for the asset using a name filter on CogniteAsset*
Found asset "Main Turbine" (externalId: main-turbine-001, space: plant-assets).

<reasoning>
The assistant did not use the todo list because this is a single search operation against one view. The result is returned directly with no additional steps needed.
</reasoning>
</example>

<example>
User: Does equipment EP-200 exist?
Assistant: *Queries the CogniteEquipment view for EP-200*
Yes, equipment EP-200 exists in space "industrial-assets" with name "Centrifugal Pump EP-200".

<reasoning>
The assistant did not use the todo list because this is a single instance lookup that can be completed in one step. No multi-step decomposition is needed.
</reasoning>
</example>

## Task States and Management

1. **Task States**: Use these states to track progress:
   - pending: Task not yet started
   - in_progress: Currently working on (limit to ONE task at a time)
   - completed: Task finished successfully

   **IMPORTANT**: Task descriptions must have two forms:
   - content: The imperative form describing what needs to be done (e.g., "Find the asset instance for 1st stage compressor", "Search for files referencing the asset")
   - activeForm: The present continuous form shown during execution (e.g., "Finding the asset instance for 1st stage compressor", "Searching for files referencing the asset")

2. **Task Management**:
   - Update task status in real-time as you work
   - Mark tasks complete IMMEDIATELY after finishing (don't batch completions)
   - Exactly ONE task must be in_progress at any time (not less, not more)
   - Complete current tasks before starting new ones
   - Remove tasks that are no longer relevant from the list entirely
   - **Always mark the final task as completed before delivering your answer** — do not give the response and stop without updating the todo list. The last tool call before responding to the user must be a TodoWrite that marks the final task completed.
   - **Update pending task titles with discovered node names**: after completing a step that returns concrete instances, rewrite the titles of downstream pending tasks to reflect what was actually found. If there are many results, use the short form: "WMT:23, WMT:24, WMT:25 … (12 total)". For example, once level-1 children are known, change "Query level-2 children (assets where parent is any level-1 result)" to "Query level-2 children of WMT:23, WMT:24 … (4 total)".

3. **Task Completion Requirements**:
   - ONLY mark a task as completed when you have FULLY accomplished it
   - If a query returns errors or unexpected results, keep the task as in_progress
   - When blocked, create a new task describing what needs to be resolved
   - Never mark a task as completed if:
     - The query returned an error
     - You received partial or empty results when data was expected
     - You need to retry with a different approach or filter
     - Required views or instances were not found

4. **Task Breakdown**:
   - Create specific, actionable items
   - Break complex queries into smaller, focused steps
   - Use clear, descriptive task names
   - Always provide both forms:
     - content: "Search for files referencing asset X"
     - activeForm: "Searching for files referencing asset X"

When in doubt, use this tool. Being proactive with task management demonstrates attentiveness and ensures you complete all requirements successfully.`;

export interface TodoWriteToolDeps {
  getTodos: () => TodoList;
  setTodos: (todos: TodoList) => void;
}

export function createTodoWriteTool(deps: TodoWriteToolDeps): AtlasTool<typeof parameters> {
  return {
    name: 'TodoWrite',
    description: DESCRIPTION,
    parameters,
    execute: (args) => {
      const oldTodos = deps.getTodos();
      const allDone = args.todos.every((t) => t.status === 'completed');
      const newTodos = allDone ? [] : args.todos;
      deps.setTodos(newTodos);

      const completed = args.todos.filter((t) => t.status === 'completed').length;
      const inProgress = args.todos.filter((t) => t.status === 'in_progress').length;
      const pending = args.todos.filter((t) => t.status === 'pending').length;

      return {
        output:
          'Todos have been modified successfully. Ensure that you continue to use the todo list ' +
          'to track your progress. Please proceed with the current tasks if applicable.',
        details: { oldTodos, newTodos: args.todos, completed, inProgress, pending },
      };
    },
  };
}
