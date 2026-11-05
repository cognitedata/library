/**
 * Resolved properties of the default `ops_summarizer_config` node.
 * Keep this equal to `default.config.yaml` plus `data_modeling/ops_summarizer_config.Node.yaml` after variable substitution.
 * `spaces.dataInstances` is the podcast function's source-data space (previously hardcoded).
 */
export const DEFAULT_NODE_PROPERTIES: Record<string, unknown> = {
  schemaVersion: 1,
  name: "Operations Summarizer",
  description:
    "Project settings for the Operations Summarizer app and its functions.",
  companyName: "",
  spaces: {
    summaries: "ops_summaries",
    dataInstances: "inst_data",
  },
  externalIdPrefixes: {
    operationSummary: "ops_summary_",
    testReport: "test_report_",
    podcast: "ops_podcast_",
  },
  functions: {
    summarizer: {
      externalId: "Operations_Summarizer_Function_v2",
      pollIntervalMs: 3000,
      timeoutMs: 600000,
      maxConsecutivePollErrors: 5,
    },
  },
  prompts: {
    header:
      "You are an Operations Summarizer summarizing new data added to the data platform from {start_time} to {end_time}. Briefly summarize these {num_instances} {data_type}s with the following data:\nData: {table_text}\n\nDO NOT USE ANY TOOLS TO ACCESS DATA FROM CDF. ONLY WORK WITH THE DATA PROVIDED.\n\nFollow these rules:\n",
    holistic:
      'Here are {num_chunks} partial summaries for {total_instances} {data_type} instances that were split into chunks due to large dataset size.\n\nPlease create a single, comprehensive, holistic summary that follows the structure of the original summaries.\n- Do not mention the chunks or holistic in the summary\n- Do not give any evaluative statements about the dataset or an overall conclusion\n- Stay concise and readable, under 500 words\n- Do not include any concluding sentences or meta-statements about the summary itself (e.g., "This summary highlights...", "Overall, this data shows...", "In conclusion..."). The summary should end directly after presenting the information.\nHere are the partial summaries:\n\n{combined_summaries}\n\nPlease provide a comprehensive summary that gives a complete picture of all {total_instances} {data_type} instances:',
    agentInstructions:
      "You are an Operations Summarizer AI agent that specializes in analyzing and summarizing operational data from industrial data platforms. Your primary role is to create comprehensive, concise summaries of various types of operational data including assets, files, work orders, time series, and equipment.\n\nCORE RESPONSIBILITIES:\n1. Analyze operational data provided in tabular format\n2. Create structured, informative summaries based on data type\n3. Identify patterns, trends, and key insights\n4. Provide actionable information for operations teams\n\nGENERAL GUIDELINES:\n- Always work ONLY with the data provided - do not use tools to access additional data\n- Be concise and factual in your summaries\n- Use bullet points when applicable for clarity\n- Be explicit and use exact numbers when referring to multiple instances\n- Never include concluding sentences or meta-statements about the summary itself\n- Focus on operational significance and business value\n\nDATA TYPE SPECIFIC INSTRUCTIONS:\n\nASSETS:\n- Summarize types of assets based on their descriptions\n- Discuss trends and patterns in asset descriptions\n- Identify asset hierarchies and relationships where applicable\n- Highlight critical or high-value assets\n\nFILES:\n- Categorize files by type and purpose based on descriptions\n- Identify documentation patterns and file organization\n- Note any critical operational documents or procedures\n\nWORK ORDERS:\n- Distinguish between routine maintenance and critical/emergency work orders\n- Analyze work patterns and asset maintenance trends\n- Identify which assets are being worked on most frequently\n- Highlight any urgent or high-priority work orders\n\nTIME SERIES:\n- Summarize types of time series data and their characteristics\n- Note any patterns in data collection or monitoring\n- Identify key operational metrics being tracked\n\nEQUIPMENT:\n- Categorize equipment by type and function\n- Analyze equipment distribution and patterns\n- Note any critical or specialized equipment\n\nFORMATTING REQUIREMENTS:\n- Keep summaries under 400 words\n- Use clear, professional language\n- Structure information logically\n- End summaries directly after presenting information (no conclusions)\n- Use specific numbers and data points when available\n\nRemember: Your goal is to provide operations teams with clear, actionable insights that help them understand their operational data landscape and make informed decisions.",
    dataTypes: {
      assets:
        '- Summarize the types of assets added to CDF based on their descriptions\n- Discuss trends and patterns in the descriptions. \n- Write in a concise paragraph less than 400 words. Use Bullet points when applicable. Be explicit and use exact numbers when referring to multiple instances. \n- Do not include any concluding sentences or meta-statements about the summary itself (e.g., "This summary highlights...", "Overall, this data shows...", "In conclusion..."). The summary should end directly after presenting the information.',
      files:
        '- Summarize the types of files added to CDF based on their descriptions\n- Discuss trends and patterns in the descriptions. \n- Write in a concise paragraph less than 400 words. Use Bullet points when applicable. Be explicit and use exact numbers when referring to multiple instances\n- Do not include any concluding sentences or meta-statements about the summary itself (e.g., "This summary highlights...", "Overall, this data shows...", "In conclusion..."). The summary should end directly after presenting the information.',
      work_orders:
        '- Make note of important/critical/emergency work orders if any. Distinguish between routine maintenance and work orders \n- Discuss trends and patterns based on the descriptions \n- Explain which assets are being worked on\n- Write in a concise paragraph less than 400 words. Use Bullet points when applicable. Be explicit and use exact numbers when referring to multiple instances\n- Do not include any concluding sentences or meta-statements about the summary itself (e.g., "This summary highlights...", "Overall, this data shows...", "In conclusion..."). The summary should end directly after presenting the information.',
      time_series:
        '- Summarize the types of time series added to CDF based on their descriptions\n- Discuss trends and patterns in the descriptions and any notable data characteristics\n- Write in a concise paragraph less than 400 words. Use Bullet points when applicable. Be explicit and use exact numbers when referring to multiple instances\n- Do not include any concluding sentences or meta-statements about the summary itself (e.g., "This summary highlights...", "Overall, this data shows...", "In conclusion..."). The summary should end directly after presenting the information.',
      equipment:
        '- Summarize the types of equipment added to CDF based on their descriptions\n- Discuss trends and patterns in the descriptions and equipment types\n- Write in a concise paragraph less than 400 words. Use Bullet points when applicable. Be explicit and use exact numbers when referring to multiple instances\n- Do not include any concluding sentences or meta-statements about the summary itself (e.g., "This summary highlights...", "Overall, this data shows...", "In conclusion..."). The summary should end directly after presenting the information.',
    },
  },
  defaults: {
    timezoneOffsetHours: -7,
    summaryPeriodDays: 7,
    personaPresets: {
      engineer: ["work_orders"],
    },
  },
  agent: {
    name: "Operation Summarizer Agent",
    externalIdSuffix: "_operation_summarizer_helper_agent",
    defaultLanguageModel: "cognite/balanced",
    maxConcurrentTasks: 3,
  },
  dataModel: {
    views: {
      summaryOutputFile: {
        space: "cdf_cdm",
        externalId: "CogniteFile",
        version: "v1",
      },
      file: { space: "cdf_cdm", externalId: "CogniteFile", version: "v1" },
      asset: { space: "cdf_cdm", externalId: "CogniteAsset", version: "v1" },
      timeSeries: {
        space: "cdf_cdm",
        externalId: "CogniteTimeSeries",
        version: "v1",
      },
      activity: {
        space: "cdf_cdm",
        externalId: "CogniteActivity",
        version: "v1",
      },
      equipment: {
        space: "cdf_cdm",
        externalId: "CogniteEquipment",
        version: "v1",
      },
    },
    summaryListViewProperties: [
      "name",
      "description",
      "mimeType",
      "uploadedTime",
    ],
    cdfAlphaVersionHeader: "20250919-alpha",
  },
  limits: {
    chunkSize: 50,
    instanceQueryLimit: 1000,
    maxInstancesPerDatatype: 15000,
    summaryListPageSize: 15,
    operationSummaryPickerMax: 500,
    defaultPageSize: 5,
    retry: { baseMs: 1000, maxDelayMs: 30000 },
    concurrency: { default: 8, delete: 2, agent: 3 },
  },
  dataTypes: {
    assets: {
      displayName: "Assets",
      view: "asset",
      enabledByDefault: true,
      isSpecialCase: false,
      tableColumns: [
        { header: "id", property: "externalId" },
        { header: "name", property: "name" },
        { header: "description", property: "description" },
        { header: "parent", property: "parent", default: "None" },
      ],
    },
    files: {
      displayName: "Files",
      view: "file",
      enabledByDefault: true,
      isSpecialCase: false,
      tableColumns: [
        { header: "name", property: "name" },
        { header: "description", property: "description" },
        {
          header: "asset",
          property: "assets",
          formatter: "assetsList",
          default: "None",
        },
      ],
    },
    work_orders: {
      displayName: "Work Orders",
      view: "activity",
      enabledByDefault: true,
      isSpecialCase: true,
      subcategories: [
        "completed_work_orders",
        "ongoing_work_orders",
        "scheduled_work_orders",
      ],
      tableColumns: [
        { header: "id", property: "externalId" },
        { header: "name", property: "name" },
        { header: "description", property: "description" },
        { header: "start date", property: "startTime", formatter: "isoDate" },
        { header: "end date", property: "endTime", formatter: "isoDate" },
        {
          header: "assets",
          property: "assets",
          formatter: "assetsList",
          default: "N/A",
        },
      ],
    },
    time_series: {
      displayName: "Time Series",
      view: "timeSeries",
      enabledByDefault: true,
      isSpecialCase: false,
      tableColumns: [
        { header: "name", property: "name" },
        { header: "description", property: "description" },
      ],
    },
    equipment: {
      displayName: "Equipment",
      view: "equipment",
      enabledByDefault: true,
      isSpecialCase: false,
      tableColumns: [
        { header: "id", property: "externalId" },
        { header: "name", property: "name" },
        { header: "description", property: "description" },
        { header: "type", property: "equipmentType", default: "N/A" },
      ],
    },
  },
};
