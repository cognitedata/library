import {
  KeaAliasingNode,
  KeaAliasPersistenceNode,
  KeaEndNode,
  KeaExtractionNode,
  KeaReferenceIndexNode,
  KeaSourceViewNode,
  KeaStartNode,
  KeaValidationNode,
  KeaValidationRuleNode,
} from "./keaNodes";

export const KEA_FLOW_NODE_TYPES = {
  keaStart: KeaStartNode,
  keaEnd: KeaEndNode,
  keaSourceView: KeaSourceViewNode,
  keaExtraction: KeaExtractionNode,
  keaAliasing: KeaAliasingNode,
  keaValidation: KeaValidationNode,
  keaMatchValidationRuleSourceView: KeaValidationRuleNode,
  keaMatchValidationRuleExtraction: KeaValidationRuleNode,
  keaMatchValidationRuleAliasing: KeaValidationRuleNode,
  keaAliasPersistence: KeaAliasPersistenceNode,
  keaReferenceIndex: KeaReferenceIndexNode,
};
