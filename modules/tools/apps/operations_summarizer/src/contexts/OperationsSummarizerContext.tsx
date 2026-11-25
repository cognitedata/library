/**
 * Operations Summarizer session contexts — barrel re-exports.
 * Slice hooks and provider live in `operations-summarizer/` for independent maintenance.
 */

export {
	OperationsProvider,
	useOperations,
} from "@/contexts/operations-summarizer/operationsProvider";

export { useTestingSessionContext } from "@/contexts/operations-summarizer/testingSessionContext";
