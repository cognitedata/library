import { AlertCircle, Check, Copy, ExternalLink } from "lucide-react";
import { Button } from "@/components/ui";
import { useCopyCanvasLink } from "@/hooks/useCopyCanvasLink";
import {
	getCanvasValidationUserMessages,
	type WorkflowRecoveryHint,
	type WorkflowRecoveryStep,
} from "@/utils";
import { EmailShareDialog } from "../EmailShareDialog";
import type { CanvasResult } from "./types";
import { WorkflowRecoveryLinks } from "./WorkflowRecoveryLinks";

export function CanvasResultCard({
	result,
	canvasName,
	recoveryHints,
	onNavigateToWorkflowStep,
}: {
	result: CanvasResult;
	canvasName: string;
	recoveryHints: WorkflowRecoveryHint[];
	onNavigateToWorkflowStep?: (step: WorkflowRecoveryStep) => void;
}) {
	const { copied, copyLink } = useCopyCanvasLink(result.canvasLink);
	const { validation, isUpdate, canvasLink } = result;

	const action = isUpdate ? "updated" : "created";
	const isSuccess = validation.success;
	const userMessages = isSuccess
		? []
		: getCanvasValidationUserMessages(validation);

	return (
		<div
			className={`mt-4 p-4 rounded-xl border ${
				isSuccess
					? "bg-green-50 dark:bg-green-950/50 border-green-200 dark:border-green-800"
					: "bg-amber-50 dark:bg-amber-950/50 border-amber-200 dark:border-amber-800"
			}`}
		>
			<div className="flex items-start gap-3">
				<div
					className={`p-1.5 rounded-full shrink-0 ${isSuccess ? "bg-green-500" : "bg-amber-500"}`}
				>
					{isSuccess ? (
						<Check className="h-4 w-4 text-white" />
					) : (
						<AlertCircle className="h-4 w-4 text-white" />
					)}
				</div>

				<div className="flex-1 min-w-0">
					<p className="font-medium">
						{isSuccess
							? `Canvas ${action} successfully!`
							: "Canvas saved, but we couldn’t fully verify it"}
					</p>

					{userMessages.length > 0 && (
						<ul className="text-xs mt-1 text-amber-700 dark:text-amber-300 list-disc list-inside space-y-1">
							{userMessages.map((line) => (
								<li key={line}>{line}</li>
							))}
						</ul>
					)}

					{!isSuccess &&
						recoveryHints.length > 0 &&
						onNavigateToWorkflowStep && (
							<WorkflowRecoveryLinks
								hints={recoveryHints}
								isSuccess={false}
								onNavigate={onNavigateToWorkflowStep}
							/>
						)}

					<div className="flex items-center gap-3 mt-2 flex-wrap">
						<a
							href={canvasLink}
							target="_blank"
							rel="noopener noreferrer"
							className="inline-flex items-center text-sm text-blue-600 hover:text-blue-800 underline"
						>
							<ExternalLink className="h-4 w-4 mr-1" />
							Open in Fusion
						</a>

						<Button
							variant="ghost"
							className="h-auto p-0 text-sm"
							onClick={copyLink}
						>
							{copied ? (
								<>
									<Check className="h-4 w-4 mr-1 text-green-600" />
									<span className="text-green-600">Copied!</span>
								</>
							) : (
								<>
									<Copy className="h-4 w-4 mr-1" />
									Copy link
								</>
							)}
						</Button>

						<EmailShareDialog canvasLink={canvasLink} canvasName={canvasName} />
					</div>

					{isSuccess &&
						recoveryHints.length > 0 &&
						onNavigateToWorkflowStep && (
							<WorkflowRecoveryLinks
								hints={recoveryHints}
								isSuccess
								onNavigate={onNavigateToWorkflowStep}
							/>
						)}
				</div>
			</div>
		</div>
	);
}
