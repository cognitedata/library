import { FileText } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { CollapsibleSection } from "@/components/ui/collapsible-section";
import { Textarea } from "@/components/ui/textarea";
import { DATA_TYPE_CONFIGS } from "@/config/dataTypes";

interface PromptsCardProps {
  instructions: string;
  prompts: Record<string, string>;
}

export function PromptsCard({ instructions, prompts }: PromptsCardProps) {
  return (
    <Card className="border border-border">
      <CardContent className="p-6">
        <div className="flex items-center gap-2 mb-5">
          <FileText className="w-5 h-5 text-primary shrink-0" />
          <h3 className="text-base font-semibold text-foreground">Prompts</h3>
        </div>
        <p className="text-xs text-muted-foreground mb-4">
          These values come from the config node. Change them by updating that
          node and redeploying.
        </p>

        <div className="space-y-3 mb-6 rounded-lg border border-border bg-muted/40 p-4 dark:bg-muted/25">
          <label
            htmlFor="settings-base-instructions"
            className="text-sm font-medium text-foreground"
          >
            Base instructions
          </label>
          <Textarea
            id="settings-base-instructions"
            value={instructions}
            readOnly
            className="min-h-[100px] resize-none bg-background font-mono text-sm text-foreground"
          />
        </div>

        <div className="border-t border-border pt-6">
          <CollapsibleSection
            title="Data type prompts"
            defaultOpen={false}
            className="bg-muted/40 dark:bg-muted/30"
          >
            <div className="grid gap-4">
              {Object.entries(DATA_TYPE_CONFIGS).map(([key, typeConfig]) => (
                <CollapsibleSection
                  key={key}
                  title={typeConfig.displayName}
                  defaultOpen={false}
                  className="border-border/90 bg-background/95 shadow-sm ring-1 ring-border/50 dark:bg-background/60"
                >
                  <Textarea
                    aria-label={`${typeConfig.displayName} prompt template`}
                    value={prompts[key] ?? ""}
                    readOnly
                    className="min-h-[80px] resize-none bg-background font-mono text-xs text-foreground"
                  />
                </CollapsibleSection>
              ))}
            </div>
          </CollapsibleSection>
        </div>
      </CardContent>
    </Card>
  );
}
