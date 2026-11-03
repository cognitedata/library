import { Bot } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import type { Agent } from "@/types/operations";

interface AgentStatusCardProps {
  agent: Agent | null;
}

export function AgentStatusCard({ agent }: AgentStatusCardProps) {
  return (
    <Card className="border border-border">
      <CardContent className="p-6">
        <div className="flex items-center gap-2 mb-3">
          <div
            className={`w-2 h-2 rounded-full ${agent ? "bg-emerald-500" : "bg-muted-foreground/30"}`}
          />
          <h3 className="font-medium text-sm">Agent Status</h3>
        </div>
        {agent ? (
          <div className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
              <div>
                <span className="text-muted-foreground text-xs">Name</span>
                <p className="font-medium">{agent.name}</p>
              </div>
              <div>
                <span className="text-muted-foreground text-xs">Model</span>
                <p className="font-medium">{agent.model ?? "Not specified"}</p>
              </div>
              <div>
                <span className="text-muted-foreground text-xs">ID</span>
                <p
                  className="font-mono text-xs truncate"
                  title={agent.externalId}
                >
                  {agent.externalId.slice(-12)}
                </p>
              </div>
            </div>
            {agent.description ? (
              <p className="text-xs text-muted-foreground border-t pt-3">
                {agent.description}
              </p>
            ) : null}
          </div>
        ) : (
          <div className="flex items-center gap-2 p-3 bg-muted/50 rounded">
            <Bot className="w-4 h-4 text-muted-foreground" />
            <span className="text-sm text-muted-foreground">
              No Agent Configured
            </span>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
