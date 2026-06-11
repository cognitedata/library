import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import type { AnnotationState } from "@/shared/utils/types";
import { FileAnnotationStatus } from "@/shared/utils/constants";
import { useMemo, useState } from "react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/shared/components/ui/select";
import { Activity } from "lucide-react";

interface ThroughputChartProps {
  data: AnnotationState[];
}

type TimeAggregation = "hourly" | "daily" | "weekly";

export function ThroughputChart({ data }: ThroughputChartProps) {
  const [timeAgg, setTimeAgg] = useState<TimeAggregation>("daily");

  const chartData = useMemo(() => {
    const finalized = data.filter(
      (state) =>
        state.annotationStatus === FileAnnotationStatus.ANNOTATED ||
        state.annotationStatus === FileAnnotationStatus.FAILED
    );

    const buckets = new Map<string, number>();

    for (const state of finalized) {
      const date = state.lastUpdatedTime;
      let key: string;

      switch (timeAgg) {
        case "hourly":
          key = new Date(
            date.getFullYear(),
            date.getMonth(),
            date.getDate(),
            date.getHours()
          ).toISOString();
          break;
        case "weekly":
          const weekStart = new Date(date);
          weekStart.setDate(date.getDate() - date.getDay());
          key = new Date(
            weekStart.getFullYear(),
            weekStart.getMonth(),
            weekStart.getDate()
          ).toISOString();
          break;
        default:
          key = new Date(
            date.getFullYear(),
            date.getMonth(),
            date.getDate()
          ).toISOString();
      }

      buckets.set(key, (buckets.get(key) || 0) + 1);
    }

    return Array.from(buckets.entries())
      .map(([date, count]) => ({
        date: new Date(date),
        count,
        label: new Date(date).toLocaleDateString(undefined, {
          month: "short",
          day: "numeric",
        }),
      }))
      .sort((a, b) => a.date.getTime() - b.date.getTime());
  }, [data, timeAgg]);

  return (
    <Card>
      <CardHeader className="pb-2 flex flex-row items-center justify-between space-y-0">
        <CardTitle className="text-sm font-semibold flex items-center gap-2">
          <Activity className="h-4 w-4 text-muted-foreground" />
          Pipeline Throughput
        </CardTitle>
        <Select value={timeAgg} onValueChange={(value) => setTimeAgg(value as TimeAggregation)}>
          <SelectTrigger className="w-24 h-7 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="hourly">Hourly</SelectItem>
            <SelectItem value="daily">Daily</SelectItem>
            <SelectItem value="weekly">Weekly</SelectItem>
          </SelectContent>
        </Select>
      </CardHeader>
      <CardContent className="pt-2">
        {chartData.length === 0 ? (
          <div className="flex items-center justify-center h-[200px] text-sm text-muted-foreground">
            No files have been finalized yet
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={chartData} margin={{ left: -10, right: 10 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
              <XAxis
                dataKey="label"
                tick={{ fontSize: 10, fill: "hsl(var(--muted-foreground))" }}
                tickLine={false}
                axisLine={false}
              />
              <YAxis
                tick={{ fontSize: 10, fill: "hsl(var(--muted-foreground))" }}
                tickLine={false}
                axisLine={false}
              />
              <Tooltip
                cursor={{ fill: "hsl(var(--muted) / 0.3)" }}
                content={({ active, payload }) => {
                  if (!active || !payload?.[0]) return null;
                  const payloadData = payload[0].payload;
                  return (
                    <div className="bg-popover border rounded-lg shadow-xl p-3 text-xs">
                      <p className="font-semibold">{payloadData.label}</p>
                      <p className="text-muted-foreground mt-0.5">
                        Files finalized: <span className="font-medium text-foreground">{payloadData.count}</span>
                      </p>
                    </div>
                  );
                }}
              />
              <Bar dataKey="count" fill="hsl(var(--primary))" radius={[4, 4, 0, 0]} maxBarSize={32} />
            </BarChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}
