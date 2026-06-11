import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import type { GroupedCoverage } from "@/shared/utils/types";
import { BarChart2 } from "lucide-react";

interface CoverageBarChartProps {
  title: string;
  data: GroupedCoverage[];
  xAxisLabel?: string;
}

export function CoverageBarChart({
  title,
  data,
}: CoverageBarChartProps) {
  const chartData = data.map((item) => ({
    name: item.groupKey || "Unknown",
    coverage: item.coveragePct,
    actual: item.actualCount,
    potential: item.potentialCount,
    total: item.totalPossible,
  }));

  const getBarColor = (value: number) => {
    if (value >= 80) return "hsl(152, 69%, 40%)"; // emerald
    if (value >= 50) return "hsl(38, 92%, 50%)"; // amber
    return "hsl(0, 72%, 51%)"; // red
  };

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold flex items-center gap-2">
          <BarChart2 className="h-4 w-4 text-muted-foreground" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent className="pt-2">
        {data.length === 0 ? (
          <div className="flex items-center justify-center h-[200px] text-sm text-muted-foreground">
            No data available
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={Math.max(200, data.length * 40)}>
            <BarChart data={chartData} layout="vertical" margin={{ left: 0, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="hsl(var(--border))" />
              <XAxis
                type="number"
                domain={[0, 100]}
                tickFormatter={(value) => `${value}%`}
                tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }}
                tickLine={false}
                axisLine={false}
              />
              <YAxis
                type="category"
                dataKey="name"
                width={120}
                tick={{ fontSize: 11, fill: "hsl(var(--foreground))" }}
                tickLine={false}
                axisLine={false}
              />
              <Tooltip
                cursor={{ fill: "hsl(var(--muted) / 0.3)" }}
                content={({ active, payload }) => {
                  if (!active || !payload?.[0]) return null;
                  const payloadData = payload[0].payload;
                  return (
                    <div className="bg-popover border rounded-lg shadow-xl p-3 text-xs space-y-1">
                      <p className="font-semibold text-sm">{payloadData.name}</p>
                      <div className="grid grid-cols-2 gap-x-4 gap-y-0.5 text-muted-foreground">
                        <span>Coverage:</span>
                        <span className="font-medium text-foreground">{payloadData.coverage.toFixed(1)}%</span>
                        <span>Actual:</span>
                        <span className="font-medium text-foreground">{payloadData.actual.toLocaleString()}</span>
                        <span>Potential:</span>
                        <span className="font-medium text-foreground">{payloadData.potential.toLocaleString()}</span>
                        <span>Total:</span>
                        <span className="font-medium text-foreground">{payloadData.total.toLocaleString()}</span>
                      </div>
                    </div>
                  );
                }}
              />
              <Bar dataKey="coverage" radius={[0, 4, 4, 0]} maxBarSize={24}>
                {chartData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={getBarColor(entry.coverage)} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}
