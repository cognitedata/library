import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
  LabelList,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { PieChart } from "lucide-react";
import type { ThresholdBucket } from "@/shared/utils/types";

interface ThresholdChartProps {
  title: string;
  data: ThresholdBucket[];
}

export function ThresholdChart({ title, data }: ThresholdChartProps) {
  const chartData = data.map((bucket) => ({
    name: bucket.label,
    percentage: bucket.percentage,
    count: bucket.count,
    color: bucket.color,
    emoji: bucket.emoji,
  }));

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold flex items-center gap-2">
          <PieChart className="h-4 w-4 text-muted-foreground" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent className="pt-2">
        <ResponsiveContainer width="100%" height={160}>
          <BarChart data={chartData} layout="vertical" margin={{ left: 0, right: 40 }}>
            <XAxis
              type="number"
              domain={[0, 100]}
              tickFormatter={(value) => `${value}%`}
              tick={{ fontSize: 10, fill: "hsl(var(--muted-foreground))" }}
              tickLine={false}
              axisLine={false}
            />
            <YAxis
              type="category"
              dataKey="name"
              width={90}
              tick={{ fontSize: 10, fill: "hsl(var(--foreground))" }}
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
                    <p className="font-semibold">
                      {payloadData.emoji} {payloadData.name}
                    </p>
                    <div className="grid grid-cols-2 gap-x-3 text-muted-foreground">
                      <span>Files:</span>
                      <span className="font-medium text-foreground">{payloadData.count.toLocaleString()}</span>
                      <span>Percent:</span>
                      <span className="font-medium text-foreground">{payloadData.percentage.toFixed(1)}%</span>
                    </div>
                  </div>
                );
              }}
            />
            <Bar dataKey="percentage" radius={[0, 4, 4, 0]} maxBarSize={20}>
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
              <LabelList
                dataKey="count"
                position="right"
                formatter={(value: unknown) =>
                  typeof value === "number" ? value.toLocaleString() : String(value)
                }
                style={{ fontSize: 10, fill: "hsl(var(--muted-foreground))" }}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
}
