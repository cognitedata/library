import { useState } from "react";
import { useI18n } from "@/shared/i18n";
import { InfrastructureChecks } from "./InfrastructureChecks";
import { TransformationsChecks } from "./TransformationsChecks";
import { DataModelChecks } from "./DataModelChecks";
import { RawChecks } from "./RawChecks";
import { PermissionsChecks } from "./PermissionsChecks";
import { HealthChecksAll } from "./HealthChecksAll";

type CheckCategory = "landing" | "all" | "infrastructure" | "transformations" | "dataModels" | "raw" | "permissions";

const categories: Array<{
  id: Exclude<CheckCategory, "landing" | "all">;
  title: string;
  description: string;
  icon: string;
}> = [
  {
    id: "infrastructure",
    title: "Infrastructure",
    description: "Functions and scheduling overlaps",
    icon: "⚙️",
  },
  {
    id: "transformations",
    title: "Transformations",
    description: "Write efficiency and data model version consistency",
    icon: "🔄",
  },
  {
    id: "dataModels",
    title: "Data Models",
    description: "Unused views, containers, spaces, and views without containers",
    icon: "🗂️",
  },
  {
    id: "raw",
    title: "Raw Tables",
    description: "Empty tables, stale data, and Raw API availability",
    icon: "📋",
  },
  {
    id: "permissions",
    title: "Permissions",
    description: "Permission scope drift between security groups",
    icon: "🔐",
  },
];

export function HealthChecks() {
  const { t } = useI18n();
  const [activeCategory, setActiveCategory] = useState<CheckCategory>("landing");

  const goBack = () => setActiveCategory("landing");

  if (activeCategory === "all") return <HealthChecksAll onBack={goBack} />;
  if (activeCategory === "infrastructure") return <InfrastructureChecks onBack={goBack} />;
  if (activeCategory === "transformations") return <TransformationsChecks onBack={goBack} />;
  if (activeCategory === "dataModels") return <DataModelChecks onBack={goBack} />;
  if (activeCategory === "raw") return <RawChecks onBack={goBack} />;
  if (activeCategory === "permissions") return <PermissionsChecks onBack={goBack} />;

  const visibleCategories = categories;

  return (
    <section className="flex flex-col gap-6">
      <header className="flex flex-col gap-1">
        <h2 className="text-2xl font-semibold text-slate-900">
          {t("healthChecks.title")}
        </h2>
        <p className="text-sm text-slate-500">{t("healthChecks.subtitle")}</p>
      </header>

      <button
        type="button"
        className="cursor-pointer self-start rounded-md bg-slate-900 px-5 py-2.5 text-sm font-medium text-white hover:bg-slate-800 transition"
        onClick={() => setActiveCategory("all")}
      >
        Run all checks
      </button>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        {visibleCategories.map((cat) => (
          <button
            key={cat.id}
            type="button"
            className="cursor-pointer flex items-start gap-4 rounded-lg border border-slate-200 bg-white p-5 text-left transition hover:border-slate-300 hover:shadow-sm"
            onClick={() => setActiveCategory(cat.id)}
          >
            <span className="text-2xl">{cat.icon}</span>
            <div className="flex flex-col gap-1">
              <span className="text-base font-semibold text-slate-900">
                {cat.title}
              </span>
              <span className="text-sm text-slate-500">{cat.description}</span>
            </div>
          </button>
        ))}
      </div>
    </section>
  );
}
