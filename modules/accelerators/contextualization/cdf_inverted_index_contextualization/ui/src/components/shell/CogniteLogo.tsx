import { useAppSettings } from "../../context/AppSettingsContext";

type Props = {
  className?: string;
};

export function CogniteLogo({ className = "" }: Props) {
  const { t } = useAppSettings();
  return (
    <a
      href="https://www.cognite.com"
      className={`cognite-logo${className ? ` ${className}` : ""}`}
      target="_blank"
      rel="noopener noreferrer"
      aria-label={t("brand.cognite")}
    >
      <img src="/brand/cognite-logo-light.svg" alt="" className="cognite-logo__img cognite-logo__img--light" />
      <img src="/brand/cognite-logo-dark.svg" alt="" className="cognite-logo__img cognite-logo__img--dark" />
    </a>
  );
}
