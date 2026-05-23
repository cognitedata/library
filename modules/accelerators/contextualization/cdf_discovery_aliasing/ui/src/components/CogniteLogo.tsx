import logoDarkUrl from "../../../../../shared/brand/cognite-logo-dark.svg";
import logoLightUrl from "../../../../../shared/brand/cognite-logo-light.svg";
import "../../../../../shared/brand/cognite-logo.css";
import { useAppSettings } from "../context/AppSettingsContext";

type Props = {
  className?: string;
};

/** Cognite wordmark (light/dark variants) for module header. */
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
      <img src={logoLightUrl} alt="" className="cognite-logo__img cognite-logo__img--light" />
      <img src={logoDarkUrl} alt="" className="cognite-logo__img cognite-logo__img--dark" />
    </a>
  );
}
