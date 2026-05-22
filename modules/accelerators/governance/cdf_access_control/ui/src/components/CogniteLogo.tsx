import logoDarkUrl from "../../../../../shared/brand/cognite-logo-dark.svg";
import logoLightUrl from "../../../../../shared/brand/cognite-logo-light.svg";
import "../../../../../shared/brand/cognite-logo.css";

type Props = {
  className?: string;
};

/** Cognite wordmark (light/dark variants) for module header. */
export function CogniteLogo({ className = "" }: Props) {
  return (
    <a
      href="https://www.cognite.com"
      className={`cognite-logo${className ? ` ${className}` : ""}`}
      target="_blank"
      rel="noopener noreferrer"
      aria-label="Cognite"
    >
      <img src={logoLightUrl} alt="" className="cognite-logo__img cognite-logo__img--light" />
      <img src={logoDarkUrl} alt="" className="cognite-logo__img cognite-logo__img--dark" />
    </a>
  );
}
