import type { ReactNode } from "react";

type Props = {
  children: ReactNode;
  variant?: "default" | "expert";
};

export function SectionIntro({ children, variant = "default" }: Props) {
  return (
    <p
      className={
        variant === "expert"
          ? "idx-section-intro idx-section-intro--expert"
          : "idx-section-intro"
      }
    >
      {children}
    </p>
  );
}
