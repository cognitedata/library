import type { ReactNode } from "react";

type Props = {
  children: ReactNode;
  sticky?: boolean;
};

export function ActionsBar({ children, sticky }: Props) {
  return (
    <div className={sticky ? "idx-actions-bar idx-actions-bar--sticky" : "idx-actions-bar"}>
      {children}
    </div>
  );
}
