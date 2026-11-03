import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useMemo,
  useState,
} from "react";

export type NavigationPage = "summaries" | "tests" | "settings";

interface NavigationContextType {
  currentPage: NavigationPage;
  navigate: (page: NavigationPage) => void;
}

const NavigationContext = createContext<NavigationContextType | undefined>(
  undefined,
);

export function NavigationProvider({ children }: { children: ReactNode }) {
  const [currentPage, setCurrentPage] = useState<NavigationPage>("summaries");

  const navigate = useCallback((page: NavigationPage) => {
    setCurrentPage(page);
  }, []);

  const value = useMemo(
    () => ({ currentPage, navigate }),
    [currentPage, navigate],
  );

  return (
    <NavigationContext.Provider value={value}>
      {children}
    </NavigationContext.Provider>
  );
}

export function useNavigation() {
  const context = useContext(NavigationContext);
  if (context === undefined) {
    throw new Error("useNavigation must be used within a NavigationProvider");
  }
  return context;
}
