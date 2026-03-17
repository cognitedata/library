import { createContext, useContext, useCallback, useState } from "react";

type NavigationContextValue = {
  transformationToSelect: string | null;
  navigateToTransformation: (id: string) => void;
  clearTransformationToSelect: () => void;
};

const NavigationContext = createContext<NavigationContextValue | null>(null);

export function NavigationProvider({
  children,
  onNavigateToTransformations,
}: {
  children: React.ReactNode;
  onNavigateToTransformations: () => void;
}) {
  const [transformationToSelect, setTransformationToSelect] = useState<string | null>(null);

  const navigateToTransformation = useCallback(
    (id: string) => {
      setTransformationToSelect(id);
      onNavigateToTransformations();
    },
    [onNavigateToTransformations]
  );

  const clearTransformationToSelect = useCallback(() => {
    setTransformationToSelect(null);
  }, []);

  return (
    <NavigationContext.Provider
      value={{
        transformationToSelect,
        navigateToTransformation,
        clearTransformationToSelect,
      }}
    >
      {children}
    </NavigationContext.Provider>
  );
}

export function useNavigation() {
  const ctx = useContext(NavigationContext);
  return ctx;
}
