export interface SunburstData {
  name: string;
  value?: number;
  children?: SunburstData[];
  id: number;
  coverage: number;
  level: number;
}

export interface SunburstProps {
  width?: number;
  height?: number;
  title?: string;
  data?: SunburstData;
}
