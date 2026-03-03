// Fix for Recharts type compatibility with React 18
import { ComponentType, SVGProps, ElementType } from 'react';

declare module 'recharts' {
  export const LineChart: ComponentType<any>;
  export const Line: ComponentType<any>;
  export const XAxis: ComponentType<any>;
  export const YAxis: ComponentType<any>;
  export const CartesianGrid: ComponentType<any>;
  export const Tooltip: ComponentType<any>;
  export const ResponsiveContainer: ComponentType<any>;
  export const BarChart: ComponentType<any>;
  export const Bar: ComponentType<any>;
  export const Legend: ComponentType<any>;
  export const AreaChart: ComponentType<any>;
  export const Area: ComponentType<any>;
}
