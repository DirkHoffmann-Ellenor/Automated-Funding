"use client";

import * as React from "react";
import { cn } from "../../lib/utils";

interface ProgressProps extends React.HTMLAttributes<HTMLDivElement> {
  value?: number;
}

const Progress = React.forwardRef<HTMLDivElement, ProgressProps>(({ className, value = 0, ...props }, ref) => (
  <div
    ref={ref}
    className={cn(
      "relative h-2 w-full overflow-hidden rounded-full bg-neutral-200",
      className
    )}
    {...props}
  >
    <div
      className="h-full w-full origin-left rounded-full bg-orange-500 transition-transform"
      style={{ transform: `scaleX(${Math.min(Math.max(value, 0), 100) / 100})` }}
    />
  </div>
));
Progress.displayName = "Progress";

export { Progress };
