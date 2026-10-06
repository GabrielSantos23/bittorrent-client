import { useEffect, useRef, useState } from "react";

import { AnimatedCounter } from "@/components/ui/animated-counter";

import { byteParts } from "../format";

// Live torrent data arrives several times a second; animating every digit on
// every event makes long lists laggy. Values commit to the counters at most
// once per interval (leading edge, plus one trailing update).
const SLOW_INTERVAL = 1000;

function useSlowValue(value: number, interval = SLOW_INTERVAL): number {
  const [slow, setSlow] = useState(value);
  const lastRef = useRef({ value, at: 0 });
  const timerRef = useRef<number | null>(null);

  useEffect(() => {
    if (value === lastRef.current.value) return;
    const elapsed = Date.now() - lastRef.current.at;
    if (elapsed >= interval) {
      lastRef.current = { value, at: Date.now() };
      setSlow(value);
      return;
    }
    timerRef.current = window.setTimeout(() => {
      timerRef.current = null;
      lastRef.current = { value, at: Date.now() };
      setSlow(value);
    }, interval - elapsed);
    return () => {
      if (timerRef.current !== null) {
        clearTimeout(timerRef.current);
        timerRef.current = null;
      }
    };
  }, [value, interval]);

  return slow;
}

export function AnimatedRate({
  bytes,
  className,
}: {
  bytes: number;
  className?: string;
}) {
  const { value, unit } = byteParts(useSlowValue(bytes));
  return (
    <AnimatedCounter
      value={value}
      decimals={2}
      suffix={"\u00A0" + unit + "/s"}
      className={className}
    />
  );
}

export function AnimatedBytes({
  bytes,
  className,
}: {
  bytes: number;
  className?: string;
}) {
  const { value, unit } = byteParts(useSlowValue(bytes));
  return (
    <AnimatedCounter
      value={value}
      decimals={2}
      suffix={"\u00A0" + unit}
      className={className}
    />
  );
}

export function AnimatedRatio({
  ratio,
  className,
}: {
  ratio: number;
  className?: string;
}) {
  return (
    <AnimatedCounter
      value={useSlowValue(ratio)}
      decimals={2}
      // no grouping: wide ratios must stay inside the fixed table column
      separator=""
      suffix="×"
      className={className}
    />
  );
}

export function AnimatedPercent({
  value,
  className,
}: {
  value: number;
  className?: string;
}) {
  return (
    <AnimatedCounter
      value={useSlowValue(value)}
      decimals={1}
      suffix="%"
      className={className}
    />
  );
}

export function AnimatedCount({
  value,
  className,
}: {
  value: number;
  className?: string;
}) {
  return (
    <AnimatedCounter
      value={useSlowValue(value)}
      className={className}
    />
  );
}
