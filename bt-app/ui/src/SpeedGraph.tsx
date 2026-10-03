interface SpeedGraphProps {
  history: number[];
  width?: number;
  height?: number;
}

export default function SpeedGraph({ history, width = 260, height = 44 }: SpeedGraphProps) {
  const max = Math.max(...history, 1);
  const points = history.map((value, index) => {
    const x = (index / Math.max(history.length - 1, 1)) * width;
    const y = height - (value / max) * (height - 4) - 2;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const line = points.length > 0 ? points.join(" ") : `0,${height} ${width},${height}`;
  const area = `0,${height} ${line} ${width},${height}`;
  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className="border border-border bg-surface"
      role="img"
      aria-label={`Download speed history, current ${max} bytes per second peak`}
    >
      <polygon points={area} className="fill-accent/10" />
      <polyline
        points={line}
        fill="none"
        strokeWidth="1.5"
        strokeLinejoin="round"
        className="stroke-accent"
      />
    </svg>
  );
}
