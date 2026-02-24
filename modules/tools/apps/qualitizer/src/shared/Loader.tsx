import { useEffect, useRef, useState } from "react";
import { useI18n } from "@/shared/i18n";

type LoaderProps = {
  open: boolean;
  onClose: () => void;
  title?: string;
};

export function Loader({ open, onClose, title }: LoaderProps) {
  const { t } = useI18n();
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const rafRef = useRef<number | null>(null);
  const zoomRef = useRef(1);
  const phaseRef = useRef(0);
  const lastRenderRef = useRef(0);
  const [dismissedForever, setDismissedForever] = useState(() => {
    if (typeof window === "undefined") return false;
    return window.localStorage.getItem("qualitizer.loader.dismissed") === "true";
  });

  useEffect(() => {
    if (!open) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const resize = () => {
      const rect = canvas.getBoundingClientRect();
      canvas.width = Math.max(1, Math.floor(rect.width));
      canvas.height = Math.max(1, Math.floor(rect.height));
    };

    const render = () => {
      const now = performance.now();
      if (now - lastRenderRef.current < 120) return;
      lastRenderRef.current = now;

      const width = canvas.width;
      const height = canvas.height;
      const image = ctx.createImageData(width, height);
      const maxIter = 64;
      const phase = phaseRef.current;
      const centerX = -0.743643887037151 + Math.cos(phase) * 0.006;
      const centerY = 0.13182590420533 + Math.sin(phase * 0.85) * 0.004;
      const scale = 3.5 / zoomRef.current;
      for (let py = 0; py < height; py += 1) {
        const y0 = centerY + (py / height - 0.5) * scale;
        for (let px = 0; px < width; px += 1) {
          const x0 = centerX + (px / width - 0.5) * scale;
          let x = 0;
          let y = 0;
          let iter = 0;
          while (x * x + y * y <= 4 && iter < maxIter) {
            const xTemp = x * x - y * y + x0;
            y = 2 * x * y + y0;
            x = xTemp;
            iter += 1;
          }
          const idx = (py * width + px) * 4;
          if (iter === maxIter) {
            image.data[idx] = 10;
            image.data[idx + 1] = 10;
            image.data[idx + 2] = 16;
            image.data[idx + 3] = 255;
          } else {
            const shade = Math.floor((iter / maxIter) * 255);
            image.data[idx] = shade;
            image.data[idx + 1] = 120;
            image.data[idx + 2] = 255 - shade;
            image.data[idx + 3] = 255;
          }
        }
      }
      ctx.putImageData(image, 0, 0);

      // If the frame is nearly a single color, reset the zoom.
      const sampleStep = Math.max(1, Math.floor((width * height) / 1000));
      let min = 255;
      let max = 0;
      for (let i = 0; i < image.data.length; i += sampleStep * 4) {
        const value = image.data[i];
        min = Math.min(min, value);
        max = Math.max(max, value);
      }
      if (max - min < 8) {
        zoomRef.current = 1;
        phaseRef.current = Math.random() * Math.PI * 2;
      }
    };

    const tick = () => {
      zoomRef.current *= 1.006;
      if (zoomRef.current > 3e5) {
        zoomRef.current = 1;
      }
      phaseRef.current += 0.0012;
      render();
      rafRef.current = requestAnimationFrame(tick);
    };

    resize();
    render();
    rafRef.current = requestAnimationFrame(tick);
    window.addEventListener("resize", resize);

    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      window.removeEventListener("resize", resize);
    };
  }, [open]);

  if (!open || dismissedForever) return null;

  const resolvedTitle = title ?? t("shared.loader.title");

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-3xl rounded-lg bg-white p-6 shadow-xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-center justify-between gap-4">
          <div>
            <h3 className="text-lg font-semibold text-slate-900">{resolvedTitle}</h3>
            <p className="text-sm text-slate-500">
              {t("shared.loader.description")}
            </p>
          </div>
          <div className="flex flex-col items-end gap-2">
            <button
              type="button"
              className="rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
              onClick={onClose}
            >
              {t("shared.loader.dismissOnce")}
            </button>
            <button
              type="button"
              className="rounded-md bg-slate-900 px-3 py-1 text-sm text-white hover:bg-slate-800"
              onClick={() => {
                if (typeof window !== "undefined") {
                  window.localStorage.setItem("qualitizer.loader.dismissed", "true");
                }
                setDismissedForever(true);
                onClose();
              }}
            >
              {t("shared.loader.dismissForever")}
            </button>
          </div>
        </div>
        <div className="mt-4 h-80 w-full overflow-hidden rounded-md border border-slate-200 bg-black">
          <canvas ref={canvasRef} className="h-full w-full" />
        </div>
      </div>
    </div>
  );
}
