import atlasSparkleIcon from './atlas-sparkle.png';

type AtlasSparklesIconProps = {
  filled?: boolean;
};

export function AtlasSparklesIcon({ filled = false }: AtlasSparklesIconProps) {
  return (
    <img
      src={atlasSparkleIcon}
      className={filled ? 'size-3.5 invert' : 'size-3.5'}
      alt=""
      aria-hidden="true"
    />
  );
}
