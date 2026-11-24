/**
 * Fluid horizontal inset for shell chrome and page content.
 * Keeps a ~90rem max content width on large viewports (side space grows with viewport)
 * and at least 1rem padding on smaller screens.
 */
export const appContentHorizontalInsetClassName =
  'px-[max(1rem,calc((100%-90rem)/2))]';
