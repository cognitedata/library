/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      screens: {
        xs: "480px", // Target mobile breakpoint
        tablet: "760px", // Target tablet breakpoint
      },
    },
  },
  plugins: [],
  registries: {
    "@aura": "https://cognitedata.github.io/aura/r/{name}.json",
  },
};
