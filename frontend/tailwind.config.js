/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        navy:   '#0d2a5e',
        blue:   '#1a4a9e',
        sky:    '#4a90d9',
        cyan:   '#06b6d4',
        green:  '#16a34a',
        amber:  '#d97706',
        red:    '#dc2626',
        purple: '#7c3aed',
        slate:  '#64748b',
        rose:   '#e11d48',
        bg:     '#f5f7fc',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'Arial', 'sans-serif'],
      },
      boxShadow: {
        card:  '0 1px 12px rgba(13,42,94,0.08)',
        float: '0 4px 24px rgba(13,42,94,0.14)',
        deep:  '0 8px 40px rgba(7,15,34,0.24)',
      },
      borderRadius: {
        xl2: '1rem',
        xl3: '1.25rem',
      },
    },
  },
  plugins: [],
}
