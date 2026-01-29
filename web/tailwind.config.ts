import type { Config } from 'tailwindcss'

const config: Config = {
  content: [
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        spark: {
          orange: '#E25A1C',
          'orange-light': '#FF7043',
          'orange-dark': '#BF4A15',
        },
        bg: {
          darkest: '#050709',
          dark: '#0A0D12',
          medium: '#141A24',
          light: '#1E2633',
        },
        text: {
          primary: '#F0F2F6',
          secondary: '#A8B2C4',
          muted: '#6B7A94',
        },
        node: {
          input: '#22C55E',
          transform: '#3B82F6',
          shuffle: '#E25A1C',
          action: '#F59E0B',
        },
        severity: {
          high: '#EF4444',
          medium: '#F59E0B',
          warning: '#FBBF24',
          info: '#3B82F6',
        },
      },
      fontFamily: {
        display: ['DM Serif Display', 'serif'],
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      },
      animation: {
        'pulse-glow': 'pulse-glow 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'fade-in': 'fade-in 0.3s ease-out',
        'slide-up': 'slide-up 0.3s ease-out',
      },
      keyframes: {
        'pulse-glow': {
          '0%, 100%': {
            opacity: '1',
            boxShadow: '0 0 20px rgba(226, 90, 28, 0.4)',
          },
          '50%': {
            opacity: '0.8',
            boxShadow: '0 0 40px rgba(226, 90, 28, 0.6)',
          },
        },
        'fade-in': {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        'slide-up': {
          '0%': { transform: 'translateY(10px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
      },
      boxShadow: {
        'glow-orange': '0 0 20px rgba(226, 90, 28, 0.3)',
        'glow-green': '0 0 15px rgba(34, 197, 94, 0.3)',
        'glow-blue': '0 0 15px rgba(59, 130, 246, 0.3)',
      },
    },
  },
  plugins: [],
}
export default config
