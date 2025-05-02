module.exports = {
  content: [
    './js/**/*.js', // Looks in assets/js
    '../lib/frontend_web.ex', // Main web module
    '../lib/frontend_web/**/*.*ex' // Looks in components, live, controllers etc. for .ex and .heex
  ],
  theme: {
    extend: {},
  },
  plugins: [
    require('@tailwindcss/forms')
  ]
}