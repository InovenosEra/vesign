import i18n from 'i18next'
import { initReactI18next } from 'react-i18next'
import en from './locales/en.json'
import he from './locales/he.json'
import es from './locales/es.json'
import fr from './locales/fr.json'
import de from './locales/de.json'
import it from './locales/it.json'

const savedLang = localStorage.getItem('lang') || 'en'

i18n.use(initReactI18next).init({
  resources: {
    en: { translation: en },
    he: { translation: he },
    es: { translation: es },
    fr: { translation: fr },
    de: { translation: de },
    it: { translation: it },
  },
  lng: savedLang,
  fallbackLng: 'en',
  interpolation: { escapeValue: false },
})

// Apply RTL direction on initial load
document.documentElement.dir = savedLang === 'he' ? 'rtl' : 'ltr'
document.documentElement.lang = savedLang

export default i18n
