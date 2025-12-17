import path from "path"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

const isDev = process.env.NODE_ENV === "development"
console.log("Vite Config - __dirname:", __dirname)
console.log("Vite Config - Content Base:", isDev ? "/" : "./")

export default defineConfig({
  plugins: [react()],
  base: isDev ? '/' : './',
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
})
