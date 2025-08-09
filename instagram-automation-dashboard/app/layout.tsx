import type React from "react"
import type { Metadata } from "next"
import { Inter } from "next/font/google"
import "./globals.css"
import "./globals-override.css" // Добавляем наши переопределения стилей
import "./sidebar-custom.css"
import "./mobile-styles.css"
import { ThemeProvider } from "@/components/theme-provider"
import { AnimatedLayout } from "@/components/animations/animated-layout"
import { RouteChangeAnimation } from "@/components/animations/route-change-animation"

const inter = Inter({ subsets: ["latin", "cyrillic"] })

export const metadata: Metadata = {
  title: "Instagram Automation Dashboard",
  description: "Управление Instagram аккаунтами",
    generator: 'v0.dev'
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="ru" suppressHydrationWarning>
      <body className={`${inter.className} antialiased`}>
        <ThemeProvider attribute="class" defaultTheme="dark" enableSystem>
          <RouteChangeAnimation />
          <AnimatedLayout>{children}</AnimatedLayout>
        </ThemeProvider>
      </body>
    </html>
  )
}
