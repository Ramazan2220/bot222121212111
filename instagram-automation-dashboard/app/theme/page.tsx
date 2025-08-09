"use client"

import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Moon, Sun, Monitor, Palette, Check, Brush, Save } from "lucide-react"
import PageContainer from "@/components/page-container"
import { useTheme } from "@/hooks/use-theme"
import { motion } from "framer-motion"

export default function ThemePage() {
  const { theme, setTheme, mounted } = useTheme()
  const [primaryColor, setPrimaryColor] = useState("blue")
  const [savedTheme, setSavedTheme] = useState<string | null>(null)

  useEffect(() => {
    if (mounted && theme) {
      setSavedTheme(theme)
    }
  }, [mounted, theme])

  const themes = [
    { id: "light", name: "Светлая", icon: Sun },
    { id: "dark", name: "Темная", icon: Moon },
    { id: "system", name: "Системная", icon: Monitor },
  ]

  const colors = [
    { id: "blue", name: "Синий", class: "bg-blue-500" },
    { id: "purple", name: "Фиолетовый", class: "bg-purple-500" },
    { id: "green", name: "Зеленый", class: "bg-green-500" },
    { id: "red", name: "Красный", class: "bg-red-500" },
    { id: "orange", name: "Оранжевый", class: "bg-orange-500" },
    { id: "pink", name: "Розовый", class: "bg-pink-500" },
  ]

  const handleSaveSettings = () => {
    setTheme(savedTheme || "dark")
    // Здесь можно сохранить primaryColor и другие настройки
  }

  const actions = (
    <Button className="bg-blue-600 hover:bg-blue-700" onClick={handleSaveSettings}>
      <Save className="h-5 w-5 mr-2" />
      Сохранить настройки
    </Button>
  )

  if (!mounted) {
    return null
  }

  return (
    <PageContainer
      title="Настройки темы"
      description="Настройте внешний вид интерфейса под свои предпочтения"
      actions={actions}
      centerContent={true}
    >
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 w-full max-w-4xl">
        <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm dark:bg-slate-800/50 dark:border-slate-700 light:bg-white light:border-slate-200">
          <CardHeader>
            <CardTitle className="text-white flex items-center gap-2 text-xl dark:text-white light:text-slate-900">
              <Moon className="h-6 w-6 text-blue-400" />
              Выбор темы
            </CardTitle>
            <CardDescription className="text-slate-400 dark:text-slate-400 light:text-slate-500">
              Выберите предпочитаемую тему оформления
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="grid grid-cols-3 gap-4">
              {themes.map((themeOption) => {
                const Icon = themeOption.icon
                const isActive = savedTheme === themeOption.id
                return (
                  <motion.div
                    key={themeOption.id}
                    className={`relative flex flex-col items-center justify-center p-6 rounded-lg cursor-pointer border ${
                      isActive
                        ? "bg-blue-600/20 border-blue-500"
                        : "bg-slate-700/50 border-slate-600 hover:bg-slate-700 dark:bg-slate-700/50 dark:border-slate-600 dark:hover:bg-slate-700 light:bg-slate-100 light:border-slate-300 light:hover:bg-slate-200"
                    }`}
                    onClick={() => setSavedTheme(themeOption.id)}
                    whileHover={{ scale: 1.03 }}
                    whileTap={{ scale: 0.97 }}
                  >
                    <div
                      className={`w-16 h-16 rounded-full flex items-center justify-center mb-4 ${
                        isActive ? "bg-blue-600" : "bg-slate-600 dark:bg-slate-600 light:bg-slate-400"
                      }`}
                    >
                      <Icon className="h-8 w-8 text-white" />
                    </div>
                    <p className="text-white dark:text-white light:text-slate-900 text-lg">{themeOption.name}</p>
                    {isActive && (
                      <div className="absolute top-2 right-2">
                        <Check className="h-5 w-5 text-blue-400" />
                      </div>
                    )}
                  </motion.div>
                )
              })}
            </div>

            <div className="p-6 rounded-lg bg-slate-700/50 border border-slate-600 dark:bg-slate-700/50 dark:border-slate-600 light:bg-slate-100 light:border-slate-300">
              <h3 className="text-lg text-white font-medium mb-4 dark:text-white light:text-slate-900">
                Автоматическое переключение
              </h3>
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-slate-300 dark:text-slate-300 light:text-slate-700">
                    Переключать тему по времени суток
                  </p>
                  <p className="text-slate-400 text-sm dark:text-slate-400 light:text-slate-500">
                    Светлая тема днем, темная ночью
                  </p>
                </div>
                <div className="flex items-center h-6">
                  <div className="w-14 h-7 rounded-full bg-slate-600 relative dark:bg-slate-600 light:bg-slate-300">
                    <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 left-0.5"></div>
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm dark:bg-slate-800/50 dark:border-slate-700 light:bg-white light:border-slate-200">
          <CardHeader>
            <CardTitle className="text-white flex items-center gap-2 text-xl dark:text-white light:text-slate-900">
              <Palette className="h-6 w-6 text-blue-400" />
              Цветовая схема
            </CardTitle>
            <CardDescription className="text-slate-400 dark:text-slate-400 light:text-slate-500">
              Настройте основные цвета интерфейса
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-6">
            <div>
              <h3 className="text-lg text-white font-medium mb-4 dark:text-white light:text-slate-900">
                Основной цвет
              </h3>
              <div className="grid grid-cols-3 gap-4">
                {colors.map((color) => (
                  <motion.div
                    key={color.id}
                    className={`relative flex flex-col items-center p-4 rounded-lg cursor-pointer border ${
                      primaryColor === color.id
                        ? "bg-slate-700/70 border-slate-500"
                        : "bg-slate-700/50 border-slate-600 hover:bg-slate-700 dark:bg-slate-700/50 dark:border-slate-600 dark:hover:bg-slate-700 light:bg-slate-100 light:border-slate-300 light:hover:bg-slate-200"
                    }`}
                    onClick={() => setPrimaryColor(color.id)}
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                  >
                    <div className={`w-10 h-10 rounded-full mb-2 ${color.class}`}></div>
                    <p className="text-white dark:text-white light:text-slate-900">{color.name}</p>
                    {primaryColor === color.id && (
                      <div className="absolute top-2 right-2">
                        <Check className="h-5 w-5 text-blue-400" />
                      </div>
                    )}
                  </motion.div>
                ))}
              </div>
            </div>

            <div className="p-6 rounded-lg bg-slate-700/50 border border-slate-600 dark:bg-slate-700/50 dark:border-slate-600 light:bg-slate-100 light:border-slate-300">
              <h3 className="text-lg text-white font-medium mb-4 dark:text-white light:text-slate-900">
                Дополнительные настройки
              </h3>
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-slate-300 dark:text-slate-300 light:text-slate-700">Высокая контрастность</p>
                    <p className="text-slate-400 text-sm dark:text-slate-400 light:text-slate-500">
                      Повышенная читаемость
                    </p>
                  </div>
                  <div className="flex items-center h-6">
                    <div className="w-14 h-7 rounded-full bg-slate-600 relative dark:bg-slate-600 light:bg-slate-300">
                      <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 left-0.5"></div>
                    </div>
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-slate-300 dark:text-slate-300 light:text-slate-700">Анимации интерфейса</p>
                    <p className="text-slate-400 text-sm dark:text-slate-400 light:text-slate-500">Плавные переходы</p>
                  </div>
                  <div className="flex items-center h-6">
                    <div className="w-14 h-7 rounded-full bg-blue-600 relative">
                      <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 right-0.5"></div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm md:col-span-2 dark:bg-slate-800/50 dark:border-slate-700 light:bg-white light:border-slate-200">
          <CardHeader>
            <CardTitle className="text-white flex items-center gap-2 text-xl dark:text-white light:text-slate-900">
              <Brush className="h-6 w-6 text-blue-400" />
              Предпросмотр
            </CardTitle>
            <CardDescription className="text-slate-400 dark:text-slate-400 light:text-slate-500">
              Так будет выглядеть интерфейс с выбранными настройками
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="rounded-lg border border-slate-600 overflow-hidden dark:border-slate-600 light:border-slate-300">
              <div className="bg-slate-800 p-4 border-b border-slate-600 flex items-center justify-between dark:bg-slate-800 dark:border-slate-600 light:bg-slate-100 light:border-slate-300">
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 rounded-full bg-red-500"></div>
                  <div className="w-3 h-3 rounded-full bg-yellow-500"></div>
                  <div className="w-3 h-3 rounded-full bg-green-500"></div>
                </div>
                <div className="text-slate-400 dark:text-slate-400 light:text-slate-500">Предпросмотр интерфейса</div>
                <div></div>
              </div>
              <div className={`p-6 ${savedTheme === "light" ? "bg-slate-100" : "bg-slate-900"} flex gap-4`}>
                <div
                  className={`w-1/4 ${
                    savedTheme === "light" ? "bg-white border-slate-200" : "bg-slate-800 border-slate-700"
                  } rounded-lg p-3 space-y-2 border`}
                >
                  <div
                    className={`h-6 ${savedTheme === "light" ? "bg-slate-200" : "bg-slate-700"} rounded w-3/4`}
                  ></div>
                  <div
                    className={`h-4 ${savedTheme === "light" ? "bg-slate-200" : "bg-slate-700"} rounded w-full`}
                  ></div>
                  <div
                    className={`h-4 ${savedTheme === "light" ? "bg-slate-200" : "bg-slate-700"} rounded w-full`}
                  ></div>
                  <div
                    className={`h-4 ${savedTheme === "light" ? "bg-slate-200" : "bg-slate-700"} rounded w-full`}
                  ></div>
                  <div
                    className={`h-4 ${savedTheme === "light" ? "bg-slate-200" : "bg-slate-700"} rounded w-full`}
                  ></div>
                </div>
                <div className="w-3/4 space-y-4">
                  <div
                    className={`h-8 ${
                      savedTheme === "light" ? "bg-white border-slate-200" : "bg-slate-800 border-slate-700"
                    } rounded w-1/3 border`}
                  ></div>
                  <div className="grid grid-cols-3 gap-4">
                    <div
                      className={`h-24 ${
                        savedTheme === "light" ? "bg-white border-slate-200" : "bg-slate-800 border-slate-700"
                      } rounded border`}
                    ></div>
                    <div
                      className={`h-24 ${
                        savedTheme === "light" ? "bg-white border-slate-200" : "bg-slate-800 border-slate-700"
                      } rounded border`}
                    ></div>
                    <div
                      className={`h-24 ${
                        savedTheme === "light" ? "bg-white border-slate-200" : "bg-slate-800 border-slate-700"
                      } rounded border`}
                    ></div>
                  </div>
                  <div
                    className={`h-40 ${
                      savedTheme === "light" ? "bg-white border-slate-200" : "bg-slate-800 border-slate-700"
                    } rounded border`}
                  ></div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </PageContainer>
  )
}
