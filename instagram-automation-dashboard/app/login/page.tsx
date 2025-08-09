"use client"

import type React from "react"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Checkbox } from "@/components/ui/checkbox"
import { Bot, Shield, Zap, Users } from "lucide-react"

export default function LoginPage() {
  const [isLoading, setIsLoading] = useState(false)

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)
    // Simulate login
    setTimeout(() => {
      setIsLoading(false)
      window.location.href = "/"
    }, 2000)
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 flex items-center justify-center p-4">
      <div className="w-full max-w-6xl grid lg:grid-cols-2 gap-8 items-center">
        {/* Left side - Branding */}
        <div className="hidden lg:block space-y-8">
          <div className="space-y-4">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-gradient-to-r from-blue-500 to-purple-500 rounded-xl flex items-center justify-center">
                <Bot className="h-7 w-7 text-white" />
              </div>
              <span className="text-3xl font-bold text-white">InstaHub</span>
            </div>
            <h1 className="text-5xl font-bold text-white leading-tight">
              Автоматизация
              <br />
              <span className="bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
                Instagram
              </span>
              <br />
              нового уровня
            </h1>
            <p className="text-xl text-slate-300 leading-relaxed">
              Управляйте до 100 аккаунтами одновременно с помощью ИИ и передовых технологий автоматизации
            </p>
          </div>

          <div className="space-y-6">
            {[
              { icon: Users, title: "До 100 аккаунтов", desc: "Массовое управление аккаунтами" },
              { icon: Zap, title: "ИИ автоматизация", desc: "Умные алгоритмы для безопасности" },
              { icon: Shield, title: "Максимальная защита", desc: "Прокси и антидетект системы" },
            ].map((feature, index) => (
              <div key={index} className="flex items-center gap-4">
                <div className="w-10 h-10 bg-slate-800/50 rounded-lg flex items-center justify-center">
                  <feature.icon className="h-5 w-5 text-blue-400" />
                </div>
                <div>
                  <h3 className="text-white font-semibold">{feature.title}</h3>
                  <p className="text-slate-400 text-sm">{feature.desc}</p>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Right side - Login Form */}
        <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
          <CardHeader className="space-y-1">
            <CardTitle className="text-2xl text-white">Вход в систему</CardTitle>
            <CardDescription className="text-slate-400">
              Введите ваши данные для доступа к панели управления
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={handleLogin} className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="email" className="text-slate-300">
                  Email
                </Label>
                <Input
                  id="email"
                  type="email"
                  placeholder="admin@instahub.com"
                  className="bg-slate-700 border-slate-600 text-white placeholder:text-slate-400"
                  required
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="password" className="text-slate-300">
                  Пароль
                </Label>
                <Input id="password" type="password" className="bg-slate-700 border-slate-600 text-white" required />
              </div>
              <div className="flex items-center space-x-2">
                <Checkbox id="remember" className="border-slate-600" />
                <Label htmlFor="remember" className="text-sm text-slate-300">
                  Запомнить меня
                </Label>
              </div>
              <Button
                type="submit"
                className="w-full bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700"
                disabled={isLoading}
              >
                {isLoading ? "Вход..." : "Войти в систему"}
              </Button>
            </form>

            <div className="mt-6 text-center">
              <p className="text-slate-400 text-sm">
                Нет доступа?
                <Button variant="link" className="text-blue-400 hover:text-blue-300 p-0 ml-1">
                  Связаться с поддержкой
                </Button>
              </p>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
