"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Users, Globe, Upload, TrendingUp, Bot, Activity, Zap, BarChart3 } from "lucide-react"
import PageContainer from "@/components/page-container"

export default function Dashboard() {
  const [activeAccounts] = useState(47)
  const [totalPosts] = useState(1247)
  const [successRate] = useState(94.2)

  return (
    <PageContainer
      title="Instagram Automation Hub"
      description="Управляйте своими Instagram аккаунтами с максимальной эффективностью"
    >
      {/* Stats Overview */}
      <div className="grid grid-cols-4 gap-4 mb-6">
        <Card className="bg-slate-800/50 border-slate-700">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-blue-500/20 rounded-full flex items-center justify-center">
                <Users className="h-6 w-6 text-blue-400" />
              </div>
              <div>
                <p className="text-2xl font-bold text-white">{activeAccounts}</p>
                <p className="text-slate-400">Активных аккаунтов</p>
                <Badge className="mt-1 bg-green-600">+12%</Badge>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-slate-700">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-green-500/20 rounded-full flex items-center justify-center">
                <Upload className="h-6 w-6 text-green-400" />
              </div>
              <div>
                <p className="text-2xl font-bold text-white">{totalPosts}</p>
                <p className="text-slate-400">Публикаций сегодня</p>
                <Badge className="mt-1 bg-green-600">+8%</Badge>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-slate-700">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-purple-500/20 rounded-full flex items-center justify-center">
                <TrendingUp className="h-6 w-6 text-purple-400" />
              </div>
              <div>
                <p className="text-2xl font-bold text-white">{successRate}%</p>
                <p className="text-slate-400">Успешность</p>
                <Badge className="mt-1 bg-green-600">+2.1%</Badge>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-slate-700">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-orange-500/20 rounded-full flex items-center justify-center">
                <Globe className="h-6 w-6 text-orange-400" />
              </div>
              <div>
                <p className="text-2xl font-bold text-white">23</p>
                <p className="text-slate-400">Активных прокси</p>
                <Badge className="mt-1 bg-green-600">100%</Badge>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Quick Actions and System Status */}
      <div className="grid grid-cols-2 gap-6 mb-6">
        <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
          <CardHeader>
            <CardTitle className="text-white flex items-center gap-2 text-xl">
              <Zap className="h-6 w-6 text-yellow-400" />
              Быстрые действия
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <Button className="w-full justify-start bg-blue-600 hover:bg-blue-700 text-lg py-6">
              <Users className="h-5 w-5 mr-3" />
              Добавить аккаунты
            </Button>
            <Button className="w-full justify-start bg-purple-600 hover:bg-purple-700 text-lg py-6">
              <Upload className="h-5 w-5 mr-3" />
              Массовая публикация
            </Button>
            <Button className="w-full justify-start bg-green-600 hover:bg-green-700 text-lg py-6">
              <Bot className="h-5 w-5 mr-3" />
              Запустить прогрев
            </Button>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
          <CardHeader>
            <CardTitle className="text-white flex items-center gap-2 text-xl">
              <Activity className="h-6 w-6 text-green-400" />
              Системный статус
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <div className="flex justify-between">
                <span className="text-slate-300">Загрузка CPU</span>
                <span className="text-white">23%</span>
              </div>
              <Progress value={23} className="h-2" />
            </div>
            <div className="space-y-2">
              <div className="flex justify-between">
                <span className="text-slate-300">Использование RAM</span>
                <span className="text-white">67%</span>
              </div>
              <Progress value={67} className="h-2" />
            </div>
            <div className="flex items-center gap-2 mt-4">
              <div className="w-3 h-3 bg-green-400 rounded-full animate-pulse"></div>
              <span className="text-slate-300">Все сервисы работают нормально</span>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Recent Activity */}
      <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
        <CardHeader>
          <CardTitle className="text-white flex items-center gap-2 text-xl">
            <BarChart3 className="h-6 w-6 text-blue-400" />
            Последняя активность
          </CardTitle>
          <CardDescription className="text-slate-400">Обзор последних операций в системе</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {[
              { action: "Добавлено 15 новых аккаунтов", time: "2 минуты назад", status: "success" },
              { action: "Опубликовано 47 постов в Reels", time: "5 минут назад", status: "success" },
              { action: "Обновлены настройки прокси", time: "12 минут назад", status: "info" },
              { action: "Завершен прогрев 8 аккаунтов", time: "25 минут назад", status: "success" },
              { action: "ИИ обработал 156 комментариев", time: "1 час назад", status: "success" },
            ].map((item, index) => (
              <div key={index} className="flex items-center justify-between p-4 rounded-lg bg-slate-700/30">
                <div className="flex items-center gap-3">
                  <Badge
                    variant={item.status === "success" ? "default" : "secondary"}
                    className={`${item.status === "success" ? "bg-green-600" : "bg-blue-600"}`}
                  >
                    {item.status === "success" ? "Успешно" : "Инфо"}
                  </Badge>
                  <span className="text-white text-lg">{item.action}</span>
                </div>
                <span className="text-slate-400">{item.time}</span>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </PageContainer>
  )
}
