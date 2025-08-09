"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { TrendingUp, Settings, BarChart3, Play, Users, Clock } from "lucide-react"
import PageContainer from "@/components/page-container"
import { Card, CardContent } from "@/components/ui/card"
import WarmupSettings from "@/components/warmup-settings"
import WarmupProgress from "@/components/warmup-progress"
import WarmupAnalytics from "@/components/warmup-analytics"
import AccountSelector from "@/components/account-selector"

export default function WarmupPage() {
  const [selectedAccounts, setSelectedAccounts] = useState<string[]>([])

  const actions = (
    <div className="flex gap-2">
      <Button className="bg-green-600 hover:bg-green-700">
        <Play className="h-5 w-5 mr-2" />
        Запустить прогрев
      </Button>
      <Button variant="outline" className="border-slate-600 text-slate-300">
        <TrendingUp className="h-5 w-5 mr-2" />
        Быстрая настройка
      </Button>
    </div>
  )

  return (
    <PageContainer
      title="Прогрев аккаунтов"
      description="Безопасный прогрев новых Instagram аккаунтов для предотвращения блокировок"
      actions={actions}
    >
      <Tabs defaultValue="settings" className="w-full space-y-6">
        <TabsList className="bg-slate-800 border-slate-700">
          <TabsTrigger value="settings" className="data-[state=active]:bg-blue-600">
            <Settings className="h-5 w-5 mr-2" />
            Настройки
          </TabsTrigger>
          <TabsTrigger value="progress" className="data-[state=active]:bg-green-600">
            <Clock className="h-5 w-5 mr-2" />
            Прогресс
          </TabsTrigger>
          <TabsTrigger value="analytics" className="data-[state=active]:bg-purple-600">
            <BarChart3 className="h-5 w-5 mr-2" />
            Аналитика
          </TabsTrigger>
          <TabsTrigger value="accounts" className="data-[state=active]:bg-orange-600">
            <Users className="h-5 w-5 mr-2" />
            Выбор аккаунтов
          </TabsTrigger>
        </TabsList>

        {/* Quick Stats */}
        <div className="grid grid-cols-4 gap-4">
          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-12 h-12 bg-blue-500/20 rounded-full flex items-center justify-center">
                  <Play className="h-6 w-6 text-blue-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">12</p>
                  <p className="text-slate-400">В прогреве</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-12 h-12 bg-green-500/20 rounded-full flex items-center justify-center">
                  <TrendingUp className="h-6 w-6 text-green-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">94.2%</p>
                  <p className="text-slate-400">Успешность</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-12 h-12 bg-purple-500/20 rounded-full flex items-center justify-center">
                  <Clock className="h-6 w-6 text-purple-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">12.5</p>
                  <p className="text-slate-400">Дней в среднем</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-12 h-12 bg-orange-500/20 rounded-full flex items-center justify-center">
                  <Users className="h-6 w-6 text-orange-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">147</p>
                  <p className="text-slate-400">Прогрето всего</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        <TabsContent value="settings">
          <WarmupSettings />
        </TabsContent>

        <TabsContent value="progress">
          <WarmupProgress />
        </TabsContent>

        <TabsContent value="analytics">
          <WarmupAnalytics />
        </TabsContent>

        <TabsContent value="accounts">
          <div className="grid lg:grid-cols-3 gap-6">
            <div className="lg:col-span-2">
              <AccountSelector selectedAccounts={selectedAccounts} onSelectionChange={setSelectedAccounts} />
            </div>
            <div className="space-y-6">
              <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
                <CardContent className="p-6">
                  <h3 className="text-xl font-bold text-white mb-4">Рекомендации для прогрева</h3>
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <h4 className="text-lg text-white font-medium">Подходящие аккаунты:</h4>
                      <ul className="text-slate-300 space-y-2 pl-4">
                        <li className="flex items-center gap-2">
                          <div className="w-2 h-2 rounded-full bg-green-400"></div>
                          Новые аккаунты (до 7 дней)
                        </li>
                        <li className="flex items-center gap-2">
                          <div className="w-2 h-2 rounded-full bg-green-400"></div>
                          Аккаунты без активности
                        </li>
                        <li className="flex items-center gap-2">
                          <div className="w-2 h-2 rounded-full bg-green-400"></div>
                          Восстановленные аккаунты
                        </li>
                        <li className="flex items-center gap-2">
                          <div className="w-2 h-2 rounded-full bg-green-400"></div>
                          Купленные аккаунты
                        </li>
                      </ul>
                    </div>
                    <div className="space-y-2">
                      <h4 className="text-lg text-white font-medium">Не рекомендуется:</h4>
                      <ul className="text-slate-300 space-y-2 pl-4">
                        <li className="flex items-center gap-2">
                          <div className="w-2 h-2 rounded-full bg-red-400"></div>
                          Аккаунты с историей блокировок
                        </li>
                        <li className="flex items-center gap-2">
                          <div className="w-2 h-2 rounded-full bg-red-400"></div>
                          Аккаунты с высокой активностью
                        </li>
                      </ul>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
                <CardContent className="p-6">
                  <h3 className="text-xl font-bold text-white mb-4">Выбрано аккаунтов: {selectedAccounts.length}</h3>
                  <Button className="w-full bg-green-600 hover:bg-green-700 text-lg py-6">
                    <Play className="h-5 w-5 mr-2" />
                    Запустить прогрев
                  </Button>
                </CardContent>
              </Card>
            </div>
          </div>
        </TabsContent>
      </Tabs>
    </PageContainer>
  )
}
