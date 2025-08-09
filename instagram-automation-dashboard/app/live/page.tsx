"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Activity, RefreshCw, Users, Clock, AlertTriangle, CheckCircle, XCircle, AlertCircle } from "lucide-react"
import SidebarNavigation from "@/components/sidebar-navigation"
import LiveActivityFeed from "@/components/live-activity-feed"
import AccountStatusMonitor from "@/components/account-status-monitor"
import RealTimeMetrics from "@/components/real-time-metrics"
import { COMPACT_STYLES } from "@/components/global-optimization"

export default function LivePage() {
  const [refreshInterval, setRefreshInterval] = useState("30")

  return (
    <SidebarNavigation>
      <div className={COMPACT_STYLES.pageHeaderMargin}>
        <h1
          className={`${COMPACT_STYLES.pageHeaderSize} font-bold text-white mb-0.5 bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent`}
        >
          Live Мониторинг
        </h1>
        <p className={`text-slate-300 ${COMPACT_STYLES.pageDescriptionSize}`}>
          Отслеживание активности и статуса аккаунтов в реальном времени
        </p>
      </div>

      <Tabs defaultValue="activity" className={`space-y-2`}>
        <div className={`flex flex-col sm:flex-row ${COMPACT_STYLES.gap} justify-between`}>
          <TabsList className="bg-slate-800 border-slate-700 h-6">
            <TabsTrigger value="activity" className="data-[state=active]:bg-blue-600 text-[10px] h-4 px-1.5">
              <Activity className="h-2.5 w-2.5 mr-0.5" />
              Активность
            </TabsTrigger>
            <TabsTrigger value="accounts" className="data-[state=active]:bg-green-600 text-[10px] h-4 px-1.5">
              <Users className="h-2.5 w-2.5 mr-0.5" />
              Статус аккаунтов
            </TabsTrigger>
            <TabsTrigger value="metrics" className="data-[state=active]:bg-purple-600 text-[10px] h-4 px-1.5">
              <Clock className="h-2.5 w-2.5 mr-0.5" />
              Метрики
            </TabsTrigger>
          </TabsList>

          <div className={`flex ${COMPACT_STYLES.smallGap}`}>
            <select
              value={refreshInterval}
              onChange={(e) => setRefreshInterval(e.target.value)}
              className="bg-slate-800 border-slate-700 text-white rounded-md px-2 py-1 text-xs h-6"
            >
              <option value="10">Обновлять каждые 10с</option>
              <option value="30">Обновлять каждые 30с</option>
              <option value="60">Обновлять каждые 60с</option>
              <option value="300">Обновлять каждые 5м</option>
            </select>
            <Button className="bg-blue-600 hover:bg-blue-700 h-6 text-[10px]">
              <RefreshCw className="h-2.5 w-2.5 mr-1" />
              Обновить сейчас
            </Button>
          </div>
        </div>

        {/* Live Stats */}
        <div className="grid grid-cols-4 gap-2">
          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-1.5">
              <div className="flex items-center gap-1.5">
                <div className="w-6 h-6 bg-green-500/20 rounded-full flex items-center justify-center">
                  <CheckCircle className="h-3 w-3 text-green-400" />
                </div>
                <div>
                  <p className="text-base font-bold text-white leading-tight">42</p>
                  <p className="text-[10px] text-slate-400 leading-tight">Активные</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-1.5">
              <div className="flex items-center gap-1.5">
                <div className="w-6 h-6 bg-yellow-500/20 rounded-full flex items-center justify-center">
                  <AlertCircle className="h-3 w-3 text-yellow-400" />
                </div>
                <div>
                  <p className="text-base font-bold text-white leading-tight">3</p>
                  <p className="text-[10px] text-slate-400 leading-tight">Предупреждения</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-1.5">
              <div className="flex items-center gap-1.5">
                <div className="w-6 h-6 bg-red-500/20 rounded-full flex items-center justify-center">
                  <XCircle className="h-3 w-3 text-red-400" />
                </div>
                <div>
                  <p className="text-base font-bold text-white leading-tight">1</p>
                  <p className="text-[10px] text-slate-400 leading-tight">Ошибки</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-1.5">
              <div className="flex items-center gap-1.5">
                <div className="w-6 h-6 bg-blue-500/20 rounded-full flex items-center justify-center">
                  <Activity className="h-3 w-3 text-blue-400" />
                </div>
                <div>
                  <p className="text-base font-bold text-white leading-tight">247</p>
                  <p className="text-[10px] text-slate-400 leading-tight">Действий/час</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
          <CardHeader className={COMPACT_STYLES.cardHeaderPadding}>
            <div className="flex items-center justify-between">
              <CardTitle className="text-white text-sm">Системные уведомления</CardTitle>
              <Badge className="bg-red-500 text-white text-[8px] px-1 py-0">2 новых</Badge>
            </div>
          </CardHeader>
          <CardContent className={COMPACT_STYLES.cardContentPadding}>
            <div className="space-y-1">
              <div className="flex items-center gap-1.5 p-1.5 rounded-lg bg-red-500/10 border border-red-500/20">
                <AlertTriangle className="h-3 w-3 text-red-400" />
                <div>
                  <p className="text-white text-[10px]">
                    Аккаунт @fashion_style_2024 получил предупреждение от Instagram
                  </p>
                  <p className="text-slate-400 text-[9px]">2 минуты назад</p>
                </div>
              </div>
              <div className="flex items-center gap-1.5 p-1.5 rounded-lg bg-yellow-500/10 border border-yellow-500/20">
                <AlertCircle className="h-3 w-3 text-yellow-400" />
                <div>
                  <p className="text-white text-[10px]">Высокая нагрузка на прокси US-01, возможны задержки</p>
                  <p className="text-slate-400 text-[9px]">15 минут назад</p>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        <TabsContent value="activity" className="max-h-[calc(100vh-280px)] overflow-auto">
          <LiveActivityFeed refreshInterval={Number.parseInt(refreshInterval)} />
        </TabsContent>

        <TabsContent value="accounts" className="max-h-[calc(100vh-280px)] overflow-auto">
          <AccountStatusMonitor refreshInterval={Number.parseInt(refreshInterval)} />
        </TabsContent>

        <TabsContent value="metrics" className="max-h-[calc(100vh-280px)] overflow-auto">
          <RealTimeMetrics refreshInterval={Number.parseInt(refreshInterval)} />
        </TabsContent>
      </Tabs>
    </SidebarNavigation>
  )
}
