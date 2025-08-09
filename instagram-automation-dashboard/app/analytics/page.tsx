"use client"
import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { BarChart3, Users, Target, TrendingUp, Download, RefreshCw } from "lucide-react"
import SidebarNavigation from "@/components/sidebar-navigation"
import AnalyticsOverview from "@/components/analytics-overview"
import AccountAnalytics from "@/components/account-analytics"
import PerformanceAnalytics from "@/components/performance-analytics"

export default function AnalyticsPage() {
  return (
    <SidebarNavigation>
      <div className="mb-8">
        <h1 className="text-4xl font-bold text-white mb-2 bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
          Аналитика и отчеты
        </h1>
        <p className="text-slate-300 text-lg">Комплексная аналитика эффективности Instagram автоматизации</p>
      </div>

      <Tabs defaultValue="overview" className="space-y-6">
        <div className="flex flex-col sm:flex-row gap-4 justify-between">
          <TabsList className="bg-slate-800 border-slate-700">
            <TabsTrigger value="overview" className="data-[state=active]:bg-blue-600">
              <BarChart3 className="h-4 w-4 mr-2" />
              Обзор
            </TabsTrigger>
            <TabsTrigger value="accounts" className="data-[state=active]:bg-green-600">
              <Users className="h-4 w-4 mr-2" />
              По аккаунтам
            </TabsTrigger>
            <TabsTrigger value="performance" className="data-[state=active]:bg-purple-600">
              <Target className="h-4 w-4 mr-2" />
              Производительность
            </TabsTrigger>
          </TabsList>

          <div className="flex gap-2">
            <Button variant="outline" className="border-slate-600 text-slate-300">
              <RefreshCw className="h-4 w-4 mr-2" />
              Обновить данные
            </Button>
            <Button className="bg-blue-600 hover:bg-blue-700">
              <Download className="h-4 w-4 mr-2" />
              Скачать отчет
            </Button>
          </div>
        </div>

        {/* Quick Stats */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-2">
                <div className="w-10 h-10 bg-blue-500/20 rounded-full flex items-center justify-center">
                  <Users className="h-5 w-5 text-blue-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">47</p>
                  <p className="text-sm text-slate-400">Активных аккаунтов</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-2">
                <div className="w-10 h-10 bg-green-500/20 rounded-full flex items-center justify-center">
                  <TrendingUp className="h-5 w-5 text-green-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">2.4M</p>
                  <p className="text-sm text-slate-400">Общий охват</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-2">
                <div className="w-10 h-10 bg-purple-500/20 rounded-full flex items-center justify-center">
                  <Target className="h-5 w-5 text-purple-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">5.2%</p>
                  <p className="text-sm text-slate-400">Средняя вовлеченность</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardContent className="p-4">
              <div className="flex items-center gap-2">
                <div className="w-10 h-10 bg-orange-500/20 rounded-full flex items-center justify-center">
                  <BarChart3 className="h-5 w-5 text-orange-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">94.2%</p>
                  <p className="text-sm text-slate-400">Успешность автоматизации</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        <TabsContent value="overview">
          <AnalyticsOverview />
        </TabsContent>

        <TabsContent value="accounts">
          <AccountAnalytics />
        </TabsContent>

        <TabsContent value="performance">
          <PerformanceAnalytics />
        </TabsContent>
      </Tabs>
    </SidebarNavigation>
  )
}
