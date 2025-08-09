"use client"

import { useState } from "react"
import SidebarNavigation from "@/components/sidebar-navigation"
import { Card, CardContent } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Button } from "@/components/ui/button"
import { Shield, Lock, Activity, Eye, FileText, AlertTriangle } from "lucide-react"
import SecuritySettings from "@/components/security-settings"
import RiskMonitoring from "@/components/risk-monitoring"
import BlockProtection from "@/components/block-protection"
import PrivacySettings from "@/components/privacy-settings"
import SecurityLog from "@/components/security-log"

export default function SecurityPage() {
  const [securityScore, setSecurityScore] = useState(78)

  return (
    <SidebarNavigation>
      <div className="mb-8">
        <h1 className="text-4xl font-bold text-white mb-2 bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
          Безопасность
        </h1>
        <p className="text-slate-300 text-lg">Настройки безопасности и защиты аккаунтов от блокировок</p>
      </div>

      {/* Security Score Card */}
      <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm mb-6">
        <CardContent className="p-6">
          <div className="flex flex-col md:flex-row items-center justify-between gap-6">
            <div className="flex items-center gap-4">
              <div className="relative">
                <svg className="w-24 h-24">
                  <circle
                    className="text-slate-700"
                    strokeWidth="8"
                    stroke="currentColor"
                    fill="transparent"
                    r="40"
                    cx="48"
                    cy="48"
                  />
                  <circle
                    className={`${
                      securityScore >= 80 ? "text-green-500" : securityScore >= 60 ? "text-yellow-500" : "text-red-500"
                    }`}
                    strokeWidth="8"
                    strokeDasharray={`${securityScore * 2.51} 251`}
                    strokeLinecap="round"
                    stroke="currentColor"
                    fill="transparent"
                    r="40"
                    cx="48"
                    cy="48"
                    transform="rotate(-90 48 48)"
                  />
                </svg>
                <div className="absolute inset-0 flex items-center justify-center">
                  <div className="text-center">
                    <span className="text-2xl font-bold text-white">{securityScore}</span>
                    <span className="text-sm text-slate-400 block">из 100</span>
                  </div>
                </div>
              </div>
              <div>
                <h3 className="text-xl font-bold text-white">Уровень безопасности</h3>
                <p className="text-slate-400">
                  {securityScore >= 80
                    ? "Высокий уровень защиты"
                    : securityScore >= 60
                      ? "Средний уровень защиты"
                      : "Низкий уровень защиты"}
                </p>
                <div className="flex items-center gap-2 mt-1">
                  <AlertTriangle
                    className={`h-4 w-4 ${
                      securityScore >= 80 ? "text-green-500" : securityScore >= 60 ? "text-yellow-500" : "text-red-500"
                    }`}
                  />
                  <span className="text-sm text-slate-300">
                    {securityScore >= 80
                      ? "Риск блокировки минимальный"
                      : securityScore >= 60
                        ? "Умеренный риск блокировки"
                        : "Высокий риск блокировки"}
                  </span>
                </div>
              </div>
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
              <div className="bg-slate-700/50 rounded-lg p-3 text-center">
                <div className="text-2xl font-bold text-white">12</div>
                <div className="text-xs text-slate-400">Активных аккаунтов</div>
              </div>
              <div className="bg-slate-700/50 rounded-lg p-3 text-center">
                <div className="text-2xl font-bold text-green-400">0</div>
                <div className="text-xs text-slate-400">Блокировок</div>
              </div>
              <div className="bg-slate-700/50 rounded-lg p-3 text-center">
                <div className="text-2xl font-bold text-yellow-400">2</div>
                <div className="text-xs text-slate-400">Предупреждения</div>
              </div>
            </div>

            <div>
              <Button className="bg-blue-600 hover:bg-blue-700">
                <Shield className="h-4 w-4 mr-2" />
                Повысить безопасность
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      <Tabs defaultValue="settings" className="space-y-6">
        <TabsList className="bg-slate-800 border-slate-700">
          <TabsTrigger value="settings" className="data-[state=active]:bg-blue-600">
            <Shield className="h-4 w-4 mr-2" />
            Настройки
          </TabsTrigger>
          <TabsTrigger value="protection" className="data-[state=active]:bg-green-600">
            <Lock className="h-4 w-4 mr-2" />
            Защита от блокировок
          </TabsTrigger>
          <TabsTrigger value="monitoring" className="data-[state=active]:bg-yellow-600">
            <Activity className="h-4 w-4 mr-2" />
            Мониторинг рисков
          </TabsTrigger>
          <TabsTrigger value="privacy" className="data-[state=active]:bg-purple-600">
            <Eye className="h-4 w-4 mr-2" />
            Приватность
          </TabsTrigger>
          <TabsTrigger value="log" className="data-[state=active]:bg-slate-600">
            <FileText className="h-4 w-4 mr-2" />
            Журнал
          </TabsTrigger>
        </TabsList>

        <TabsContent value="settings">
          <SecuritySettings onScoreChange={setSecurityScore} />
        </TabsContent>

        <TabsContent value="protection">
          <BlockProtection />
        </TabsContent>

        <TabsContent value="monitoring">
          <RiskMonitoring />
        </TabsContent>

        <TabsContent value="privacy">
          <PrivacySettings />
        </TabsContent>

        <TabsContent value="log">
          <SecurityLog />
        </TabsContent>
      </Tabs>
    </SidebarNavigation>
  )
}
