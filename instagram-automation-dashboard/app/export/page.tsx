"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Download, FileJson, FileSpreadsheet, FileText, Filter, Settings } from "lucide-react"
import PageContainer from "@/components/page-container"

export default function ExportPage() {
  const [exportFormat, setExportFormat] = useState("csv")
  const [dateRange, setDateRange] = useState("last7days")

  const actions = (
    <Button className="bg-blue-600 hover:bg-blue-700">
      <Download className="h-5 w-5 mr-2" />
      Экспортировать данные
    </Button>
  )

  return (
    <PageContainer
      title="Экспорт данных"
      description="Экспортируйте данные из системы в различных форматах"
      actions={actions}
      centerContent={true}
    >
      <Tabs defaultValue="accounts" className="w-full space-y-6">
        <TabsList className="bg-slate-800 border-slate-700">
          <TabsTrigger value="accounts" className="data-[state=active]:bg-blue-600">
            <FileSpreadsheet className="h-5 w-5 mr-2" />
            Аккаунты
          </TabsTrigger>
          <TabsTrigger value="analytics" className="data-[state=active]:bg-green-600">
            <FileJson className="h-5 w-5 mr-2" />
            Аналитика
          </TabsTrigger>
          <TabsTrigger value="logs" className="data-[state=active]:bg-purple-600">
            <FileText className="h-5 w-5 mr-2" />
            Логи
          </TabsTrigger>
          <TabsTrigger value="settings" className="data-[state=active]:bg-orange-600">
            <Settings className="h-5 w-5 mr-2" />
            Настройки
          </TabsTrigger>
        </TabsList>

        <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
          <CardHeader>
            <CardTitle className="text-white text-xl">Параметры экспорта</CardTitle>
            <CardDescription className="text-slate-400">Настройте параметры для экспорта данных</CardDescription>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="grid grid-cols-2 gap-6">
              <div className="space-y-2">
                <label className="text-lg text-white">Формат файла</label>
                <select
                  value={exportFormat}
                  onChange={(e) => setExportFormat(e.target.value)}
                  className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white"
                >
                  <option value="csv">CSV</option>
                  <option value="json">JSON</option>
                  <option value="xlsx">Excel (XLSX)</option>
                  <option value="pdf">PDF</option>
                </select>
              </div>
              <div className="space-y-2">
                <label className="text-lg text-white">Период данных</label>
                <select
                  value={dateRange}
                  onChange={(e) => setDateRange(e.target.value)}
                  className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white"
                >
                  <option value="today">Сегодня</option>
                  <option value="yesterday">Вчера</option>
                  <option value="last7days">Последние 7 дней</option>
                  <option value="last30days">Последние 30 дней</option>
                  <option value="custom">Произвольный период</option>
                </select>
              </div>
            </div>

            {dateRange === "custom" && (
              <div className="grid grid-cols-2 gap-6">
                <div className="space-y-2">
                  <label className="text-lg text-white">Начальная дата</label>
                  <input
                    type="date"
                    className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white"
                  />
                </div>
                <div className="space-y-2">
                  <label className="text-lg text-white">Конечная дата</label>
                  <input
                    type="date"
                    className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white"
                  />
                </div>
              </div>
            )}

            <TabsContent value="accounts" className="mt-6 space-y-6">
              <div className="space-y-2">
                <label className="text-lg text-white">Данные для экспорта</label>
                <div className="grid grid-cols-2 gap-4">
                  {[
                    "Основная информация",
                    "Статистика",
                    "История активности",
                    "Прокси",
                    "Публикации",
                    "Настройки прогрева",
                  ].map((item) => (
                    <div key={item} className="flex items-center gap-2">
                      <input type="checkbox" defaultChecked className="h-5 w-5" />
                      <label className="text-slate-300">{item}</label>
                    </div>
                  ))}
                </div>
              </div>

              <div className="space-y-2">
                <label className="text-lg text-white">Фильтр аккаунтов</label>
                <div className="flex gap-4">
                  <select className="flex-1 bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white">
                    <option value="all">Все аккаунты</option>
                    <option value="active">Только активные</option>
                    <option value="warming">В прогреве</option>
                    <option value="error">С ошибками</option>
                  </select>
                  <Button variant="outline" className="border-slate-600 text-slate-300">
                    <Filter className="h-5 w-5 mr-2" />
                    Дополнительные фильтры
                  </Button>
                </div>
              </div>
            </TabsContent>

            <TabsContent value="analytics" className="mt-6 space-y-6">
              <div className="space-y-2">
                <label className="text-lg text-white">Тип аналитики</label>
                <div className="grid grid-cols-2 gap-4">
                  {[
                    "Общая статистика",
                    "Производительность",
                    "Активность аккаунтов",
                    "Эффективность прокси",
                    "Успешность публикаций",
                    "Метрики прогрева",
                  ].map((item) => (
                    <div key={item} className="flex items-center gap-2">
                      <input type="checkbox" defaultChecked className="h-5 w-5" />
                      <label className="text-slate-300">{item}</label>
                    </div>
                  ))}
                </div>
              </div>

              <div className="space-y-2">
                <label className="text-lg text-white">Группировка данных</label>
                <select className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white">
                  <option value="daily">По дням</option>
                  <option value="weekly">По неделям</option>
                  <option value="monthly">По месяцам</option>
                  <option value="account">По аккаунтам</option>
                </select>
              </div>
            </TabsContent>

            <TabsContent value="logs" className="mt-6 space-y-6">
              <div className="space-y-2">
                <label className="text-lg text-white">Уровень логов</label>
                <div className="grid grid-cols-2 gap-4">
                  {["Информация", "Предупреждения", "Ошибки", "Критические", "Отладка", "Системные"].map((item) => (
                    <div key={item} className="flex items-center gap-2">
                      <input type="checkbox" defaultChecked className="h-5 w-5" />
                      <label className="text-slate-300">{item}</label>
                    </div>
                  ))}
                </div>
              </div>

              <div className="space-y-2">
                <label className="text-lg text-white">Источник логов</label>
                <select className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white">
                  <option value="all">Все источники</option>
                  <option value="accounts">Аккаунты</option>
                  <option value="proxy">Прокси</option>
                  <option value="posts">Публикации</option>
                  <option value="warmup">Прогрев</option>
                  <option value="system">Система</option>
                </select>
              </div>
            </TabsContent>

            <TabsContent value="settings" className="mt-6 space-y-6">
              <div className="space-y-2">
                <label className="text-lg text-white">Формат даты и времени</label>
                <select className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white">
                  <option value="iso">ISO (2023-04-15T14:30:00)</option>
                  <option value="eu">Европейский (15.04.2023 14:30)</option>
                  <option value="us">Американский (04/15/2023 2:30 PM)</option>
                  <option value="unix">Unix timestamp</option>
                </select>
              </div>

              <div className="space-y-2">
                <label className="text-lg text-white">Дополнительные настройки</label>
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <label className="text-slate-300">Включить заголовки</label>
                    <input type="checkbox" defaultChecked className="h-5 w-5" />
                  </div>
                  <div className="flex items-center justify-between">
                    <label className="text-slate-300">Сжать файл (ZIP)</label>
                    <input type="checkbox" className="h-5 w-5" />
                  </div>
                  <div className="flex items-center justify-between">
                    <label className="text-slate-300">Отправить на email</label>
                    <input type="checkbox" className="h-5 w-5" />
                  </div>
                </div>
              </div>
            </TabsContent>

            <div className="flex justify-end gap-4 pt-4">
              <Button variant="outline" className="border-slate-600 text-slate-300 text-lg py-6 px-8">
                Отмена
              </Button>
              <Button className="bg-blue-600 hover:bg-blue-700 text-lg py-6 px-8">
                <Download className="h-5 w-5 mr-2" />
                Экспортировать
              </Button>
            </div>
          </CardContent>
        </Card>
      </Tabs>
    </PageContainer>
  )
}
