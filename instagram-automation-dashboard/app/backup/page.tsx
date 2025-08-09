"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Database, Clock, Upload, Download, Settings, History, CheckCircle } from "lucide-react"
import PageContainer from "@/components/page-container"
import { Progress } from "@/components/ui/progress"

export default function BackupPage() {
  const [backupProgress, setBackupProgress] = useState(0)
  const [isBackingUp, setIsBackingUp] = useState(false)

  const startBackup = () => {
    setIsBackingUp(true)
    setBackupProgress(0)

    const interval = setInterval(() => {
      setBackupProgress((prev) => {
        if (prev >= 100) {
          clearInterval(interval)
          setIsBackingUp(false)
          return 100
        }
        return prev + 10
      })
    }, 500)
  }

  const actions = (
    <Button className="bg-blue-600 hover:bg-blue-700" onClick={startBackup} disabled={isBackingUp}>
      <Database className="h-5 w-5 mr-2" />
      Создать резервную копию
    </Button>
  )

  return (
    <PageContainer
      title="Резервное копирование"
      description="Управление резервными копиями данных и настройками восстановления"
      actions={actions}
      centerContent={true}
    >
      <Tabs defaultValue="backups" className="w-full space-y-6">
        <TabsList className="bg-slate-800 border-slate-700">
          <TabsTrigger value="backups" className="data-[state=active]:bg-blue-600">
            <Database className="h-5 w-5 mr-2" />
            Резервные копии
          </TabsTrigger>
          <TabsTrigger value="schedule" className="data-[state=active]:bg-green-600">
            <Clock className="h-5 w-5 mr-2" />
            Расписание
          </TabsTrigger>
          <TabsTrigger value="restore" className="data-[state=active]:bg-purple-600">
            <Upload className="h-5 w-5 mr-2" />
            Восстановление
          </TabsTrigger>
          <TabsTrigger value="settings" className="data-[state=active]:bg-orange-600">
            <Settings className="h-5 w-5 mr-2" />
            Настройки
          </TabsTrigger>
        </TabsList>

        {isBackingUp && (
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm mb-6">
            <CardContent className="p-6">
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-xl font-bold text-white">Создание резервной копии...</h3>
                  <span className="text-white">{backupProgress}%</span>
                </div>
                <Progress value={backupProgress} className="h-2" />
                <div className="flex justify-between text-sm text-slate-400">
                  <span>Сбор данных</span>
                  <span>Сжатие</span>
                  <span>Шифрование</span>
                  <span>Сохранение</span>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        <TabsContent value="backups">
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
            <CardHeader>
              <CardTitle className="text-white text-xl">Доступные резервные копии</CardTitle>
              <CardDescription className="text-slate-400">
                Список всех созданных резервных копий системы
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {[
                  { date: "15.06.2023", time: "14:30", size: "1.2 GB", type: "Полная", auto: true },
                  { date: "10.06.2023", time: "08:15", size: "1.1 GB", type: "Полная", auto: true },
                  { date: "05.06.2023", time: "22:45", size: "0.8 GB", type: "Частичная", auto: false },
                  { date: "01.06.2023", time: "10:00", size: "1.2 GB", type: "Полная", auto: true },
                  { date: "25.05.2023", time: "16:20", size: "1.0 GB", type: "Полная", auto: true },
                ].map((backup, index) => (
                  <div
                    key={index}
                    className="p-4 rounded-lg bg-slate-700/30 border border-slate-700 flex items-center justify-between"
                  >
                    <div className="flex items-center gap-4">
                      <div className="w-12 h-12 bg-blue-500/20 rounded-full flex items-center justify-center">
                        <Database className="h-6 w-6 text-blue-400" />
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <h4 className="text-lg text-white font-medium">Резервная копия от {backup.date}</h4>
                          {backup.auto && (
                            <span className="text-xs bg-slate-600 text-slate-300 px-2 py-1 rounded">Авто</span>
                          )}
                        </div>
                        <div className="flex items-center gap-4 text-slate-400 text-sm mt-1">
                          <span className="flex items-center gap-1">
                            <Clock className="h-4 w-4" /> {backup.time}
                          </span>
                          <span className="flex items-center gap-1">
                            <Database className="h-4 w-4" /> {backup.size}
                          </span>
                          <span className="flex items-center gap-1">
                            <CheckCircle className="h-4 w-4" /> {backup.type}
                          </span>
                        </div>
                      </div>
                    </div>
                    <div className="flex gap-2">
                      <Button variant="outline" size="sm" className="border-slate-600 text-slate-300">
                        <Upload className="h-4 w-4 mr-1" />
                        Восстановить
                      </Button>
                      <Button variant="outline" size="sm" className="border-slate-600 text-slate-300">
                        <Download className="h-4 w-4 mr-1" />
                        Скачать
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="schedule">
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
            <CardHeader>
              <CardTitle className="text-white text-xl">Расписание резервного копирования</CardTitle>
              <CardDescription className="text-slate-400">
                Настройте автоматическое создание резервных копий
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="space-y-2">
                <label className="text-lg text-white">Частота резервного копирования</label>
                <select className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white">
                  <option value="daily">Ежедневно</option>
                  <option value="weekly">Еженедельно</option>
                  <option value="monthly">Ежемесячно</option>
                  <option value="custom">Произвольно</option>
                </select>
              </div>

              <div className="space-y-2">
                <label className="text-lg text-white">Время запуска</label>
                <input
                  type="time"
                  defaultValue="03:00"
                  className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white"
                />
                <p className="text-sm text-slate-400">
                  Рекомендуется выбирать время с минимальной нагрузкой на систему
                </p>
              </div>

              <div className="space-y-2">
                <label className="text-lg text-white">Тип резервной копии</label>
                <div className="grid grid-cols-2 gap-4">
                  <div className="p-4 rounded-lg bg-blue-600/20 border border-blue-500 cursor-pointer">
                    <h4 className="text-lg text-white font-medium">Полная</h4>
                    <p className="text-slate-300 text-sm">Все данные и настройки системы</p>
                  </div>
                  <div className="p-4 rounded-lg bg-slate-700/50 border border-slate-600 cursor-pointer">
                    <h4 className="text-lg text-white font-medium">Частичная</h4>
                    <p className="text-slate-300 text-sm">Только критически важные данные</p>
                  </div>
                </div>
              </div>

              <div className="space-y-2">
                <label className="text-lg text-white">Хранение резервных копий</label>
                <select className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white">
                  <option value="5">Хранить последние 5 копий</option>
                  <option value="10">Хранить последние 10 копий</option>
                  <option value="30">Хранить последние 30 копий</option>
                  <option value="all">Хранить все копии</option>
                </select>
              </div>

              <div className="flex justify-end gap-4 pt-4">
                <Button variant="outline" className="border-slate-600 text-slate-300 text-lg py-6 px-8">
                  Отмена
                </Button>
                <Button className="bg-blue-600 hover:bg-blue-700 text-lg py-6 px-8">Сохранить расписание</Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="restore">
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
            <CardHeader>
              <CardTitle className="text-white text-xl">Восстановление из резервной копии</CardTitle>
              <CardDescription className="text-slate-400">
                Загрузите резервную копию для восстановления системы
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="border-2 border-dashed border-slate-600 rounded-lg p-8 text-center">
                <Upload className="h-12 w-12 text-slate-400 mx-auto mb-4" />
                <h3 className="text-xl text-white font-medium mb-2">Перетащите файл резервной копии</h3>
                <p className="text-slate-400 mb-4">или</p>
                <Button className="bg-blue-600 hover:bg-blue-700 text-lg py-6 px-8">Выбрать файл</Button>
                <p className="text-sm text-slate-400 mt-4">Поддерживаемые форматы: .zip, .bak, .enc</p>
              </div>

              <div className="p-4 rounded-lg bg-yellow-500/10 border border-yellow-500/30">
                <div className="flex items-start gap-3">
                  <div className="mt-1">
                    <History className="h-5 w-5 text-yellow-400" />
                  </div>
                  <div>
                    <h4 className="text-lg text-white font-medium">Внимание: Процесс восстановления</h4>
                    <p className="text-slate-300 mt-1">
                      Восстановление из резервной копии перезапишет все текущие данные. Этот процесс нельзя отменить.
                      Рекомендуется создать новую резервную копию перед восстановлением.
                    </p>
                  </div>
                </div>
              </div>

              <div className="flex justify-end gap-4 pt-4">
                <Button variant="outline" className="border-slate-600 text-slate-300 text-lg py-6 px-8">
                  Отмена
                </Button>
                <Button className="bg-yellow-600 hover:bg-yellow-700 text-lg py-6 px-8">Начать восстановление</Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="settings">
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
            <CardHeader>
              <CardTitle className="text-white text-xl">Настройки резервного копирования</CardTitle>
              <CardDescription className="text-slate-400">
                Настройте параметры резервного копирования и восстановления
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="space-y-2">
                <label className="text-lg text-white">Место хранения</label>
                <select className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white">
                  <option value="local">Локальное хранилище</option>
                  <option value="cloud">Облачное хранилище</option>
                  <option value="both">Локальное + Облачное</option>
                </select>
              </div>

              <div className="space-y-2">
                <label className="text-lg text-white">Шифрование</label>
                <div className="flex items-center justify-between">
                  <p className="text-slate-300">Шифровать резервные копии</p>
                  <div className="flex items-center h-6">
                    <div className="w-14 h-7 rounded-full bg-blue-600 relative">
                      <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 right-0.5"></div>
                    </div>
                  </div>
                </div>
              </div>

              <div className="space-y-2">
                <label className="text-lg text-white">Пароль шифрования</label>
                <input
                  type="password"
                  placeholder="Введите надежный пароль"
                  className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white"
                />
                <p className="text-sm text-slate-400">
                  Важно: Если вы забудете этот пароль, восстановление данных будет невозможно
                </p>
              </div>

              <div className="space-y-2">
                <label className="text-lg text-white">Сжатие</label>
                <select className="w-full bg-slate-700 border border-slate-600 rounded-md px-4 py-3 text-white">
                  <option value="none">Без сжатия</option>
                  <option value="normal">Нормальное (рекомендуется)</option>
                  <option value="maximum">Максимальное</option>
                </select>
              </div>

              <div className="space-y-2">
                <label className="text-lg text-white">Уведомления</label>
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <p className="text-slate-300">Уведомлять об успешном резервном копировании</p>
                    <div className="flex items-center h-6">
                      <div className="w-14 h-7 rounded-full bg-blue-600 relative">
                        <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 right-0.5"></div>
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center justify-between">
                    <p className="text-slate-300">Уведомлять об ошибках резервного копирования</p>
                    <div className="flex items-center h-6">
                      <div className="w-14 h-7 rounded-full bg-blue-600 relative">
                        <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 right-0.5"></div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <div className="flex justify-end gap-4 pt-4">
                <Button variant="outline" className="border-slate-600 text-slate-300 text-lg py-6 px-8">
                  Отмена
                </Button>
                <Button className="bg-blue-600 hover:bg-blue-700 text-lg py-6 px-8">Сохранить настройки</Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </PageContainer>
  )
}
