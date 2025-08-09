"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Bell, Settings, Shield, CheckCircle, XCircle, AlertCircle, Trash2, CheckCheck } from "lucide-react"
import PageContainer from "@/components/page-container"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"

export default function NotificationsPage() {
  const [filter, setFilter] = useState("all")

  const notifications = [
    {
      id: 1,
      type: "success",
      title: "Публикация выполнена",
      message: "Успешно опубликовано 12 постов в аккаунтах",
      time: "2 минуты назад",
      read: false,
    },
    {
      id: 2,
      type: "error",
      title: "Ошибка авторизации",
      message: "Не удалось войти в аккаунт @travel_blogger_pro",
      time: "15 минут назад",
      read: false,
    },
    {
      id: 3,
      type: "warning",
      title: "Предупреждение",
      message: "Высокая нагрузка на прокси US-01",
      time: "1 час назад",
      read: true,
    },
    {
      id: 4,
      type: "info",
      title: "Обновление системы",
      message: "Доступна новая версия системы 2.5.0",
      time: "3 часа назад",
      read: true,
    },
    {
      id: 5,
      type: "success",
      title: "Прогрев завершен",
      message: "Успешно завершен прогрев 5 аккаунтов",
      time: "5 часов назад",
      read: true,
    },
  ]

  const getIcon = (type: string) => {
    switch (type) {
      case "success":
        return <CheckCircle className="h-5 w-5 text-green-400" />
      case "error":
        return <XCircle className="h-5 w-5 text-red-400" />
      case "warning":
        return <AlertCircle className="h-5 w-5 text-yellow-400" />
      case "info":
        return <Bell className="h-5 w-5 text-blue-400" />
      default:
        return <Bell className="h-5 w-5 text-slate-400" />
    }
  }

  const getBadgeColor = (type: string) => {
    switch (type) {
      case "success":
        return "bg-green-500/20 text-green-400 border-green-500/30"
      case "error":
        return "bg-red-500/20 text-red-400 border-red-500/30"
      case "warning":
        return "bg-yellow-500/20 text-yellow-400 border-yellow-500/30"
      case "info":
        return "bg-blue-500/20 text-blue-400 border-blue-500/30"
      default:
        return "bg-slate-500/20 text-slate-400 border-slate-500/30"
    }
  }

  const filteredNotifications = filter === "all" ? notifications : notifications.filter((n) => !n.read)

  const actions = (
    <div className="flex gap-2">
      <Button variant="outline" className="border-slate-600 text-slate-300">
        <CheckCheck className="h-5 w-5 mr-2" />
        Прочитать все
      </Button>
      <Button variant="outline" className="border-slate-600 text-slate-300">
        <Trash2 className="h-5 w-5 mr-2" />
        Очистить
      </Button>
    </div>
  )

  return (
    <PageContainer
      title="Центр уведомлений"
      description="Управление уведомлениями и оповещениями системы"
      actions={actions}
    >
      {/* Notification Stats */}
      <div className="grid grid-cols-4 gap-4 mb-6">
        <Card className="bg-slate-800/50 border-slate-700">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-blue-500/20 rounded-full flex items-center justify-center">
                <Bell className="h-6 w-6 text-blue-400" />
              </div>
              <div>
                <p className="text-2xl font-bold text-white">12</p>
                <p className="text-slate-400">Новых</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-slate-700">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-green-500/20 rounded-full flex items-center justify-center">
                <CheckCircle className="h-6 w-6 text-green-400" />
              </div>
              <div>
                <p className="text-2xl font-bold text-white">47</p>
                <p className="text-slate-400">Успешных</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-slate-700">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-yellow-500/20 rounded-full flex items-center justify-center">
                <AlertCircle className="h-6 w-6 text-yellow-400" />
              </div>
              <div>
                <p className="text-2xl font-bold text-white">3</p>
                <p className="text-slate-400">Предупреждений</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-slate-700">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-red-500/20 rounded-full flex items-center justify-center">
                <XCircle className="h-6 w-6 text-red-400" />
              </div>
              <div>
                <p className="text-2xl font-bold text-white">1</p>
                <p className="text-slate-400">Ошибок</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Tabs defaultValue="notifications" className="w-full">
        <TabsList className="bg-slate-800 border-slate-700">
          <TabsTrigger value="notifications" className="data-[state=active]:bg-blue-600">
            <Bell className="h-5 w-5 mr-2" />
            Уведомления
          </TabsTrigger>
          <TabsTrigger value="settings" className="data-[state=active]:bg-green-600">
            <Settings className="h-5 w-5 mr-2" />
            Настройки
          </TabsTrigger>
          <TabsTrigger value="alerts" className="data-[state=active]:bg-purple-600">
            <Shield className="h-5 w-5 mr-2" />
            Оповещения
          </TabsTrigger>
        </TabsList>

        <TabsContent value="notifications" className="mt-6">
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle className="text-white text-xl">Все уведомления</CardTitle>
                <CardDescription className="text-slate-400">Последние уведомления и события системы</CardDescription>
              </div>
              <div className="flex gap-2">
                <Badge
                  className={`cursor-pointer ${filter === "all" ? "bg-blue-600" : "bg-slate-700"}`}
                  onClick={() => setFilter("all")}
                >
                  Все
                </Badge>
                <Badge
                  className={`cursor-pointer ${filter === "unread" ? "bg-blue-600" : "bg-slate-700"}`}
                  onClick={() => setFilter("unread")}
                >
                  Непрочитанные
                </Badge>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {filteredNotifications.length === 0 ? (
                  <div className="flex flex-col items-center justify-center py-12">
                    <Bell className="h-20 w-20 text-slate-500 mb-4" />
                    <p className="text-lg text-slate-400">Нет уведомлений</p>
                  </div>
                ) : (
                  filteredNotifications.map((notification) => (
                    <div
                      key={notification.id}
                      className={`p-4 rounded-lg border ${
                        notification.read ? "bg-slate-800/30 border-slate-700/50" : "bg-slate-800/50 border-slate-700"
                      }`}
                    >
                      <div className="flex items-start gap-4">
                        <div
                          className={`w-12 h-12 rounded-full flex items-center justify-center ${getBadgeColor(notification.type)}`}
                        >
                          {getIcon(notification.type)}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between gap-2">
                            <h4 className="text-lg text-white font-medium truncate">{notification.title}</h4>
                            {!notification.read && <Badge className="bg-blue-600">Новое</Badge>}
                          </div>
                          <p className="text-slate-300 mt-1">{notification.message}</p>
                          <div className="flex items-center justify-between mt-2">
                            <span className="text-slate-400">{notification.time}</span>
                            <Button variant="ghost" size="sm" className="h-8 w-8 p-0">
                              <XCircle className="h-5 w-5 text-slate-400" />
                            </Button>
                          </div>
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="settings" className="mt-6">
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
            <CardHeader>
              <CardTitle className="text-white text-xl">Настройки уведомлений</CardTitle>
              <CardDescription className="text-slate-400">Управление настройками уведомлений</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-6">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="text-lg text-white font-medium">Push-уведомления</h3>
                    <p className="text-slate-400">Получать уведомления в браузере</p>
                  </div>
                  <div className="flex items-center h-6">
                    <div className="w-14 h-7 rounded-full bg-blue-600 relative">
                      <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 right-0.5"></div>
                    </div>
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="text-lg text-white font-medium">Email-уведомления</h3>
                    <p className="text-slate-400">Получать уведомления на email</p>
                  </div>
                  <div className="flex items-center h-6">
                    <div className="w-14 h-7 rounded-full bg-slate-600 relative">
                      <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 left-0.5"></div>
                    </div>
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="text-lg text-white font-medium">Звуковые уведомления</h3>
                    <p className="text-slate-400">Проигрывать звук при уведомлении</p>
                  </div>
                  <div className="flex items-center h-6">
                    <div className="w-14 h-7 rounded-full bg-blue-600 relative">
                      <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 right-0.5"></div>
                    </div>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="alerts" className="mt-6">
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
            <CardHeader>
              <CardTitle className="text-white text-xl">Система оповещений</CardTitle>
              <CardDescription className="text-slate-400">Настройка системы оповещений</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-6">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="text-lg text-white font-medium">Критические ошибки</h3>
                    <p className="text-slate-400">Оповещения о критических ошибках</p>
                  </div>
                  <div className="flex items-center h-6">
                    <div className="w-14 h-7 rounded-full bg-blue-600 relative">
                      <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 right-0.5"></div>
                    </div>
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="text-lg text-white font-medium">Блокировки аккаунтов</h3>
                    <p className="text-slate-400">Оповещения о блокировках аккаунтов</p>
                  </div>
                  <div className="flex items-center h-6">
                    <div className="w-14 h-7 rounded-full bg-blue-600 relative">
                      <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 right-0.5"></div>
                    </div>
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="text-lg text-white font-medium">Проблемы с прокси</h3>
                    <p className="text-slate-400">Оповещения о проблемах с прокси</p>
                  </div>
                  <div className="flex items-center h-6">
                    <div className="w-14 h-7 rounded-full bg-blue-600 relative">
                      <div className="absolute w-6 h-6 bg-white rounded-full top-0.5 right-0.5"></div>
                    </div>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </PageContainer>
  )
}
