"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Globe,
  Plus,
  Search,
  Filter,
  RefreshCw,
  Activity,
  Trash2,
  Edit,
  CheckCircle,
  XCircle,
  AlertCircle,
  Users,
} from "lucide-react"
import SidebarNavigation from "@/components/sidebar-navigation"
import ProxyForm from "@/components/proxy-form"
import ProxyRotationSettings from "@/components/proxy-rotation-settings"
import ProxyMonitoring from "@/components/proxy-monitoring"
import { AddProxyDialog } from "@/components/add-proxy-dialog"
import { COMPACT_STYLES } from "@/components/global-optimization"

export default function ProxyPage() {
  const [searchTerm, setSearchTerm] = useState("")
  const [statusFilter, setStatusFilter] = useState<string>("all")
  const [countryFilter, setCountryFilter] = useState<string>("all")
  const [addProxyOpen, setAddProxyOpen] = useState(false)

  const proxies = [
    {
      id: "1",
      ip: "192.168.1.1",
      port: "8080",
      country: "US",
      city: "New York",
      type: "HTTP",
      username: "user1",
      status: "active",
      ping: 120,
      uptime: 99.8,
      lastChecked: "10 мин назад",
      connectedAccounts: 5,
    },
    {
      id: "2",
      ip: "192.168.1.2",
      port: "3128",
      country: "DE",
      city: "Berlin",
      type: "SOCKS5",
      username: "user2",
      status: "active",
      ping: 85,
      uptime: 99.9,
      lastChecked: "5 мин назад",
      connectedAccounts: 3,
    },
    {
      id: "3",
      ip: "192.168.1.3",
      port: "80",
      country: "UK",
      city: "London",
      type: "HTTP",
      username: "user3",
      status: "warning",
      ping: 210,
      uptime: 98.2,
      lastChecked: "15 мин назад",
      connectedAccounts: 2,
    },
    {
      id: "4",
      ip: "192.168.1.4",
      port: "1080",
      country: "FR",
      city: "Paris",
      type: "SOCKS4",
      username: "user4",
      status: "error",
      ping: 350,
      uptime: 95.1,
      lastChecked: "3 мин назад",
      connectedAccounts: 0,
    },
  ]

  const filteredProxies = proxies.filter((proxy) => {
    const matchesSearch =
      proxy.ip.includes(searchTerm) ||
      proxy.country.toLowerCase().includes(searchTerm.toLowerCase()) ||
      proxy.city.toLowerCase().includes(searchTerm.toLowerCase()) ||
      proxy.type.toLowerCase().includes(searchTerm.toLowerCase())

    const matchesStatus = statusFilter === "all" || proxy.status === statusFilter
    const matchesCountry = countryFilter === "all" || proxy.country === countryFilter

    return matchesSearch && matchesStatus && matchesCountry
  })

  const getStatusColor = (status: string) => {
    switch (status) {
      case "active":
        return "bg-green-500/20 text-green-400 border-green-500/30"
      case "warning":
        return "bg-yellow-500/20 text-yellow-400 border-yellow-500/30"
      case "error":
        return "bg-red-500/20 text-red-400 border-red-500/30"
      default:
        return "bg-slate-500/20 text-slate-400 border-slate-500/30"
    }
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "active":
        return <CheckCircle className={COMPACT_STYLES.iconSize} />
      case "warning":
        return <AlertCircle className={COMPACT_STYLES.iconSize} />
      case "error":
        return <XCircle className={COMPACT_STYLES.iconSize} />
      default:
        return <AlertCircle className={COMPACT_STYLES.iconSize} />
    }
  }

  const getPingColor = (ping: number) => {
    if (ping < 100) return "text-green-400"
    if (ping < 200) return "text-yellow-400"
    return "text-red-400"
  }

  const getUptimeColor = (uptime: number) => {
    if (uptime >= 99.5) return "text-green-400"
    if (uptime >= 98) return "text-yellow-400"
    return "text-red-400"
  }

  const countries = [...new Set(proxies.map((proxy) => proxy.country))]

  return (
    <SidebarNavigation>
      <div className={COMPACT_STYLES.pageHeaderMargin}>
        <h1
          className={`${COMPACT_STYLES.pageHeaderSize} font-bold text-white mb-0.5 bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent`}
        >
          Управление прокси
        </h1>
        <p className={`text-slate-300 ${COMPACT_STYLES.pageDescriptionSize}`}>
          Настройка, мониторинг и ротация прокси-серверов для безопасной работы аккаунтов
        </p>
      </div>

      <Tabs defaultValue="list" className={`space-y-2`}>
        <div className={`flex flex-col sm:flex-row ${COMPACT_STYLES.gap} justify-between`}>
          <TabsList className="bg-slate-800 border-slate-700 h-6">
            <TabsTrigger value="list" className="data-[state=active]:bg-blue-600 text-[10px] h-4 px-1.5">
              <Globe className="h-2.5 w-2.5 mr-0.5" />
              Список прокси
            </TabsTrigger>
            <TabsTrigger value="add" className="data-[state=active]:bg-green-600 text-[10px] h-4 px-1.5">
              <Plus className="h-2.5 w-2.5 mr-0.5" />
              Добавить прокси
            </TabsTrigger>
            <TabsTrigger value="rotation" className="data-[state=active]:bg-purple-600 text-[10px] h-4 px-1.5">
              <RefreshCw className="h-2.5 w-2.5 mr-0.5" />
              Ротация
            </TabsTrigger>
            <TabsTrigger value="monitoring" className="data-[state=active]:bg-orange-600 text-[10px] h-4 px-1.5">
              <Activity className="h-2.5 w-2.5 mr-0.5" />
              Мониторинг
            </TabsTrigger>
          </TabsList>

          <div className={`flex ${COMPACT_STYLES.smallGap}`}>
            <Button className="bg-blue-600 hover:bg-blue-700 h-6 text-[10px]" onClick={() => setAddProxyOpen(true)}>
              <Plus className="h-2.5 w-2.5 mr-1" />
              Добавить прокси
            </Button>
            <Button variant="outline" className="border-slate-600 text-slate-300 h-6 text-[10px]">
              <RefreshCw className="h-2.5 w-2.5 mr-1" />
              Проверить все
            </Button>
          </div>
        </div>

        <TabsContent value="list" className={`space-y-2`}>
          {/* Filters */}
          <div className={`flex flex-col sm:flex-row ${COMPACT_STYLES.gap}`}>
            <div className="relative flex-1">
              <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 text-slate-400 h-3 w-3" />
              <Input
                placeholder="Поиск по IP, стране, городу..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-7 bg-slate-800 border-slate-700 text-white h-6 text-xs"
              />
            </div>

            <div className={`flex ${COMPACT_STYLES.smallGap}`}>
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white rounded-md px-2 py-1 text-xs h-6"
              >
                <option value="all">Все статусы</option>
                <option value="active">Активные</option>
                <option value="warning">Предупреждения</option>
                <option value="error">Ошибки</option>
              </select>

              <select
                value={countryFilter}
                onChange={(e) => setCountryFilter(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white rounded-md px-2 py-1 text-xs h-6"
              >
                <option value="all">Все страны</option>
                {countries.map((country) => (
                  <option key={country} value={country}>
                    {country}
                  </option>
                ))}
              </select>

              <Button variant="outline" className="border-slate-600 text-slate-300 h-6 text-[10px]">
                <Filter className="h-2.5 w-2.5 mr-1" />
                Фильтры
              </Button>
            </div>
          </div>

          {/* Proxy Stats */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
            <Card className="bg-slate-800/50 border-slate-700">
              <CardContent className="p-1.5">
                <div className="flex items-center gap-1.5">
                  <div className="w-6 h-6 bg-green-500/20 rounded-full flex items-center justify-center">
                    <CheckCircle className="h-3 w-3 text-green-400" />
                  </div>
                  <div>
                    <p className="text-base font-bold text-white">18</p>
                    <p className="text-[10px] text-slate-400">Активные</p>
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
                    <p className="text-base font-bold text-white">3</p>
                    <p className="text-[10px] text-slate-400">Предупреждения</p>
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
                    <p className="text-base font-bold text-white">2</p>
                    <p className="text-[10px] text-slate-400">Ошибки</p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-slate-800/50 border-slate-700">
              <CardContent className="p-1.5">
                <div className="flex items-center gap-1.5">
                  <div className="w-6 h-6 bg-blue-500/20 rounded-full flex items-center justify-center">
                    <Users className="h-3 w-3 text-blue-400" />
                  </div>
                  <div>
                    <p className="text-base font-bold text-white">28</p>
                    <p className="text-[10px] text-slate-400">Подключено аккаунтов</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Proxy List */}
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
            <CardHeader className={COMPACT_STYLES.cardHeaderPadding}>
              <CardTitle className="text-white text-sm">Список прокси-серверов</CardTitle>
              <CardDescription className="text-slate-400 text-[10px]">
                Всего прокси: {filteredProxies.length}
              </CardDescription>
            </CardHeader>
            <CardContent className={COMPACT_STYLES.cardContentPadding}>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-slate-700">
                      <th
                        className={`text-left ${COMPACT_STYLES.tableCellPadding} text-slate-400 font-medium text-[10px]`}
                      >
                        IP:Порт
                      </th>
                      <th
                        className={`text-left ${COMPACT_STYLES.tableCellPadding} text-slate-400 font-medium text-[10px]`}
                      >
                        Тип
                      </th>
                      <th
                        className={`text-left ${COMPACT_STYLES.tableCellPadding} text-slate-400 font-medium text-[10px]`}
                      >
                        Локация
                      </th>
                      <th
                        className={`text-left ${COMPACT_STYLES.tableCellPadding} text-slate-400 font-medium text-[10px]`}
                      >
                        Статус
                      </th>
                      <th
                        className={`text-left ${COMPACT_STYLES.tableCellPadding} text-slate-400 font-medium text-[10px]`}
                      >
                        Пинг
                      </th>
                      <th
                        className={`text-left ${COMPACT_STYLES.tableCellPadding} text-slate-400 font-medium text-[10px]`}
                      >
                        Аптайм
                      </th>
                      <th
                        className={`text-left ${COMPACT_STYLES.tableCellPadding} text-slate-400 font-medium text-[10px]`}
                      >
                        Аккаунты
                      </th>
                      <th
                        className={`text-left ${COMPACT_STYLES.tableCellPadding} text-slate-400 font-medium text-[10px]`}
                      >
                        Действия
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredProxies.map((proxy) => (
                      <tr key={proxy.id} className="border-b border-slate-700 hover:bg-slate-700/30">
                        <td className={COMPACT_STYLES.tableCellPadding}>
                          <div className="text-white font-medium text-xs">
                            {proxy.ip}:{proxy.port}
                          </div>
                          <div className="text-slate-400 text-[9px]">
                            {proxy.username ? `${proxy.username}:****` : "Без авторизации"}
                          </div>
                        </td>
                        <td className={COMPACT_STYLES.tableCellPadding}>
                          <Badge
                            variant="outline"
                            className={`border-slate-600 text-slate-300 ${COMPACT_STYLES.badgePadding} text-[8px]`}
                          >
                            {proxy.type}
                          </Badge>
                        </td>
                        <td className={COMPACT_STYLES.tableCellPadding}>
                          <div className="flex items-center gap-1">
                            <span className="text-white text-xs">{proxy.country}</span>
                            <span className="text-slate-400 text-[9px]">{proxy.city}</span>
                          </div>
                        </td>
                        <td className={COMPACT_STYLES.tableCellPadding}>
                          <Badge
                            className={`${getStatusColor(proxy.status)} ${COMPACT_STYLES.badgePadding} text-[8px]`}
                          >
                            {getStatusIcon(proxy.status)}
                            <span className="ml-0.5 capitalize">
                              {proxy.status === "active"
                                ? "Активен"
                                : proxy.status === "warning"
                                  ? "Предупреждение"
                                  : "Ошибка"}
                            </span>
                          </Badge>
                        </td>
                        <td className={COMPACT_STYLES.tableCellPadding}>
                          <span className={`${getPingColor(proxy.ping)} text-xs`}>{proxy.ping} мс</span>
                        </td>
                        <td className={COMPACT_STYLES.tableCellPadding}>
                          <span className={`${getUptimeColor(proxy.uptime)} text-xs`}>{proxy.uptime}%</span>
                          <div className="text-slate-400 text-[9px]">Проверен {proxy.lastChecked}</div>
                        </td>
                        <td className={COMPACT_STYLES.tableCellPadding}>
                          <div className="flex items-center gap-1">
                            <Users className="h-2.5 w-2.5 text-blue-400" />
                            <span className="text-white text-xs">{proxy.connectedAccounts}</span>
                          </div>
                        </td>
                        <td className={COMPACT_STYLES.tableCellPadding}>
                          <div className="flex items-center gap-1">
                            <Button variant="ghost" size="sm" className="h-5 w-5 p-0">
                              <RefreshCw className="h-2.5 w-2.5 text-slate-400" />
                            </Button>
                            <Button variant="ghost" size="sm" className="h-5 w-5 p-0">
                              <Edit className="h-2.5 w-2.5 text-slate-400" />
                            </Button>
                            <Button variant="ghost" size="sm" className="h-5 w-5 p-0">
                              <Trash2 className="h-2.5 w-2.5 text-slate-400" />
                            </Button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="add">
          <ProxyForm />
        </TabsContent>

        <TabsContent value="rotation">
          <ProxyRotationSettings />
        </TabsContent>

        <TabsContent value="monitoring">
          <ProxyMonitoring />
        </TabsContent>
      </Tabs>

      {/* Модальное окно для добавления прокси */}
      <AddProxyDialog open={addProxyOpen} onOpenChange={setAddProxyOpen} />
    </SidebarNavigation>
  )
}
