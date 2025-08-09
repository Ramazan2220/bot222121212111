"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Plus, Search, Filter, MoreHorizontal, CheckCircle, XCircle, AlertCircle, Users, Upload } from "lucide-react"
import SidebarNavigation from "@/components/sidebar-navigation"
import { AddAccountDialog } from "@/components/add-account-dialog"
import { AccountFiltersDialog } from "@/components/account-filters-dialog"
import { ImportAccountsDialog } from "@/components/import-accounts-dialog"
import { COMPACT_STYLES } from "@/components/global-optimization"

export default function AccountsPage() {
  const [accounts] = useState([
    { id: 1, username: "@fashion_style_2024", status: "active", followers: "12.5K", posts: 47, proxy: "US-01" },
    { id: 2, username: "@travel_blogger_pro", status: "warming", followers: "8.2K", posts: 23, proxy: "EU-03" },
    { id: 3, username: "@fitness_motivation", status: "active", followers: "15.8K", posts: 89, proxy: "US-02" },
    { id: 4, username: "@food_lover_daily", status: "error", followers: "5.1K", posts: 12, proxy: "AS-01" },
    { id: 5, username: "@tech_reviews_hub", status: "active", followers: "22.3K", posts: 156, proxy: "EU-01" },
  ])

  // Состояния для модальных окон
  const [addAccountOpen, setAddAccountOpen] = useState(false)
  const [filtersOpen, setFiltersOpen] = useState(false)
  const [importOpen, setImportOpen] = useState(false)

  const getStatusColor = (status: string) => {
    switch (status) {
      case "active":
        return "bg-green-500/20 text-green-400 border-green-500/30"
      case "warming":
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
      case "warming":
        return <AlertCircle className={COMPACT_STYLES.iconSize} />
      case "error":
        return <XCircle className={COMPACT_STYLES.iconSize} />
      default:
        return <AlertCircle className={COMPACT_STYLES.iconSize} />
    }
  }

  return (
    <SidebarNavigation>
      <div className={COMPACT_STYLES.pageHeaderMargin}>
        <h1
          className={`${COMPACT_STYLES.pageHeaderSize} font-bold text-white mb-0.5 bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent`}
        >
          Управление аккаунтами
        </h1>
        <p className={`text-slate-300 ${COMPACT_STYLES.pageDescriptionSize}`}>
          Добавляйте, настраивайте и контролируйте до 100 Instagram аккаунтов одновременно
        </p>
      </div>

      <Tabs defaultValue="all" className={`space-y-2`}>
        <div className={`flex flex-col sm:flex-row ${COMPACT_STYLES.gap} justify-between`}>
          <TabsList className="bg-slate-800 border-slate-700 h-6">
            <TabsTrigger value="all" className="data-[state=active]:bg-blue-600 text-[10px] h-4 px-1.5">
              Все аккаунты ({accounts.length})
            </TabsTrigger>
            <TabsTrigger value="active" className="data-[state=active]:bg-green-600 text-[10px] h-4 px-1.5">
              Активные (3)
            </TabsTrigger>
            <TabsTrigger value="warming" className="data-[state=active]:bg-yellow-600 text-[10px] h-4 px-1.5">
              Прогрев (1)
            </TabsTrigger>
            <TabsTrigger value="error" className="data-[state=active]:bg-red-600 text-[10px] h-4 px-1.5">
              Ошибки (1)
            </TabsTrigger>
          </TabsList>

          <div className={`flex ${COMPACT_STYLES.smallGap}`}>
            <Button className="bg-blue-600 hover:bg-blue-700 h-6 text-[10px]" onClick={() => setAddAccountOpen(true)}>
              <Plus className="h-2.5 w-2.5 mr-1" />
              Добавить аккаунты
            </Button>
            <Button
              variant="outline"
              className="border-slate-600 text-slate-300 h-6 text-[10px]"
              onClick={() => setImportOpen(true)}
            >
              <Upload className="h-2.5 w-2.5 mr-1" />
              Импорт CSV
            </Button>
          </div>
        </div>

        <div className={`flex flex-col sm:flex-row ${COMPACT_STYLES.gap}`}>
          <div className="relative flex-1">
            <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 text-slate-400 h-3 w-3" />
            <Input
              placeholder="Поиск по username или ID..."
              className="pl-7 bg-slate-800 border-slate-700 text-white h-6 text-xs"
            />
          </div>
          <Button
            variant="outline"
            className="border-slate-600 text-slate-300 h-6 text-[10px]"
            onClick={() => setFiltersOpen(true)}
          >
            <Filter className="h-2.5 w-2.5 mr-1" />
            Фильтры
          </Button>
        </div>

        <TabsContent value="all" className={`space-y-2`}>
          <div className={`grid ${COMPACT_STYLES.gridGap}`}>
            {accounts.map((account) => (
              <Card
                key={account.id}
                className="bg-slate-800/50 border-slate-700 backdrop-blur-sm hover:bg-slate-800/70 transition-all duration-300"
              >
                <CardContent className={COMPACT_STYLES.cardPadding}>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-purple-500 rounded-full flex items-center justify-center">
                        <Users className="h-4 w-4 text-white" />
                      </div>
                      <div>
                        <h3 className="text-sm font-semibold text-white">{account.username}</h3>
                        <div className="flex items-center gap-1 mt-0.5">
                          <Badge
                            className={`${getStatusColor(account.status)} ${COMPACT_STYLES.badgePadding} text-[8px]`}
                          >
                            {getStatusIcon(account.status)}
                            <span className="ml-0.5 capitalize">{account.status}</span>
                          </Badge>
                          <Badge
                            variant="outline"
                            className={`border-slate-600 text-slate-300 ${COMPACT_STYLES.badgePadding} text-[8px]`}
                          >
                            {account.proxy}
                          </Badge>
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-4">
                      <div className="text-center">
                        <div className="text-base font-bold text-white">{account.followers}</div>
                        <div className="text-[10px] text-slate-400">Подписчики</div>
                      </div>
                      <div className="text-center">
                        <div className="text-base font-bold text-white">{account.posts}</div>
                        <div className="text-[10px] text-slate-400">Посты</div>
                      </div>
                      <Button
                        variant="ghost"
                        size="sm"
                        className={`text-slate-400 hover:text-white ${COMPACT_STYLES.iconButtonSize}`}
                      >
                        <MoreHorizontal className="h-3 w-3" />
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </TabsContent>

        <TabsContent value="active">
          <Card className="bg-slate-800/50 border-slate-700">
            <CardHeader className={COMPACT_STYLES.cardHeaderPadding}>
              <CardTitle className="text-white text-sm">Активные аккаунты</CardTitle>
              <CardDescription className="text-slate-400 text-[10px]">
                Аккаунты, которые готовы к работе и публикациям
              </CardDescription>
            </CardHeader>
          </Card>
        </TabsContent>

        <TabsContent value="warming">
          <Card className="bg-slate-800/50 border-slate-700">
            <CardHeader className={COMPACT_STYLES.cardHeaderPadding}>
              <CardTitle className="text-white text-sm">Прогрев аккаунтов</CardTitle>
              <CardDescription className="text-slate-400 text-[10px]">
                Аккаунты в процессе прогрева для безопасного использования
              </CardDescription>
            </CardHeader>
          </Card>
        </TabsContent>

        <TabsContent value="error">
          <Card className="bg-slate-800/50 border-slate-700">
            <CardHeader className={COMPACT_STYLES.cardHeaderPadding}>
              <CardTitle className="text-white text-sm">Проблемные аккаунты</CardTitle>
              <CardDescription className="text-slate-400 text-[10px]">
                Аккаунты, требующие внимания или исправления ошибок
              </CardDescription>
            </CardHeader>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Модальные окна */}
      <AddAccountDialog open={addAccountOpen} onOpenChange={setAddAccountOpen} />
      <AccountFiltersDialog open={filtersOpen} onOpenChange={setFiltersOpen} />
      <ImportAccountsDialog open={importOpen} onOpenChange={setImportOpen} />
    </SidebarNavigation>
  )
}
