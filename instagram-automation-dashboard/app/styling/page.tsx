"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { User, ImageIcon, Edit, Link, Palette, Upload, Save, Eye, Copy, Wand2, CheckCircle } from "lucide-react"
import SidebarNavigation from "@/components/sidebar-navigation"
import ProfileEditor from "@/components/profile-editor"
import MediaUploader from "@/components/media-uploader"
import ProfileTemplates from "@/components/profile-templates"
import AccountSelector from "@/components/account-selector"
import StylingProgress from "@/components/styling-progress"

export default function StylingPage() {
  const [selectedAccounts, setSelectedAccounts] = useState<string[]>([])
  const [activeTemplate, setActiveTemplate] = useState<string | null>(null)
  const [uploadedAvatars, setUploadedAvatars] = useState<File[]>([])
  const [uploadedContent, setUploadedContent] = useState<File[]>([])

  return (
    <SidebarNavigation>
      <div className="mb-8">
        <h1 className="text-4xl font-bold text-white mb-2 bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
          Оформление профилей
        </h1>
        <p className="text-slate-300 text-lg">
          Массовое оформление Instagram профилей: аватары, описания, ссылки и контент
        </p>
      </div>

      <Tabs defaultValue="editor" className="space-y-6">
        <div className="flex flex-col sm:flex-row gap-4 justify-between">
          <TabsList className="bg-slate-800 border-slate-700">
            <TabsTrigger value="editor" className="data-[state=active]:bg-blue-600">
              <Edit className="h-4 w-4 mr-2" />
              Редактор профилей
            </TabsTrigger>
            <TabsTrigger value="media" className="data-[state=active]:bg-green-600">
              <ImageIcon className="h-4 w-4 mr-2" />
              Медиа контент
            </TabsTrigger>
            <TabsTrigger value="templates" className="data-[state=active]:bg-purple-600">
              <Palette className="h-4 w-4 mr-2" />
              Шаблоны
            </TabsTrigger>
            <TabsTrigger value="progress" className="data-[state=active]:bg-orange-600">
              <CheckCircle className="h-4 w-4 mr-2" />
              Прогресс
            </TabsTrigger>
          </TabsList>

          <div className="flex gap-2">
            <Button className="bg-blue-600 hover:bg-blue-700">
              <Wand2 className="h-4 w-4 mr-2" />
              Применить к выбранным
            </Button>
            <Button variant="outline" className="border-slate-600 text-slate-300">
              <Eye className="h-4 w-4 mr-2" />
              Предпросмотр
            </Button>
          </div>
        </div>

        <TabsContent value="editor" className="space-y-6">
          <div className="grid lg:grid-cols-3 gap-6">
            {/* Left Column - Profile Editor */}
            <div className="lg:col-span-2">
              <ProfileEditor />
            </div>

            {/* Right Column - Account Selection */}
            <div className="space-y-6">
              <AccountSelector selectedAccounts={selectedAccounts} onSelectionChange={setSelectedAccounts} />

              {/* Quick Stats */}
              <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
                <CardHeader>
                  <CardTitle className="text-white text-sm">Статистика оформления</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Оформлено сегодня:</span>
                    <span className="text-green-400 font-medium">23</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">В процессе:</span>
                    <span className="text-yellow-400 font-medium">5</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Ошибки:</span>
                    <span className="text-red-400 font-medium">1</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Успешность:</span>
                    <span className="text-blue-400 font-medium">96%</span>
                  </div>
                </CardContent>
              </Card>

              {/* Profile Preview */}
              <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
                <CardHeader>
                  <CardTitle className="text-white text-sm flex items-center gap-2">
                    <Eye className="h-4 w-4 text-blue-400" />
                    Предпросмотр профиля
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex items-center gap-3">
                    <div className="w-16 h-16 bg-gradient-to-r from-blue-500 to-purple-500 rounded-full flex items-center justify-center">
                      <User className="h-8 w-8 text-white" />
                    </div>
                    <div className="flex-1">
                      <h3 className="text-white font-semibold">@example_account</h3>
                      <p className="text-slate-400 text-sm">Имя профиля</p>
                    </div>
                  </div>
                  <div className="space-y-2">
                    <p className="text-slate-300 text-sm">Описание профиля будет отображаться здесь...</p>
                    <div className="flex items-center gap-2">
                      <Link className="h-4 w-4 text-blue-400" />
                      <span className="text-blue-400 text-sm">example.com</span>
                    </div>
                  </div>
                  <div className="grid grid-cols-3 gap-1">
                    {[1, 2, 3, 4, 5, 6].map((i) => (
                      <div key={i} className="aspect-square bg-slate-700 rounded"></div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>
        </TabsContent>

        <TabsContent value="media" className="space-y-6">
          <div className="grid lg:grid-cols-2 gap-6">
            {/* Avatar Upload */}
            <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2">
                  <User className="h-5 w-5 text-blue-400" />
                  Аватары профилей
                </CardTitle>
                <CardDescription className="text-slate-400">
                  Загрузите изображения для использования в качестве аватаров
                </CardDescription>
              </CardHeader>
              <CardContent>
                <MediaUploader onFilesChange={setUploadedAvatars} maxFiles={50} acceptedTypes="image/*" />

                {uploadedAvatars.length > 0 && (
                  <div className="mt-4 space-y-3">
                    <h4 className="text-white font-medium">Загруженные аватары ({uploadedAvatars.length})</h4>
                    <div className="grid grid-cols-4 gap-3">
                      {uploadedAvatars.slice(0, 8).map((file, index) => (
                        <div
                          key={index}
                          className="aspect-square bg-slate-700 rounded-lg flex items-center justify-center"
                        >
                          <ImageIcon className="h-6 w-6 text-slate-400" />
                        </div>
                      ))}
                      {uploadedAvatars.length > 8 && (
                        <div className="aspect-square bg-slate-700 rounded-lg flex items-center justify-center">
                          <span className="text-slate-400 text-sm">+{uploadedAvatars.length - 8}</span>
                        </div>
                      )}
                    </div>
                    <div className="flex gap-2">
                      <Button variant="outline" className="border-slate-600 text-slate-300">
                        <Wand2 className="h-4 w-4 mr-2" />
                        Случайное распределение
                      </Button>
                      <Button variant="outline" className="border-slate-600 text-slate-300">
                        <Copy className="h-4 w-4 mr-2" />
                        Дублировать на все
                      </Button>
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Content Upload */}
            <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2">
                  <ImageIcon className="h-5 w-5 text-green-400" />
                  Контент для профилей
                </CardTitle>
                <CardDescription className="text-slate-400">Загрузите посты для оформления профилей</CardDescription>
              </CardHeader>
              <CardContent>
                <MediaUploader onFilesChange={setUploadedContent} maxFiles={200} acceptedTypes="image/*,video/*" />

                {uploadedContent.length > 0 && (
                  <div className="mt-4 space-y-3">
                    <h4 className="text-white font-medium">Загруженный контент ({uploadedContent.length})</h4>
                    <div className="grid grid-cols-6 gap-2">
                      {uploadedContent.slice(0, 12).map((file, index) => (
                        <div
                          key={index}
                          className="aspect-square bg-slate-700 rounded flex items-center justify-center"
                        >
                          <ImageIcon className="h-4 w-4 text-slate-400" />
                        </div>
                      ))}
                      {uploadedContent.length > 12 && (
                        <div className="aspect-square bg-slate-700 rounded flex items-center justify-center">
                          <span className="text-slate-400 text-xs">+{uploadedContent.length - 12}</span>
                        </div>
                      )}
                    </div>
                    <div className="space-y-2">
                      <Label className="text-slate-300">Количество постов на аккаунт</Label>
                      <div className="flex items-center gap-4">
                        <Input
                          type="number"
                          defaultValue="6"
                          min="1"
                          max="20"
                          className="w-20 bg-slate-700 border-slate-600 text-white"
                        />
                        <span className="text-slate-400 text-sm">постов на профиль</span>
                      </div>
                    </div>
                    <Button className="bg-green-600 hover:bg-green-700">
                      <Upload className="h-4 w-4 mr-2" />
                      Распределить контент
                    </Button>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Content Distribution Settings */}
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
            <CardHeader>
              <CardTitle className="text-white">Настройки распределения контента</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="space-y-2">
                  <Label className="text-slate-300">Стратегия распределения</Label>
                  <select className="w-full bg-slate-700 border-slate-600 text-white rounded-md px-3 py-2">
                    <option value="random">Случайное распределение</option>
                    <option value="sequential">Последовательное</option>
                    <option value="balanced">Сбалансированное</option>
                    <option value="themed">По тематикам</option>
                  </select>
                </div>

                <div className="space-y-2">
                  <Label className="text-slate-300">Интервал публикации</Label>
                  <select className="w-full bg-slate-700 border-slate-600 text-white rounded-md px-3 py-2">
                    <option value="immediate">Сразу после оформления</option>
                    <option value="1h">Каждый час</option>
                    <option value="6h">Каждые 6 часов</option>
                    <option value="24h">Раз в день</option>
                  </select>
                </div>

                <div className="space-y-2">
                  <Label className="text-slate-300">Добавить описания</Label>
                  <select className="w-full bg-slate-700 border-slate-600 text-white rounded-md px-3 py-2">
                    <option value="none">Без описаний</option>
                    <option value="auto">Автоматические</option>
                    <option value="template">По шаблону</option>
                    <option value="ai">ИИ генерация</option>
                  </select>
                </div>
              </div>

              <div className="flex items-center gap-4">
                <Button className="bg-blue-600 hover:bg-blue-700">
                  <Save className="h-4 w-4 mr-2" />
                  Сохранить настройки
                </Button>
                <Button variant="outline" className="border-slate-600 text-slate-300">
                  <Eye className="h-4 w-4 mr-2" />
                  Предпросмотр распределения
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="templates">
          <ProfileTemplates activeTemplate={activeTemplate} onTemplateSelect={setActiveTemplate} />
        </TabsContent>

        <TabsContent value="progress">
          <StylingProgress />
        </TabsContent>
      </Tabs>
    </SidebarNavigation>
  )
}
