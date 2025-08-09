"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Checkbox } from "@/components/ui/checkbox"
import { Upload, ImageIcon, Video, Calendar, Hash, Clock, Send, Eye, CheckCircle } from "lucide-react"
import SidebarNavigation from "@/components/sidebar-navigation"
import MediaUploader from "@/components/media-uploader"
import AccountSelector from "@/components/account-selector"
import PostQueue from "@/components/post-queue"

export default function PostsPage() {
  const [selectedAccounts, setSelectedAccounts] = useState<string[]>([])
  const [postType, setPostType] = useState("post")
  const [caption, setCaption] = useState("")
  const [scheduledTime, setScheduledTime] = useState("")
  const [uploadedFiles, setUploadedFiles] = useState<File[]>([])

  const postTypes = [
    { value: "post", label: "Пост", icon: ImageIcon, desc: "Обычная публикация в ленте" },
    { value: "reel", label: "Reels", icon: Video, desc: "Короткое видео до 90 секунд" },
    { value: "story", label: "Stories", icon: Clock, desc: "Временная публикация на 24 часа" },
    { value: "carousel", label: "Карусель", icon: ImageIcon, desc: "Несколько фото/видео в одном посте" },
  ]

  const handlePublish = () => {
    console.log("Publishing to accounts:", selectedAccounts)
    console.log("Post type:", postType)
    console.log("Caption:", caption)
    console.log("Files:", uploadedFiles)
  }

  return (
    <SidebarNavigation>
      <div className="mb-8">
        <h1 className="text-4xl font-bold text-white mb-2 bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
          Массовые публикации
        </h1>
        <p className="text-slate-300 text-lg">Создавайте и публикуйте контент на множество аккаунтов одновременно</p>
      </div>

      <Tabs defaultValue="create" className="space-y-6">
        <TabsList className="bg-slate-800 border-slate-700">
          <TabsTrigger value="create" className="data-[state=active]:bg-blue-600">
            <Upload className="h-4 w-4 mr-2" />
            Создать публикацию
          </TabsTrigger>
          <TabsTrigger value="queue" className="data-[state=active]:bg-purple-600">
            <Clock className="h-4 w-4 mr-2" />
            Очередь публикаций
          </TabsTrigger>
          <TabsTrigger value="history" className="data-[state=active]:bg-green-600">
            <CheckCircle className="h-4 w-4 mr-2" />
            История
          </TabsTrigger>
        </TabsList>

        <TabsContent value="create" className="space-y-6">
          <div className="grid lg:grid-cols-3 gap-6">
            {/* Left Column - Content Creation */}
            <div className="lg:col-span-2 space-y-6">
              {/* Post Type Selection */}
              <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
                <CardHeader>
                  <CardTitle className="text-white flex items-center gap-2">
                    <Hash className="h-5 w-5 text-blue-400" />
                    Тип публикации
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 gap-3">
                    {postTypes.map((type) => (
                      <div
                        key={type.value}
                        className={`p-4 rounded-lg border-2 cursor-pointer transition-all ${
                          postType === type.value
                            ? "border-blue-500 bg-blue-500/10"
                            : "border-slate-600 hover:border-slate-500"
                        }`}
                        onClick={() => setPostType(type.value)}
                      >
                        <div className="flex items-center gap-3 mb-2">
                          <type.icon
                            className={`h-5 w-5 ${postType === type.value ? "text-blue-400" : "text-slate-400"}`}
                          />
                          <span className={`font-medium ${postType === type.value ? "text-white" : "text-slate-300"}`}>
                            {type.label}
                          </span>
                        </div>
                        <p className="text-sm text-slate-400">{type.desc}</p>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>

              {/* Media Upload */}
              <MediaUploader
                onFilesChange={setUploadedFiles}
                maxFiles={postType === "carousel" ? 10 : 1}
                acceptedTypes={postType === "reel" ? "video/*" : "image/*,video/*"}
              />

              {/* Caption and Settings */}
              <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
                <CardHeader>
                  <CardTitle className="text-white">Описание и настройки</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <Label htmlFor="caption" className="text-slate-300">
                      Описание
                    </Label>
                    <Textarea
                      id="caption"
                      placeholder="Добавьте описание к вашей публикации... #хештеги"
                      value={caption}
                      onChange={(e) => setCaption(e.target.value)}
                      className="bg-slate-700 border-slate-600 text-white placeholder:text-slate-400 min-h-[120px]"
                    />
                    <div className="flex justify-between mt-2">
                      <span className="text-sm text-slate-400">{caption.length}/2200 символов</span>
                      <span className="text-sm text-slate-400">Хештегов: {(caption.match(/#\w+/g) || []).length}</span>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="location" className="text-slate-300">
                        Геолокация
                      </Label>
                      <Input
                        id="location"
                        placeholder="Добавить место"
                        className="bg-slate-700 border-slate-600 text-white"
                      />
                    </div>
                    <div>
                      <Label htmlFor="schedule" className="text-slate-300">
                        Запланировать
                      </Label>
                      <Input
                        id="schedule"
                        type="datetime-local"
                        value={scheduledTime}
                        onChange={(e) => setScheduledTime(e.target.value)}
                        className="bg-slate-700 border-slate-600 text-white"
                      />
                    </div>
                  </div>

                  <div className="space-y-3">
                    <Label className="text-slate-300">Дополнительные настройки</Label>
                    <div className="space-y-2">
                      <div className="flex items-center space-x-2">
                        <Checkbox id="comments" className="border-slate-600" />
                        <Label htmlFor="comments" className="text-sm text-slate-300">
                          Отключить комментарии
                        </Label>
                      </div>
                      <div className="flex items-center space-x-2">
                        <Checkbox id="likes" className="border-slate-600" />
                        <Label htmlFor="likes" className="text-sm text-slate-300">
                          Скрыть количество лайков
                        </Label>
                      </div>
                      <div className="flex items-center space-x-2">
                        <Checkbox id="crosspost" className="border-slate-600" />
                        <Label htmlFor="crosspost" className="text-sm text-slate-300">
                          Кросспостинг в Facebook
                        </Label>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Right Column - Account Selection and Actions */}
            <div className="space-y-6">
              <AccountSelector selectedAccounts={selectedAccounts} onSelectionChange={setSelectedAccounts} />

              {/* Publish Actions */}
              <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
                <CardHeader>
                  <CardTitle className="text-white flex items-center gap-2">
                    <Send className="h-5 w-5 text-green-400" />
                    Публикация
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="text-sm text-slate-300">
                    <div className="flex justify-between mb-2">
                      <span>Выбрано аккаунтов:</span>
                      <span className="text-white font-medium">{selectedAccounts.length}</span>
                    </div>
                    <div className="flex justify-between mb-2">
                      <span>Файлов загружено:</span>
                      <span className="text-white font-medium">{uploadedFiles.length}</span>
                    </div>
                    <div className="flex justify-between">
                      <span>Тип публикации:</span>
                      <span className="text-white font-medium capitalize">{postType}</span>
                    </div>
                  </div>

                  <div className="space-y-2">
                    {scheduledTime ? (
                      <Button
                        className="w-full bg-blue-600 hover:bg-blue-700"
                        onClick={handlePublish}
                        disabled={selectedAccounts.length === 0 || uploadedFiles.length === 0}
                      >
                        <Calendar className="h-4 w-4 mr-2" />
                        Запланировать публикацию
                      </Button>
                    ) : (
                      <Button
                        className="w-full bg-green-600 hover:bg-green-700"
                        onClick={handlePublish}
                        disabled={selectedAccounts.length === 0 || uploadedFiles.length === 0}
                      >
                        <Send className="h-4 w-4 mr-2" />
                        Опубликовать сейчас
                      </Button>
                    )}

                    <Button variant="outline" className="w-full border-slate-600 text-slate-300">
                      <Eye className="h-4 w-4 mr-2" />
                      Предварительный просмотр
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Quick Stats */}
              <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
                <CardHeader>
                  <CardTitle className="text-white text-sm">Статистика сегодня</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Опубликовано:</span>
                    <span className="text-green-400 font-medium">247</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">В очереди:</span>
                    <span className="text-yellow-400 font-medium">18</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Ошибки:</span>
                    <span className="text-red-400 font-medium">3</span>
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>
        </TabsContent>

        <TabsContent value="queue">
          <PostQueue />
        </TabsContent>

        <TabsContent value="history">
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur-sm">
            <CardHeader>
              <CardTitle className="text-white">История публикаций</CardTitle>
              <CardDescription className="text-slate-400">
                Просмотр всех выполненных публикаций за последние 30 дней
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="text-center py-8">
                <CheckCircle className="h-12 w-12 text-green-400 mx-auto mb-4" />
                <p className="text-slate-300">История публикаций будет отображаться здесь</p>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </SidebarNavigation>
  )
}
