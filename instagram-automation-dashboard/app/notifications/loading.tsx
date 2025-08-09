import { Skeleton } from "@/components/ui/skeleton"
import { Card, CardContent, CardHeader } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import SidebarNavigation from "@/components/sidebar-navigation"

export default function NotificationsLoading() {
  return (
    <SidebarNavigation>
      <div className="space-y-6">
        <div>
          <Skeleton className="h-10 w-64 bg-slate-700" />
          <Skeleton className="h-5 w-96 mt-2 bg-slate-700" />
        </div>

        <Tabs defaultValue="notifications" className="space-y-4">
          <TabsList className="bg-slate-700/50 border-slate-700">
            <TabsTrigger value="notifications" className="data-[state=active]:bg-slate-600">
              Уведомления
            </TabsTrigger>
            <TabsTrigger value="settings" className="data-[state=active]:bg-slate-600">
              Настройки
            </TabsTrigger>
          </TabsList>

          <TabsContent value="notifications" className="space-y-4">
            <Card className="border-slate-700 bg-slate-800/50">
              <CardHeader className="pb-2">
                <Skeleton className="h-6 w-40 bg-slate-700" />
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  {Array(5)
                    .fill(0)
                    .map((_, i) => (
                      <div key={i} className="p-3 rounded-lg border border-slate-700 flex items-start gap-3">
                        <Skeleton className="h-5 w-5 rounded-full bg-slate-700" />
                        <div className="flex-1">
                          <Skeleton className="h-5 w-full max-w-[200px] bg-slate-700 mb-2" />
                          <Skeleton className="h-4 w-full bg-slate-700 mb-3" />
                          <div className="flex items-center gap-2">
                            <Skeleton className="h-5 w-16 rounded-full bg-slate-700" />
                            <Skeleton className="h-5 w-20 rounded-full bg-slate-700" />
                          </div>
                        </div>
                      </div>
                    ))}
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="settings" className="space-y-4">
            <Card className="border-slate-700 bg-slate-800/50">
              <CardHeader>
                <Skeleton className="h-6 w-48 bg-slate-700" />
                <Skeleton className="h-4 w-64 bg-slate-700 mt-2" />
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                  <div className="md:col-span-2">
                    <Skeleton className="h-10 w-full bg-slate-700 mb-4" />
                    <div className="space-y-4">
                      {Array(6)
                        .fill(0)
                        .map((_, i) => (
                          <div key={i} className="flex items-center justify-between">
                            <div>
                              <Skeleton className="h-5 w-40 bg-slate-700 mb-1" />
                              <Skeleton className="h-3 w-56 bg-slate-700" />
                            </div>
                            <div className="flex items-center gap-8">
                              <Skeleton className="h-5 w-5 rounded-full bg-slate-700" />
                              <Skeleton className="h-5 w-5 rounded-full bg-slate-700" />
                              <Skeleton className="h-5 w-5 rounded-full bg-slate-700" />
                            </div>
                          </div>
                        ))}
                    </div>
                  </div>
                  <div>
                    <div className="space-y-6">
                      {Array(3)
                        .fill(0)
                        .map((_, i) => (
                          <div key={i} className="bg-slate-700/50 rounded-lg p-4">
                            <Skeleton className="h-5 w-32 bg-slate-700 mb-3" />
                            <div className="space-y-4">
                              {Array(3)
                                .fill(0)
                                .map((_, j) => (
                                  <div key={j} className="flex items-center justify-between">
                                    <Skeleton className="h-4 w-24 bg-slate-700" />
                                    <Skeleton className="h-5 w-10 rounded-full bg-slate-700" />
                                  </div>
                                ))}
                            </div>
                          </div>
                        ))}
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </SidebarNavigation>
  )
}
