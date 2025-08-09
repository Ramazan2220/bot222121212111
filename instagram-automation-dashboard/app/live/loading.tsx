import { Card, CardContent, CardHeader } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import SidebarNavigation from "@/components/sidebar-navigation"

export default function LiveMonitoringLoading() {
  return (
    <SidebarNavigation>
      <div className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <Skeleton className="h-12 w-80 mb-2 bg-slate-700" />
            <Skeleton className="h-6 w-96 bg-slate-700" />
          </div>
          <div className="flex items-center gap-3">
            <Skeleton className="h-8 w-32 bg-slate-700" />
            <Skeleton className="h-8 w-36 bg-slate-700" />
            <Skeleton className="h-8 w-28 bg-slate-700" />
          </div>
        </div>
      </div>

      <div className="mb-6">
        <Card className="bg-slate-800/50 border-slate-700">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <Skeleton className="h-5 w-5 bg-slate-600" />
              <div className="flex-1 space-y-2">
                <Skeleton className="h-4 w-48 bg-slate-600" />
                <Skeleton className="h-3 w-96 bg-slate-600" />
              </div>
              <Skeleton className="h-8 w-24 bg-slate-600" />
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="space-y-6">
        <div className="flex gap-2">
          <Skeleton className="h-10 w-24 bg-slate-700" />
          <Skeleton className="h-10 w-28 bg-slate-700" />
          <Skeleton className="h-10 w-32 bg-slate-700" />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <Card key={i} className="bg-slate-800/50 border-slate-700">
              <CardContent className="p-4">
                <div className="flex items-center justify-between mb-2">
                  <Skeleton className="h-10 w-10 bg-slate-600" />
                  <Skeleton className="h-6 w-16 bg-slate-600" />
                </div>
                <div className="space-y-1">
                  <Skeleton className="h-8 w-20 bg-slate-600" />
                  <Skeleton className="h-4 w-32 bg-slate-600" />
                </div>
              </CardContent>
            </Card>
          ))}
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card className="bg-slate-800/50 border-slate-700">
            <CardHeader>
              <Skeleton className="h-6 w-48 bg-slate-600" />
              <Skeleton className="h-4 w-64 bg-slate-600" />
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                {Array.from({ length: 4 }).map((_, i) => (
                  <div key={i} className="text-center p-3 rounded-lg bg-slate-700/30">
                    <Skeleton className="h-8 w-16 mx-auto mb-2 bg-slate-600" />
                    <Skeleton className="h-3 w-20 mx-auto bg-slate-600" />
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardHeader>
              <Skeleton className="h-6 w-56 bg-slate-600" />
              <Skeleton className="h-4 w-72 bg-slate-600" />
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {Array.from({ length: 5 }).map((_, i) => (
                  <div key={i} className="flex items-center justify-between p-2 rounded-lg bg-slate-700/30">
                    <div className="flex items-center gap-3">
                      <Skeleton className="h-6 w-6 bg-slate-600" />
                      <Skeleton className="h-4 w-32 bg-slate-600" />
                    </div>
                    <div className="flex items-center gap-4">
                      <Skeleton className="h-4 w-20 bg-slate-600" />
                      <Skeleton className="h-4 w-12 bg-slate-600" />
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </SidebarNavigation>
  )
}
