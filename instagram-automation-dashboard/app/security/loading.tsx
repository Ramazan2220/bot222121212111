import { Skeleton } from "@/components/ui/skeleton"
import { Card, CardContent, CardHeader } from "@/components/ui/card"

export default function SecurityLoading() {
  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="mb-8">
        <Skeleton className="h-10 w-64 mb-2 bg-slate-700" />
        <Skeleton className="h-6 w-96 bg-slate-700" />
      </div>

      {/* Security Score Card */}
      <Card className="bg-slate-800/50 border-slate-700">
        <CardContent className="p-6">
          <div className="flex flex-col md:flex-row items-center justify-between gap-6">
            <div className="flex items-center gap-4">
              <Skeleton className="w-24 h-24 rounded-full bg-slate-700" />
              <div className="space-y-2">
                <Skeleton className="h-6 w-48 bg-slate-700" />
                <Skeleton className="h-4 w-32 bg-slate-700" />
                <Skeleton className="h-4 w-40 bg-slate-700" />
              </div>
            </div>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
              <Skeleton className="h-16 w-20 rounded-lg bg-slate-700" />
              <Skeleton className="h-16 w-20 rounded-lg bg-slate-700" />
              <Skeleton className="h-16 w-20 rounded-lg bg-slate-700" />
            </div>
            <Skeleton className="h-10 w-32 bg-slate-700" />
          </div>
        </CardContent>
      </Card>

      {/* Tabs */}
      <div className="space-y-6">
        <div className="flex space-x-1 bg-slate-800 p-1 rounded-lg">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-10 w-24 bg-slate-700" />
          ))}
        </div>

        {/* Content Cards */}
        <div className="space-y-6">
          <Card className="bg-slate-800/50 border-slate-700">
            <CardHeader>
              <Skeleton className="h-6 w-64 bg-slate-700" />
              <Skeleton className="h-4 w-96 bg-slate-700" />
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {Array.from({ length: 4 }).map((_, i) => (
                  <div key={i} className="bg-slate-700/50 rounded-lg p-4">
                    <div className="flex items-start justify-between mb-4">
                      <div className="flex items-start gap-3">
                        <Skeleton className="w-5 h-5 bg-slate-600" />
                        <div className="space-y-2">
                          <Skeleton className="h-5 w-32 bg-slate-600" />
                          <Skeleton className="h-4 w-48 bg-slate-600" />
                        </div>
                      </div>
                      <Skeleton className="w-10 h-6 bg-slate-600" />
                    </div>
                    <div className="space-y-2">
                      <Skeleton className="h-4 w-full bg-slate-600" />
                      <Skeleton className="h-4 w-3/4 bg-slate-600" />
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardHeader>
              <Skeleton className="h-6 w-48 bg-slate-700" />
              <Skeleton className="h-4 w-80 bg-slate-700" />
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  {Array.from({ length: 6 }).map((_, i) => (
                    <div key={i} className="bg-slate-700/50 rounded-lg p-4 flex items-center gap-3">
                      <Skeleton className="w-10 h-10 rounded-full bg-slate-600" />
                      <div className="space-y-1">
                        <Skeleton className="h-4 w-24 bg-slate-600" />
                        <Skeleton className="h-3 w-16 bg-slate-600" />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}
