# 🔥 ПЛАН ПОЛНОЙ ИНТЕГРАЦИИ: Telegram Bot → Web Dashboard

## 📊 **ЭТАП 1: REST API BACKEND (Day 1-2)**

### **1.1 Создание FastAPI структуры**
```bash
mkdir api
cd api
touch __init__.py main.py
mkdir endpoints auth models
```

### **1.2 Основные API endpoints**

#### **📁 api/main.py**
```python
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from database.db_manager import get_session
from database.models import InstagramAccount, Proxy, PublishTask
from instagram.health_monitor import AdvancedHealthMonitor
from instagram.activity_limiter import ActivityLimiter
from instagram.improved_account_warmer import warm_account_improved
from instagram.lifecycle_manager import AccountLifecycleManager
from instagram.predictive_monitor import PredictiveMonitor
from instagram.advanced_verification import AdvancedVerificationSystem

app = FastAPI(title="Instagram Automation API", version="1.0.0")

# CORS для работы с Next.js
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://your-vercel-domain.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === ACCOUNTS MANAGEMENT ===
@app.get("/api/accounts")
async def get_accounts():
    """Получить список всех аккаунтов"""
    session = get_session()
    accounts = session.query(InstagramAccount).all()
    session.close()
    
    return [{
        "id": account.id,
        "username": account.username,
        "email": account.email,
        "is_active": account.is_active,
        "created_at": account.created_at.isoformat(),
        "full_name": account.full_name,
        "biography": account.biography
    } for account in accounts]

@app.post("/api/accounts")
async def add_account(username: str, password: str, email: str = None, email_password: str = None):
    """Добавить новый аккаунт"""
    from database.db_manager import add_instagram_account_without_login
    
    account = add_instagram_account_without_login(username, password, email, email_password)
    if account:
        return {
            "success": True,
            "account_id": account.id,
            "message": f"Аккаунт {username} успешно добавлен"
        }
    else:
        raise HTTPException(status_code=400, detail="Ошибка при добавлении аккаунта")

@app.delete("/api/accounts/{account_id}")
async def delete_account(account_id: int):
    """Удалить аккаунт"""
    from database.db_manager import delete_instagram_account
    
    success = delete_instagram_account(account_id)
    if success:
        return {"success": True, "message": "Аккаунт удален"}
    else:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")

@app.post("/api/accounts/bulk-upload")
async def bulk_upload_accounts(file: UploadFile = File(...)):
    """Массовая загрузка аккаунтов из файла"""
    content = await file.read()
    lines = content.decode().strip().split('\n')
    
    added_accounts = []
    errors = []
    
    for line in lines:
        try:
            parts = line.strip().split(':')
            if len(parts) >= 2:
                username, password = parts[0], parts[1]
                email = parts[2] if len(parts) > 2 else None
                email_password = parts[3] if len(parts) > 3 else None
                
                account = add_instagram_account_without_login(username, password, email, email_password)
                if account:
                    added_accounts.append(username)
                else:
                    errors.append(f"Ошибка добавления {username}")
        except Exception as e:
            errors.append(f"Ошибка в строке: {line} - {str(e)}")
    
    return {
        "success": True,
        "added_count": len(added_accounts),
        "added_accounts": added_accounts,
        "errors": errors
    }

# === HEALTH MONITORING ===
@app.get("/api/health/{account_id}")
async def get_health_score(account_id: int):
    """Получить health score аккаунта"""
    monitor = AdvancedHealthMonitor()
    try:
        score = monitor.calculate_comprehensive_health_score(account_id)
        recommendations = monitor.get_health_recommendations(account_id)
        
        return {
            "account_id": account_id,
            "health_score": score,
            "recommendations": recommendations,
            "status": "healthy" if score > 70 else "warning" if score > 40 else "critical"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/health")
async def get_all_health_scores():
    """Получить health scores всех аккаунтов"""
    accounts = get_instagram_accounts()
    monitor = AdvancedHealthMonitor()
    
    results = []
    for account in accounts:
        try:
            score = monitor.calculate_comprehensive_health_score(account.id)
            results.append({
                "account_id": account.id,
                "username": account.username,
                "health_score": score,
                "status": "healthy" if score > 70 else "warning" if score > 40 else "critical"
            })
        except Exception as e:
            results.append({
                "account_id": account.id,
                "username": account.username,
                "health_score": 0,
                "status": "error",
                "error": str(e)
            })
    
    return results

# === ACTIVITY LIMITS ===
@app.get("/api/limits/{account_id}")
async def get_activity_limits(account_id: int):
    """Получить лимиты активности аккаунта"""
    limiter = ActivityLimiter()
    try:
        limits = limiter.get_dynamic_limits(account_id)
        restrictions = limiter.check_current_restrictions(account_id)
        
        return {
            "account_id": account_id,
            "limits": limits,
            "restrictions": restrictions,
            "safe_delays": {
                "follow": limiter.calculate_safe_delay("follow"),
                "like": limiter.calculate_safe_delay("like"),
                "comment": limiter.calculate_safe_delay("comment")
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === WARMUP CONTROL ===
@app.post("/api/warmup/{account_id}")
async def start_warmup(account_id: int, duration_minutes: int = 30):
    """Запустить прогрев аккаунта"""
    try:
        result = warm_account_improved(account_id, duration_minutes=duration_minutes)
        return {
            "success": True,
            "account_id": account_id,
            "message": f"Прогрев запущен на {duration_minutes} минут",
            "result": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/warmup/status/{account_id}")
async def get_warmup_status(account_id: int):
    """Получить статус прогрева аккаунта"""
    manager = AccountLifecycleManager()
    try:
        stage = manager.determine_account_stage(account_id)
        recommendations = manager.get_stage_recommendations(stage)
        
        return {
            "account_id": account_id,
            "lifecycle_stage": stage,
            "recommendations": recommendations,
            "is_warming": stage == "WARMING"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === ANALYTICS & PREDICTION ===
@app.get("/api/analytics/{account_id}")
async def get_account_analytics(account_id: int):
    """Получить аналитику и предсказания рисков"""
    monitor = PredictiveMonitor()
    try:
        risk_score = monitor.calculate_ban_risk_score(account_id)
        patterns = monitor.analyze_activity_patterns(account_id)
        advice = monitor.get_risk_mitigation_advice(account_id)
        
        return {
            "account_id": account_id,
            "risk_score": risk_score,
            "risk_level": "low" if risk_score < 30 else "medium" if risk_score < 60 else "high",
            "patterns": patterns,
            "mitigation_advice": advice
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === PUBLISHING ===
@app.get("/api/tasks")
async def get_publish_tasks():
    """Получить список задач публикации"""
    session = get_session()
    tasks = session.query(PublishTask).all()
    session.close()
    
    return [{
        "id": task.id,
        "account_id": task.account_id,
        "task_type": task.task_type.value,
        "status": task.status.value,
        "media_path": task.media_path,
        "caption": task.caption,
        "scheduled_time": task.scheduled_time.isoformat() if task.scheduled_time else None,
        "created_at": task.created_at.isoformat(),
        "error_message": task.error_message
    } for task in tasks]

@app.post("/api/publish")
async def create_publish_task(
    account_id: int,
    task_type: str,
    caption: str = None,
    scheduled_time: str = None
):
    """Создать задачу публикации"""
    from database.db_manager import add_publish_task
    from database.models import TaskType, TaskStatus
    
    try:
        task_type_enum = TaskType[task_type.upper()]
        scheduled_datetime = datetime.fromisoformat(scheduled_time) if scheduled_time else None
        
        task_id = add_publish_task(
            account_id=account_id,
            task_type=task_type_enum,
            caption=caption,
            scheduled_time=scheduled_datetime
        )
        
        return {
            "success": True,
            "task_id": task_id,
            "message": "Задача создана"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === PROXY MANAGEMENT ===
@app.get("/api/proxies")
async def get_proxies():
    """Получить список прокси"""
    from database.db_manager import get_all_proxies
    
    proxies = get_all_proxies()
    return [{
        "id": proxy.id,
        "host": proxy.host,
        "port": proxy.port,
        "username": proxy.username,
        "protocol": proxy.protocol,
        "is_active": proxy.is_active,
        "created_at": proxy.created_at.isoformat()
    } for proxy in proxies]

@app.post("/api/proxies")
async def add_proxy(host: str, port: int, username: str = None, password: str = None, protocol: str = "http"):
    """Добавить новый прокси"""
    from database.db_manager import add_proxy
    
    proxy_id = add_proxy(host, port, username, password, protocol)
    if proxy_id:
        return {"success": True, "proxy_id": proxy_id}
    else:
        raise HTTPException(status_code=400, detail="Ошибка при добавлении прокси")

# === PROFILE MANAGEMENT ===
@app.get("/api/profile/{account_id}")
async def get_profile_info(account_id: int):
    """Получить информацию профиля"""
    from database.db_manager import get_instagram_account
    
    account = get_instagram_account(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    return {
        "account_id": account.id,
        "username": account.username,
        "full_name": account.full_name,
        "biography": account.biography,
        "email": account.email
    }

@app.put("/api/profile/{account_id}")
async def update_profile(account_id: int, full_name: str = None, biography: str = None):
    """Обновить профиль аккаунта"""
    from database.db_manager import update_account_profile
    
    success = update_account_profile(account_id, full_name, biography)
    if success:
        return {"success": True, "message": "Профиль обновлен"}
    else:
        raise HTTPException(status_code=500, detail="Ошибка при обновлении профиля")

# === SYSTEM STATUS ===
@app.get("/api/system/status")
async def get_system_status():
    """Получить общий статус системы"""
    accounts = get_instagram_accounts()
    
    total_accounts = len(accounts)
    active_accounts = len([a for a in accounts if a.is_active])
    
    # Получаем статистику задач
    session = get_session()
    total_tasks = session.query(PublishTask).count()
    pending_tasks = session.query(PublishTask).filter(PublishTask.status == TaskStatus.PENDING).count()
    session.close()
    
    return {
        "accounts": {
            "total": total_accounts,
            "active": active_accounts,
            "inactive": total_accounts - active_accounts
        },
        "tasks": {
            "total": total_tasks,
            "pending": pending_tasks,
            "completed": total_tasks - pending_tasks
        },
        "system": {
            "status": "operational",
            "uptime": "24h",
            "version": "1.0.0"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

---

## 📱 **ЭТАП 2: NEXT.JS INTEGRATION (Day 2-3)**

### **2.1 API Client для Next.js**

#### **📁 lib/api-client.ts**
```typescript
const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

class ApiClient {
  private async request(endpoint: string, options: RequestInit = {}) {
    const response = await fetch(`${API_BASE}${endpoint}`, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    })

    if (!response.ok) {
      throw new Error(`API Error: ${response.statusText}`)
    }

    return response.json()
  }

  // === ACCOUNTS ===
  async getAccounts() {
    return this.request('/api/accounts')
  }

  async addAccount(username: string, password: string, email?: string, emailPassword?: string) {
    return this.request('/api/accounts', {
      method: 'POST',
      body: JSON.stringify({ username, password, email, email_password: emailPassword }),
    })
  }

  async deleteAccount(accountId: number) {
    return this.request(`/api/accounts/${accountId}`, { method: 'DELETE' })
  }

  async bulkUploadAccounts(file: File) {
    const formData = new FormData()
    formData.append('file', file)
    
    return fetch(`${API_BASE}/api/accounts/bulk-upload`, {
      method: 'POST',
      body: formData,
    }).then(res => res.json())
  }

  // === HEALTH MONITORING ===
  async getHealthScore(accountId: number) {
    return this.request(`/api/health/${accountId}`)
  }

  async getAllHealthScores() {
    return this.request('/api/health')
  }

  // === ACTIVITY LIMITS ===
  async getActivityLimits(accountId: number) {
    return this.request(`/api/limits/${accountId}`)
  }

  // === WARMUP ===
  async startWarmup(accountId: number, durationMinutes: number = 30) {
    return this.request(`/api/warmup/${accountId}`, {
      method: 'POST',
      body: JSON.stringify({ duration_minutes: durationMinutes }),
    })
  }

  async getWarmupStatus(accountId: number) {
    return this.request(`/api/warmup/status/${accountId}`)
  }

  // === ANALYTICS ===
  async getAccountAnalytics(accountId: number) {
    return this.request(`/api/analytics/${accountId}`)
  }

  // === PUBLISHING ===
  async getPublishTasks() {
    return this.request('/api/tasks')
  }

  async createPublishTask(accountId: number, taskType: string, caption?: string, scheduledTime?: string) {
    return this.request('/api/publish', {
      method: 'POST',
      body: JSON.stringify({ account_id: accountId, task_type: taskType, caption, scheduled_time: scheduledTime }),
    })
  }

  // === PROXIES ===
  async getProxies() {
    return this.request('/api/proxies')
  }

  async addProxy(host: string, port: number, username?: string, password?: string, protocol: string = 'http') {
    return this.request('/api/proxies', {
      method: 'POST',
      body: JSON.stringify({ host, port, username, password, protocol }),
    })
  }

  // === PROFILE ===
  async getProfile(accountId: number) {
    return this.request(`/api/profile/${accountId}`)
  }

  async updateProfile(accountId: number, fullName?: string, biography?: string) {
    return this.request(`/api/profile/${accountId}`, {
      method: 'PUT',
      body: JSON.stringify({ full_name: fullName, biography }),
    })
  }

  // === SYSTEM ===
  async getSystemStatus() {
    return this.request('/api/system/status')
  }
}

export const apiClient = new ApiClient()
```

### **2.2 TypeScript Types**

#### **📁 lib/types.ts**
```typescript
export interface Account {
  id: number
  username: string
  email?: string
  is_active: boolean
  created_at: string
  full_name?: string
  biography?: string
}

export interface HealthScore {
  account_id: number
  username?: string
  health_score: number
  status: 'healthy' | 'warning' | 'critical' | 'error'
  recommendations?: string[]
  error?: string
}

export interface ActivityLimits {
  account_id: number
  limits: {
    daily_follows: number
    hourly_likes: number
    daily_comments: number
  }
  restrictions: any
  safe_delays: {
    follow: number
    like: number
    comment: number
  }
}

export interface PublishTask {
  id: number
  account_id: number
  task_type: string
  status: string
  media_path?: string
  caption?: string
  scheduled_time?: string
  created_at: string
  error_message?: string
}

export interface Proxy {
  id: number
  host: string
  port: number
  username?: string
  protocol: string
  is_active: boolean
  created_at: string
}

export interface SystemStatus {
  accounts: {
    total: number
    active: number
    inactive: number
  }
  tasks: {
    total: number
    pending: number
    completed: number
  }
  system: {
    status: string
    uptime: string
    version: string
  }
}
```

---

## 🎯 **ЭТАП 3: UI COMPONENTS REPLACEMENT (Day 3-4)**

### **3.1 Обновление страницы аккаунтов**

#### **📁 app/accounts/page.tsx**
```typescript
"use client"

import { useState, useEffect } from 'react'
import { apiClient } from '@/lib/api-client'
import { Account, HealthScore } from '@/lib/types'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Plus, Upload, RefreshCw } from 'lucide-react'

export default function AccountsPage() {
  const [accounts, setAccounts] = useState<Account[]>([])
  const [healthScores, setHealthScores] = useState<HealthScore[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    loadData()
  }, [])

  const loadData = async () => {
    try {
      setLoading(true)
      const [accountsData, healthData] = await Promise.all([
        apiClient.getAccounts(),
        apiClient.getAllHealthScores()
      ])
      setAccounts(accountsData)
      setHealthScores(healthData)
    } catch (error) {
      console.error('Error loading data:', error)
    } finally {
      setLoading(false)
    }
  }

  const getHealthScore = (accountId: number) => {
    return healthScores.find(h => h.account_id === accountId)
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'healthy': return 'bg-green-500/20 text-green-400 border-green-500/30'
      case 'warning': return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30'
      case 'critical': return 'bg-red-500/20 text-red-400 border-red-500/30'
      default: return 'bg-slate-500/20 text-slate-400 border-slate-500/30'
    }
  }

  if (loading) {
    return <div className="flex items-center justify-center h-64">
      <RefreshCw className="h-8 w-8 animate-spin" />
    </div>
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-3xl font-bold text-white">Управление аккаунтами</h1>
        <div className="flex gap-2">
          <Button className="bg-blue-600 hover:bg-blue-700">
            <Plus className="h-4 w-4 mr-2" />
            Добавить аккаунт
          </Button>
          <Button variant="outline" className="border-slate-600 text-slate-300">
            <Upload className="h-4 w-4 mr-2" />
            Импорт CSV
          </Button>
        </div>
      </div>

      <div className="grid gap-4">
        {accounts.map((account) => {
          const health = getHealthScore(account.id)
          return (
            <Card key={account.id} className="bg-slate-800/50 border-slate-700">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    <div className="w-12 h-12 bg-gradient-to-r from-blue-500 to-purple-500 rounded-full flex items-center justify-center">
                      <span className="text-white font-bold">
                        {account.username.charAt(0).toUpperCase()}
                      </span>
                    </div>
                    <div>
                      <h3 className="text-lg font-semibold text-white">@{account.username}</h3>
                      <p className="text-slate-400">{account.email || 'Нет email'}</p>
                    </div>
                  </div>

                  <div className="flex items-center gap-4">
                    {health && (
                      <div className="text-center">
                        <div className="text-2xl font-bold text-white">{health.health_score}/100</div>
                        <Badge className={getStatusColor(health.status)}>
                          {health.status}
                        </Badge>
                      </div>
                    )}
                    
                    <Badge className={account.is_active ? 'bg-green-600' : 'bg-red-600'}>
                      {account.is_active ? 'Активен' : 'Неактивен'}
                    </Badge>

                    <Button variant="ghost" size="sm" className="text-slate-400 hover:text-white">
                      Управление
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          )
        })}
      </div>
    </div>
  )
}
```

---

## ⚡ **ЭТАП 4: REAL-TIME FEATURES (Day 4-5)**

### **4.1 Live Updates Hook**

#### **📁 hooks/use-realtime.ts**
```typescript
import { useState, useEffect, useRef } from 'react'

export function useRealtime<T>(
  fetchFunction: () => Promise<T>,
  interval: number = 5000
) {
  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const intervalRef = useRef<NodeJS.Timeout>()

  const fetchData = async () => {
    try {
      setError(null)
      const result = await fetchFunction()
      setData(result)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
    
    intervalRef.current = setInterval(fetchData, interval)
    
    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
      }
    }
  }, [interval])

  const refresh = () => {
    setLoading(true)
    fetchData()
  }

  return { data, loading, error, refresh }
}
```

---

## 🔥 **ИТОГОВЫЙ РЕЗУЛЬТАТ**

После выполнения всех этапов вы получите:

### **✅ Полнофункциональный веб-дашборд с:**
- 🔑 **Управление аккаунтами** (добавление, удаление, массовый импорт)
- 📊 **Health мониторинг** в реальном времени
- 🎯 **Контроль активности** и лимитов
- 🚀 **Прогрев аккаунтов** одним кликом
- 📱 **Публикация контента** через веб-интерфейс
- 🌐 **Управление прокси**
- ⚙️ **Настройка профилей**
- 📈 **Аналитика и предсказания**

### **⚡ Преимущества веб-дашборда:**
- **Удобный интерфейс** вместо команд Telegram
- **Визуальная аналитика** с графиками и диаграммами
- **Batch операции** (массовые действия)
- **Real-time мониторинг** всех процессов
- **Мобильная адаптация** для управления с телефона
- **Многопользовательский доступ** (в будущем)

### **🚀 Время реализации:**
- **День 1-2:** REST API Backend
- **День 3:** Next.js Integration  
- **День 4:** UI Components
- **День 5:** Real-time Features

**Результат:** Полная замена Telegram бота веб-дашбордом! 🔥

Готовы начать с API backend? 😎 