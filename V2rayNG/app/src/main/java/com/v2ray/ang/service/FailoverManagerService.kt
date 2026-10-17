package com.v2ray.ang.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat
import com.v2ray.ang.R
import com.v2ray.ang.dto.ServerConfig
import com.v2ray.ang.util.MmkvManager
import com.v2ray.ang.util.SpeedtestUtil
import com.v2ray.ang.util.V2rayConfigUtil
import kotlinx.coroutines.*
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

class FailoverManagerService : Service() {

    private val serviceScope = CoroutineScope(Dispatchers.Default + Job())
    private var isFailoverRunning = false
    private var currentActiveServerGuid: String? = null

    companion object {
        private const val CHANNEL_ID = "failover_monitor_channel"
        private const val NOTIFICATION_ID = 2048

        // Хосты и порты для контроля сети (при необходимости вынесите в настройки UI)
        const val CHECK_SRV_1_HOST = "1.1.1.1"
        const val CHECK_SRV_1_PORT = 53

        const val CHECK_SRV_2_HOST = "8.8.8.8"
        const val CHECK_SRV_2_PORT = 53

        fun start(context: Context) {
            val intent = Intent(context, FailoverManagerService::class.java)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(intent)
            } else {
                context.startService(intent)
            }
        }

        fun stop(context: Context) {
            context.stopService(Intent(context, FailoverManagerService::class.java))
        }
    }

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
        startForeground(NOTIFICATION_ID, buildNotification("Инициализация сервиса...", StatusColor.GREEN))
        startMonitoringLoop()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onDestroy() {
        super.onDestroy()
        serviceScope.cancel()
    }

    // --- Основной цикл мониторинга доступности ---
    private fun startMonitoringLoop() {
        serviceScope.launch(Dispatchers.IO) {
            while (isActive) {
                val srv1Up = pingSocket(CHECK_SRV_1_HOST, CHECK_SRV_1_PORT)

                if (srv1Up) {
                    // Сервер 1 доступен: интернет работает напрямую
                    isFailoverRunning = false
                    updateNotification("Основная сеть активна (Сервер 1 OK)", StatusColor.GREEN)

                    // Если был запущен VPN через перебор — останавливаем
                    if (V2RayServiceManager.v2rayPoint.isRunning && currentActiveServerGuid != null) {
                        V2RayServiceManager.stopVpnService(this@FailoverManagerService)
                        currentActiveServerGuid = null
                    }
                } else {
                    // Сервер 1 упал, проверяем Сервер 2
                    val srv2Up = pingSocket(CHECK_SRV_2_HOST, CHECK_SRV_2_PORT)

                    if (srv2Up) {
                        // Сервер 2 доступен -> Желтый статус и запуск подбора VPN
                        updateNotification("Сервер 1 недоступен. Поиск рабочего VPN...", StatusColor.YELLOW)

                        if (!isFailoverRunning && !V2RayServiceManager.v2rayPoint.isRunning) {
                            isFailoverRunning = true
                            launchVpnFailover()
                        }
                    } else {
                        // Оба сервера недоступны (интернета нет вообще)
                        updateNotification("Нет связи с контрольными серверами", StatusColor.RED)
                    }
                }

                delay(15_000) // Пауза 15 секунд между проверками
            }
        }
    }

    // --- Проверка TCP-доступности хоста ---
    private fun pingSocket(host: String, port: Int, timeoutMs: Int = 3000): Boolean {
        return try {
            Socket().use { socket ->
                socket.connect(InetSocketAddress(host, port), timeoutMs)
                true
            }
        } catch (e: IOException) {
            false
        }
    }

    // --- Логика перебора и тестирования VPN узлов ---
    private fun launchVpnFailover() {
        serviceScope.launch(Dispatchers.IO) {
            // Получаем список сохраненных серверов из хранилища v2rayNG (MMKV)
            val serverGuids = MmkvManager.decodeServerList()
            if (serverGuids.isEmpty()) {
                updateNotification("Ошибка: список VPN-серверов пуст", StatusColor.YELLOW)
                isFailoverRunning = false
                return@launch
            }

            val workingServers = mutableListOf<Pair<String, Long>>()
            val isFirstConnected = AtomicBoolean(false)
            val parallelDispatcher = Dispatchers.IO.limitedParallelism(5)

            // Запускаем параллельный замер пинга для серверов
            val jobs = serverGuids.map { guid ->
                launch(parallelDispatcher) {
                    val config = MmkvManager.decodeServerConfig(guid)
                    if (config != null) {
                        val ping = SpeedtestUtil.tcping(config.getProxyOutbound()?.getServerAddress() ?: "", config.getProxyOutbound()?.getServerPort() ?: 0)
                        
                        if (ping > 0) {
                            synchronized(workingServers) {
                                workingServers.add(Pair(guid, ping))
                            }

                            // 1. Первый найденный рабочий сервер подключаем сразу
                            if (isFirstConnected.compareAndSet(false, true)) {
                                connectToServer(guid)
                                updateNotification("Подключен VPN (первый найденный: ${ping}мс)", StatusColor.YELLOW)
                            }
                        }
                    }
                }
            }

            // Ждем завершения фонового тестирования остальных серверов
            jobs.joinAll()

            // 2. Сортируем все рабочие VPN по пингу
            val sortedList = synchronized(workingServers) {
                workingServers.sortedBy { it.second }
            }

            if (sortedList.isNotEmpty()) {
                val bestServer = sortedList.first()
                // Если лучший сервер отличается от того, что подключили первым — переключаем на него
                if (bestServer.first != currentActiveServerGuid) {
                    connectToServer(bestServer.first)
                    updateNotification("Подключен лучший VPN (${bestServer.second}мс)", StatusColor.YELLOW)
                }
            } else {
                updateNotification("Рабочих VPN-серверов не найдено", StatusColor.RED)
            }

            isFailoverRunning = false
        }
    }

    private fun connectToServer(guid: String) {
        currentActiveServerGuid = guid
        MmkvManager.selectServer(guid)
        V2RayServiceManager.startVpnService(this@FailoverManagerService)
    }

    // --- Уведомления и индикация цветов ---
    private enum class StatusColor { GREEN, YELLOW, RED }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "VPN Failover Monitor",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Статус проверки сети и автоматического переключения VPN"
                setShowBadge(false)
            }
            val manager = getSystemService(NotificationManager::class.java)
            manager?.createNotificationChannel(channel)
        }
    }

    private fun buildNotification(text: String, status: StatusColor): Notification {
        // В стандартном v2rayNG используются системные ресурсы или иконка приложения R.drawable.ic_stat_name
        val iconRes = when (status) {
            StatusColor.GREEN -> android.R.drawable.presence_online
            StatusColor.YELLOW -> android.R.drawable.presence_away
            StatusColor.RED -> android.R.drawable.presence_busy
        }

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(iconRes)
            .setContentTitle("Network Failover")
            .setContentText(text)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
    }

    private fun updateNotification(text: String, status: StatusColor) {
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        manager.notify(NOTIFICATION_ID, buildNotification(text, status))
    }
}
