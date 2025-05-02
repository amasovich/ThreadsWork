package threadswork;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Кастомный пул потоков с несколькими очередями задач.
 * Позволяет задавать параметры: размер пула, лимит очереди, время ожидания простоя и т.д.
 */
public class MultiQueueExecutor {

    private static final Logger logger = LoggerFactory.getLogger(MultiQueueExecutor.class);

    private final int corePoolSize;
    private final int maxPoolSize;
    private final int queueSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int minSpareThreads;

    private final List<QueueWorker> workers;
    private final List<BlockingQueue<Runnable>> taskQueues;

    /**
     * Конструктор с параметрами настройки пула.
     *
     * @param corePoolSize     минимальное количество потоков
     * @param maxPoolSize      максимальное количество потоков
     * @param queueSize        максимальный размер очереди задач
     * @param keepAliveTime    время простоя потока до завершения
     * @param timeUnit         единицы измерения времени
     * @param minSpareThreads  минимальное количество свободных потоков
     */
    public MultiQueueExecutor(int corePoolSize, int maxPoolSize, int queueSize, long keepAliveTime, TimeUnit timeUnit, int minSpareThreads) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.queueSize = queueSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.minSpareThreads = minSpareThreads;

        this.taskQueues = new ArrayList<>();
        this.workers = new ArrayList<>();

        logger.info("Инициализация пула потоков: core={}, max={}, queueSize={}, keepAlive={} {}",
                corePoolSize, maxPoolSize, queueSize, keepAliveTime, timeUnit.name());

        for (int i = 0; i < corePoolSize; i++) {
            createWorker(i);
        }
    }

    /**
     * Создаёт и запускает новый рабочий поток с собственной очередью.
     *
     * @param index номер потока
     */
    private void createWorker(int index) {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
        QueueWorker worker = new QueueWorker(queue, index, keepAliveTime, timeUnit);
        taskQueues.add(queue);
        workers.add(worker);
        new Thread(worker, "Worker-" + index).start();
    }

    /**
     * Отправляет задачу в указанную очередь.
     *
     * @param task        задача
     * @param queueIndex  индекс очереди (0 ... corePoolSize-1)
     */
    public void submit(Runnable task, int queueIndex) {
        if (queueIndex >= 0 && queueIndex < taskQueues.size()) {
            try {
                taskQueues.get(queueIndex).put(task);
                logger.debug("Задача добавлена в очередь {}", queueIndex);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Ошибка при добавлении задачи в очередь {}: {}", queueIndex, e.getMessage());
            }
        } else {
            logger.warn("Недопустимый индекс очереди: {}", queueIndex);
        }
    }

    /**
     * Возвращает общее число активных потоков.
     */
    public int getWorkerCount() {
        return workers.size();
    }

    // TODO: в следующих версиях реализовать:
    // - Мониторинг активности потоков
    // - Добавление потоков при нехватке (если свободных < minSpareThreads)
    // - Завершение неактивных потоков после keepAliveTime

}

