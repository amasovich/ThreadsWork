package threadswork;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Управляет рабочими потоками и их очередями задач.
 * Каждому потоку соответствует собственная очередь.
 */
public class ThreadQueueMaster {

    private static final Logger logger = LoggerFactory.getLogger(ThreadQueueMaster.class);

    private final int threadCount;
    private final List<Worker> workers;
    private final List<BlockingQueue<Runnable>> taskQueues;

    /**
     * Конструктор пула с заданным количеством потоков.
     *
     * @param threadCount количество рабочих потоков
     */
    public ThreadQueueMaster(int threadCount) {
        this.threadCount = threadCount;
        this.taskQueues = new ArrayList<>(threadCount);
        this.workers = new ArrayList<>(threadCount);

        logger.info("Инициализация пула потоков с {} очередями", threadCount);

        for (int i = 0; i < threadCount; i++) {
            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
            Worker worker = new Worker(queue, i);
            taskQueues.add(queue);
            workers.add(worker);
            new Thread(worker, "Worker-" + i).start();
        }
    }

    /**
     * Отправляет задачу в указанную очередь.
     *
     * @param task задача для выполнения
     * @param queueIndex индекс очереди (0 ... threadCount - 1)
     */
    public void submit(Runnable task, int queueIndex) {
        if (queueIndex >= 0 && queueIndex < threadCount) {
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
     * Получить количество рабочих потоков.
     */
    public int getThreadCount() {
        return threadCount;
    }
}

