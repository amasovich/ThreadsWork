package threadswork;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Интерфейс управления пользовательским пулом потоков.
 */
public interface CustomExecutor extends Executor {
    @Override
    void execute(Runnable command);

    <T> Future<T> submit(Callable<T> callable);

    void shutdown();

    void shutdownNow();
}

