package com.dtp.core.spring;

import com.dtp.common.ApplicationContextHolder;
import com.dtp.common.properties.DtpProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.*;

/**
 * DtpLifecycleSupport which mainly implements Spring bean's lifecycle methods,
 * mimics spring internal thread pool {@link ThreadPoolTaskExecutor}.
 *
 * @author yanhom
 * @since 1.0.3
 **/
// 拉取配置中心的配置后注册
// InitializingBean: 获取配置后初始化bean
// DisposableBean: 当bean销毁时调用destroy()

// reference: https://blog.csdn.net/zstu_cc/article/details/53981606
// 工厂方法模式：保证了初始化的方式的多样性：使用abstract让子类可以有多种初始化实现；严格规定了子类前后的其他动作：abstract实现了底层规范
// 依赖倒置：把依赖子类具体实现，修改为 -> 子类依赖抽象类提供的抽象方法 -> 故抽象类、子类，共同依赖中间抽象类提供的abstract方法

// design：
// 第一层规范 -> 第二层规范 -> 第一个抽象类实现定义某个父类（子类只是具体实现"初始化"的功能，其他关于第一、二层规范的实现都是由抽象类规定的） -> 子类具体实现
// 第一个抽象类实现也可以把上面两层规范，交由子类具体去实现（eg：abstract都不实现上面两层规范）
// extends ThreadPoolExecutor是为了让子类都拥有ThreadPoolExecutor的功能
@Slf4j
public abstract class DtpLifecycleSupport extends ThreadPoolExecutor implements InitializingBean, DisposableBean {

    /**
     * Uniquely identifies.
     */
    protected String threadPoolName;

    /**
     * Whether to wait for scheduled tasks to complete on shutdown,
     * not interrupting running tasks and executing all tasks in the queue.
     */
    protected boolean waitForTasksToCompleteOnShutdown = false;

    /**
     * The maximum number of seconds that this executor is supposed to block
     * on shutdown in order to wait for remaining tasks to complete their execution
     * before the rest of the container continues to shut down.
     */
    protected int awaitTerminationSeconds = 0;

    public DtpLifecycleSupport(int corePoolSize,
                               int maximumPoolSize,
                               long keepAliveTime,
                               TimeUnit unit,
                               BlockingQueue<Runnable> workQueue,
                               ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public void setWaitForTasksToCompleteOnShutdown(boolean waitForTasksToCompleteOnShutdown) {
        this.waitForTasksToCompleteOnShutdown = waitForTasksToCompleteOnShutdown;
    }

    public void setAwaitTerminationSeconds(int awaitTerminationSeconds) {
        this.awaitTerminationSeconds = awaitTerminationSeconds;
    }

    public void setThreadPoolName(String threadPoolName) {
        this.threadPoolName = threadPoolName;
    }

    public String getThreadPoolName() {
        return threadPoolName;
    }

    @Override
    public void afterPropertiesSet() {
        DtpProperties dtpProperties = ApplicationContextHolder.getBean(DtpProperties.class);
        initialize(dtpProperties);
    }

    /**
     * Calls {@code internalShutdown} when the BeanFactory destroys
     * the task executor instance.
     * @see #internalShutdown()
     */
    @Override
    public void destroy() {
        internalShutdown();
    }

    /**
     * Initialize, do sth.
     *
     * @param dtpProperties dtpProperties
     */
    protected abstract void initialize(DtpProperties dtpProperties);

    /**
     * Perform a shutdown on the underlying ExecutorService.
     * @see java.util.concurrent.ExecutorService#shutdown()
     * @see java.util.concurrent.ExecutorService#shutdownNow()
     */
    public void internalShutdown() {
        if (log.isInfoEnabled()) {
            log.info("Shutting down ExecutorService, poolName: {}", threadPoolName);
        }
        if (this.waitForTasksToCompleteOnShutdown) {
            this.shutdown();
        } else {
            for (Runnable remainingTask : this.shutdownNow()) {
                cancelRemainingTask(remainingTask);
            }
        }
        awaitTerminationIfNecessary();
    }

    /**
     * Cancel the given remaining task which never commended execution,
     * as returned from {@link ExecutorService#shutdownNow()}.
     * @param task the task to cancel (typically a {@link RunnableFuture})
     * @since 5.0.5
     * @see #shutdown()
     * @see RunnableFuture#cancel(boolean)
     */
    protected void cancelRemainingTask(Runnable task) {
        if (task instanceof Future) {
            ((Future<?>) task).cancel(true);
        }
    }

    /**
     * Wait for the executor to terminate, according to the value of the
     * {@link #setAwaitTerminationSeconds "awaitTerminationSeconds"} property.
     */
    private void awaitTerminationIfNecessary() {
        if (this.awaitTerminationSeconds <= 0) {
            return;
        }
        try {
            if (!awaitTermination(this.awaitTerminationSeconds, TimeUnit.SECONDS) && log.isWarnEnabled()) {
                log.warn("Timed out while waiting for executor {} to terminate", threadPoolName);
            }
        } catch (InterruptedException ex) {
            if (log.isWarnEnabled()) {
                log.warn("Interrupted while waiting for executor {} to terminate", threadPoolName);
            }
            Thread.currentThread().interrupt();
        }
    }
}
