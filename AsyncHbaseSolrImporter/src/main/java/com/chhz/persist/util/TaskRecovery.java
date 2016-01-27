package com.chhz.persist.util;

import java.io.Serializable;
import java.lang.Thread.UncaughtExceptionHandler;
import org.slf4j.LoggerFactory;

/**
 * 线程恢复
 *
 */
public class TaskRecovery implements UncaughtExceptionHandler, Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 393049273593903850L;
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TaskRecovery.class);
    private final Runnable task;

    public TaskRecovery(Runnable task) {
        this.task = task;
    }

    @Override
    public void uncaughtException(Thread thread, Throwable thrwbl) {
        logger.error(thread.getName(), thrwbl);
        Thread newThread = new Thread(task, thread.getName());
        newThread.setUncaughtExceptionHandler(this);
        newThread.start();
    }

}
