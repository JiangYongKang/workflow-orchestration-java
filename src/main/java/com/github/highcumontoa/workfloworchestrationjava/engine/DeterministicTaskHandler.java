package com.github.highcumontoa.workfloworchestrationjava.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskContext;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 受控的模拟任务执行单元，结论完全确定、可复现。
 *
 * <p>行为类型（由 TaskDefinition.behavior 指定）：
 * <ul>
 *   <li>{@link #ALWAYS_SUCCEED}：恒成功，params[0] 可选，模拟耗时毫秒</li>
 *   <li>{@link #ALWAYS_FAIL}：恒以临时错误失败（用于触发重试耗尽）</li>
 *   <li>{@link #FAIL_FIRST_N}：前 N 次尝试失败，第 N+1 次起成功；
 *       params[0]=N，params[1] 可选，模拟耗时毫秒</li>
 *   <li>{@link #TIMEOUT}：阻塞超过任务超时上限，由执行器判定为超时失败</li>
 *   <li>{@link #FAIL_TOTAL_N}：整个生命周期内（跨恢复轮次）总共失败 N 次后成功，
 *       params[0]=N。用于确定性地演示“首轮失败、恢复后成功”：把 N 设为恰好在首轮
 *       重试预算内耗尽但最后一次仍失败，恢复轮第一次尝试即成功。</li>
 * </ul>
 */
@Component
public class DeterministicTaskHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(DeterministicTaskHandler.class);

    public static final String ALWAYS_SUCCEED = "ALWAYS_SUCCEED";
    public static final String ALWAYS_FAIL = "ALWAYS_FAIL";
    public static final String FAIL_FIRST_N = "FAIL_FIRST_N";
    public static final String TIMEOUT = "TIMEOUT";
    public static final String FAIL_TOTAL_N = "FAIL_TOTAL_N";

    /** 跨恢复轮次共享的剩余失败次数，key = workflowId:taskName。 */
    private final java.util.concurrent.ConcurrentHashMap<String, Integer> remainingFailures =
            new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public TaskOutcome execute(TaskContext context) throws InterruptedException {
        String behavior = resolveBehavior(context);
        long sleepMillis = parseSleep(context);
        String name = context.getTaskName();
        int attempt = context.getAttempt();

        switch (behavior) {
            case ALWAYS_FAIL -> {
                sleep(sleepMillis);
                log.info("[{}] 任务 {} 第{}次尝试执行，触发原因=行为{}注入临时错误",
                        context.getWorkflowId(), name, attempt, ALWAYS_FAIL);
                return TaskOutcome.failure("injected transient error by "
                        + ALWAYS_FAIL);
            }
            case FAIL_FIRST_N -> {
                int failCount = parseIntParam(context, 0, 1);
                sleep(sleepMillis);
                if (attempt <= failCount) {
                    log.info("[{}] 任务 {} 第{}/{}次尝试执行，触发原因=行为{}注入临时错误",
                            context.getWorkflowId(), name, attempt, failCount, FAIL_FIRST_N);
                    return TaskOutcome.failure("injected transient error by "
                            + FAIL_FIRST_N + " (attempt " + attempt + " <= " + failCount + ")");
                }
                log.info("[{}] 任务 {} 第{}次尝试执行，触发原因=已越过失败窗口{}，本次成功",
                        context.getWorkflowId(), name, attempt, failCount);
                return TaskOutcome.success("recovered after " + failCount
                        + " transient failure(s)");
            }
            case TIMEOUT -> {
                // 故意长睡：执行器的看门狗会先到时，将本次尝试判为超时
                log.info("[{}] 任务 {} 第{}次尝试执行，触发原因=行为{}模拟超时",
                        context.getWorkflowId(), name, attempt, TIMEOUT);
                Thread.sleep(Math.max(sleepMillis, 60_000L));
                return TaskOutcome.success("unexpected return from TIMEOUT behavior");
            }
            case FAIL_TOTAL_N -> {
                int total = parseIntParam(context, 0, 0);
                String key = context.getWorkflowId() + ":" + name;
                // 首次见到该任务时用 params 初始化预算，之后跨轮共享同一份
                Integer left = remainingFailures.computeIfAbsent(key, k -> total);
                sleep(sleepMillis);
                if (left > 0) {
                    remainingFailures.put(key, left - 1);
                    log.info("[{}] 任务 {} 第{}次尝试执行，触发原因={}跨轮失败预算剩余{}，本次失败",
                            context.getWorkflowId(), name, attempt, FAIL_TOTAL_N, left);
                    return TaskOutcome.failure("transient error by " + FAIL_TOTAL_N
                            + ", remaining budget=" + left);
                }
                log.info("[{}] 任务 {} 第{}次尝试执行，触发原因={}跨轮失败预算已耗尽，本次成功",
                        context.getWorkflowId(), name, attempt, FAIL_TOTAL_N);
                return TaskOutcome.success("succeeded after exhausting total failure budget");
            }
            case ALWAYS_SUCCEED -> {
                sleep(sleepMillis);
                log.info("[{}] 任务 {} 第{}次尝试执行，触发原因=正常执行，结果=成功",
                        context.getWorkflowId(), name, attempt);
                return TaskOutcome.success("ok");
            }
            default -> {
                log.warn("[{}] 任务 {} 使用未知行为类型 {}，按失败处理",
                        context.getWorkflowId(), name, behavior);
                return TaskOutcome.failure("unknown behavior: " + behavior);
            }
        }
    }

    /** 无行为声明时按恒成功处理。 */
    private String resolveBehavior(TaskContext context) {
        String b = context.getBehavior();
        return (b == null || b.isBlank()) ? ALWAYS_SUCCEED : b;
    }

    private long parseSleep(TaskContext context) {
        try {
            if (TIMEOUT.equals(resolveBehavior(context))
                    && (context.getParams() == null || context.getParams().isEmpty())) {
                return 0L;
            }
            int idx = (FAIL_FIRST_N.equals(resolveBehavior(context))
                    || FAIL_TOTAL_N.equals(resolveBehavior(context))) ? 1 : 0;
            if (context.getParams() != null && context.getParams().size() > idx) {
                return Long.parseLong(context.getParams().get(idx));
            }
        } catch (NumberFormatException ignored) {
            // 非法参数按 0 处理，保持确定性
        }
        return 0L;
    }

    private int parseIntParam(TaskContext context, int index, int defaultValue) {
        try {
            if (context.getParams() != null && context.getParams().size() > index) {
                return Integer.parseInt(context.getParams().get(index));
            }
        } catch (NumberFormatException ignored) {
            // 回落默认值
        }
        return defaultValue;
    }

    private void sleep(long millis) throws InterruptedException {
        if (millis > 0) {
            Thread.sleep(millis);
        }
    }
}
