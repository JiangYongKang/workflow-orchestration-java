package com.github.highcumontoa.workfloworchestrationjava.model;

/**
 * 受控任务单元的行为类型：
 * <ul>
 *   <li>{@link #SUCCEED}：始终成功；</li>
 *   <li>{@link #FAIL}：始终永久失败（不可重试，工作流立即稳定失败）；</li>
 *   <li>{@link #TRANSIENT_FAIL}：始终临时失败（可重试，直到尝试上限耗尽）；</li>
 *   <li>{@link #FLAKY}：前 failTimes 次尝试临时失败，之后成功；</li>
 *   <li>{@link #RECOVERABLE_FAIL}：首次运行（轮次 0）的尝试永久失败，恢复后的新轮次成功。
 *       用于确定性模拟“首轮失败落盘、恢复后通过”：重试无法在首轮救回，只有恢复可以。</li>
 * </ul>
 */
public enum TaskAction {
    SUCCEED,
    FAIL,
    TRANSIENT_FAIL,
    FLAKY,
    RECOVERABLE_FAIL
}
