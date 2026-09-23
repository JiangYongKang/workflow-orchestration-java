package com.github.highcumontoa.workfloworchestrationjava.model;

import java.util.List;

/**
 * 任务声明：id、依赖的任务 id 列表、受控行为。
 *
 * @param failTimes   FLAKY 行为下前多少次尝试以临时错误失败；其余行为忽略
 * @param sleepMillis 单次执行的模拟耗时（可被中断，用于超时控制）
 */
public record TaskSpec(String id,
                       List<String> dependencies,
                       TaskAction action,
                       Integer failTimes,
                       Long sleepMillis) {
}
