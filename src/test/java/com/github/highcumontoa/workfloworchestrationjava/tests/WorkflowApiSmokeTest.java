package com.github.highcumontoa.workfloworchestrationjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 端到端：REST 提交/查询/恢复/幂等/校验拒绝，结论可复现。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WorkflowApiSmokeTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowApiSmokeTest.class);

    @Autowired
    private TestRestTemplate rest;

    private final ObjectMapper json = new ObjectMapper();

    private Map<String, Object> task(String name, String behavior, List<String> params,
                                    List<String> deps) {
        return Map.of("name", name,
                "dependsOn", deps == null ? List.of() : deps,
                "behavior", behavior,
                "params", params == null ? List.of() : params);
    }

    private Map<String, Object> workflow(String id, List<Map<String, Object>> tasks,
                                         Integer concurrency, Integer attempts, Long timeout) {
        java.util.Map<String, Object> m = new java.util.HashMap<>();
        m.put("workflowId", id);
        m.put("tasks", tasks);
        if (concurrency != null) m.put("maxConcurrency", concurrency);
        if (attempts != null) m.put("maxAttempts", attempts);
        if (timeout != null) m.put("taskTimeoutMillis", timeout);
        return m;
    }

    @Test
    @DisplayName("HTTP 全链路：未知依赖被拒(422/UNKNOWN_DEPENDENCY)，合法工作流成功可查，重复提交幂等，404")
    void fullLifecycleOverHttp() throws Exception {
        // 1) 未知依赖 -> 422 + 原因码
        Map<String, Object> bad = workflow("http-bad",
                List.of(task("A", "ALWAYS_SUCCEED", null, List.of("GHOST"))),
                null, null, null);
        ResponseEntity<String> rejected = rest.exchange("/api/workflows",
                HttpMethod.PUT, new HttpEntity<>(bad), String.class);
        JsonNode rej = json.readTree(rejected.getBody());
        log.info("[HTTP 测试] 拒绝提交 status={} body={}", rejected.getStatusCode(), rejected.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, rejected.getStatusCode());
        assertEquals("UNKNOWN_DEPENDENCY", rej.get("reason").asText());

        // 2) 合法工作流：A -> B，成功
        Map<String, Object> good = workflow("http-ok",
                List.of(
                        task("A", "ALWAYS_SUCCEED", null, null),
                        task("B", "ALWAYS_SUCCEED", null, List.of("A"))
                ), 2, 1, 5000L);
        ResponseEntity<String> created = rest.exchange("/api/workflows",
                HttpMethod.PUT, new HttpEntity<>(good), String.class);
        JsonNode c = json.readTree(created.getBody());
        log.info("[HTTP 测试] 合法提交 status={} state={}", created.getStatusCode(), c.get("state").asText());
        assertEquals(HttpStatus.OK, created.getStatusCode());
        assertEquals("SUCCEEDED", c.get("state").asText());
        assertFalse(c.get("duplicateSubmission").asBoolean());

        // 3) 重复提交 -> 同一份记录，duplicate=true
        ResponseEntity<String> dup = rest.exchange("/api/workflows",
                HttpMethod.PUT, new HttpEntity<>(good), String.class);
        JsonNode d = json.readTree(dup.getBody());
        log.info("[HTTP 测试] 重复提交 state={} duplicate={} runCount={}",
                d.get("state").asText(), d.get("duplicateSubmission").asBoolean(),
                d.get("runCount").asInt());
        assertTrue(d.get("duplicateSubmission").asBoolean());
        assertEquals(1, d.get("runCount").asInt());

        // 4) 查询结论一致
        ResponseEntity<String> queried = rest.getForEntity("/api/workflows/http-ok", String.class);
        JsonNode q = json.readTree(queried.getBody());
        assertEquals("SUCCEEDED", q.get("state").asText());

        // 5) 不存在 -> 404
        ResponseEntity<String> nf = rest.getForEntity("/api/workflows/nope", String.class);
        assertEquals(HttpStatus.NOT_FOUND, nf.getStatusCode());
        log.info("[HTTP 测试] 未知工作流查询 status={}", nf.getStatusCode());
    }

    @Test
    @DisplayName("HTTP 失败恢复：首轮 FAILED，resume 后 SUCCEEDED；再次 resume 幂等")
    void failureAndResumeOverHttp() throws Exception {
        Map<String, Object> wf = workflow("http-resume",
                List.of(
                        task("A", "ALWAYS_SUCCEED", null, null),
                        task("B", "FAIL_TOTAL_N", List.of("2", "0"), null),
                        task("C", "ALWAYS_SUCCEED", null, List.of("B"))
                ), 4, 2, 5000L);

        ResponseEntity<String> first = rest.exchange("/api/workflows",
                HttpMethod.PUT, new HttpEntity<>(wf), String.class);
        JsonNode f = json.readTree(first.getBody());
        log.info("[HTTP 测试] 首轮提交 state={} failureTask={}",
                f.get("state").asText(), f.get("failureTask").asText());
        assertEquals("FAILED", f.get("state").asText());
        assertEquals("B", f.get("failureTask").asText());

        // 重复提交不能触发恢复（幂等返回失败记录）
        ResponseEntity<String> dup = rest.exchange("/api/workflows",
                HttpMethod.PUT, new HttpEntity<>(wf), String.class);
        JsonNode d = json.readTree(dup.getBody());
        assertEquals("FAILED", d.get("state").asText());
        assertTrue(d.get("duplicateSubmission").asBoolean());

        // 显式恢复
        ResponseEntity<String> resumed = rest.exchange("/api/workflows/http-resume/resume",
                HttpMethod.POST, HttpEntity.EMPTY, String.class);
        JsonNode r = json.readTree(resumed.getBody());
        log.info("[HTTP 测试] resume 后 state={} runCount={}",
                r.get("state").asText(), r.get("runCount").asInt());
        assertEquals("SUCCEEDED", r.get("state").asText());
        assertEquals(2, r.get("runCount").asInt());

        // 成功后再次恢复 -> 幂等
        ResponseEntity<String> again = rest.exchange("/api/workflows/http-resume/resume",
                HttpMethod.POST, HttpEntity.EMPTY, String.class);
        JsonNode a = json.readTree(again.getBody());
        assertEquals("SUCCEEDED", a.get("state").asText());
        assertEquals(2, a.get("runCount").asInt());
        log.info("[HTTP 测试] 成功后再次 resume 幂等，runCount 仍=2");
    }
}
