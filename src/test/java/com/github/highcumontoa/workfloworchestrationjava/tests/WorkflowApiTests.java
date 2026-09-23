package com.github.highcumontoa.workfloworchestrationjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class WorkflowApiTests {

    private static final Logger log = LoggerFactory.getLogger(WorkflowApiTests.class);

    @Autowired
    private MockMvc mvc;
    @Autowired
    private ObjectMapper json;

    @Test
    void happyPathSubmitQueryAndRepeatedSubmit() throws Exception {
        String body = """
                {
                  "id": "api-ok",
                  "tasks": [
                    {"id": "a", "dependencies": [], "action": "SUCCEED"},
                    {"id": "b", "dependencies": ["a"], "action": "SUCCEED"}
                  ]
                }
                """;
        MvcResult first = mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk()).andReturn();
        JsonNode view = json.readTree(first.getResponse().getContentAsString());
        log.info("[trigger] POST valid workflow -> [state-basis] state={}", view.path("state").asText());
        assertEquals("SUCCEEDED", view.path("state").asText());

        // 重复提交：幂等，返回同一份记录
        MvcResult again = mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk()).andReturn();
        JsonNode second = json.readTree(again.getResponse().getContentAsString());
        log.info("[trigger] POST identical workflow again -> [state-basis] state={}, task a attempts={}",
                second.path("state").asText(),
                second.path("tasks").get(0).path("totalAttempts").asInt());
        assertEquals("SUCCEEDED", second.path("state").asText());
        assertEquals(1, second.path("tasks").get(0).path("totalAttempts").asInt(),
                "duplicate submission must not produce a second execution");

        MvcResult query = mvc.perform(get("/api/workflows/api-ok"))
                .andExpect(status().isOk()).andReturn();
        JsonNode queried = json.readTree(query.getResponse().getContentAsString());
        log.info("[trigger] GET workflow -> [state-basis] state={}", queried.path("state").asText());
        assertEquals("SUCCEEDED", queried.path("state").asText());
    }

    @Test
    void cycleAndUnknownDependencyReturnedWithDistinctReasons() throws Exception {
        String cycle = """
                {
                  "id": "api-cycle",
                  "tasks": [
                    {"id": "a", "dependencies": ["b"], "action": "SUCCEED"},
                    {"id": "b", "dependencies": ["a"], "action": "SUCCEED"}
                  ]
                }
                """;
        MvcResult cycleResult = mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON).content(cycle))
                .andExpect(status().isBadRequest()).andReturn();
        JsonNode cycleNode = json.readTree(cycleResult.getResponse().getContentAsString());
        log.info("[trigger] POST cyclic workflow -> [state-basis] http=400 reason={}",
                cycleNode.path("reason").asText());
        assertEquals("CYCLIC_DEPENDENCY", cycleNode.path("reason").asText());

        String unknown = """
                {
                  "id": "api-unknown",
                  "tasks": [
                    {"id": "a", "dependencies": ["ghost"], "action": "SUCCEED"}
                  ]
                }
                """;
        MvcResult unknownResult = mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON).content(unknown))
                .andExpect(status().isBadRequest()).andReturn();
        JsonNode unknownNode = json.readTree(unknownResult.getResponse().getContentAsString());
        log.info("[trigger] POST workflow with missing dependency -> [state-basis] http=400 reason={}",
                unknownNode.path("reason").asText());
        assertEquals("UNKNOWN_DEPENDENCY", unknownNode.path("reason").asText());

        mvc.perform(get("/api/workflows/api-cycle"))
                .andExpect(status().isNotFound());
        log.info("[trigger] GET rejected workflow id -> [state-basis] http=404 (rejected registration left no record)");
    }

    @Test
    void failedWorkflowCanBeRecoveredViaResumeEndpoint() throws Exception {
        // b 首轮永久失败、恢复后成功；c 依赖 b，验证断点恢复后整条链跑通
        String body = """
                {
                  "id": "api-resume",
                  "tasks": [
                    {"id": "a", "dependencies": [], "action": "SUCCEED"},
                    {"id": "b", "dependencies": ["a"], "action": "RECOVERABLE_FAIL"},
                    {"id": "c", "dependencies": ["b"], "action": "SUCCEED"}
                  ]
                }
                """;
        MvcResult failed = mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk()).andReturn();
        JsonNode failedView = json.readTree(failed.getResponse().getContentAsString());
        log.info("[trigger] first submit has recoverable failure -> [state-basis] state={}, reason={}",
                failedView.path("state").asText(), failedView.path("failureReason").asText());
        assertEquals("FAILED", failedView.path("state").asText());

        MvcResult resumed = mvc.perform(post("/api/workflows/api-resume/resume"))
                .andExpect(status().isOk()).andReturn();
        JsonNode resumedView = json.readTree(resumed.getResponse().getContentAsString());
        log.info("[trigger] resume same workflow -> [state-basis] state={}, a attempts={}, b attempts={}, c attempts={}",
                resumedView.path("state").asText(),
                resumedView.path("tasks").get(0).path("totalAttempts").asInt(),
                resumedView.path("tasks").get(1).path("totalAttempts").asInt(),
                resumedView.path("tasks").get(2).path("totalAttempts").asInt());
        assertEquals("SUCCEEDED", resumedView.path("state").asText());
        assertEquals(1, resumedView.path("tasks").get(0).path("totalAttempts").asInt());
        assertEquals(2, resumedView.path("tasks").get(1).path("totalAttempts").asInt());
        assertEquals(1, resumedView.path("tasks").get(2).path("totalAttempts").asInt());
    }
}
