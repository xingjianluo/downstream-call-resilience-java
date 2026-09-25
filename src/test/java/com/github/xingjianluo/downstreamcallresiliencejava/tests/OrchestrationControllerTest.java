package com.github.xingjianluo.downstreamcallresiliencejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 对外契约：POST /api/orchestrate 即使部分下游降级也返回 200，
 * 响应体稳定包含 success / parts / degradedDownstreams / summary。
 */
@SpringBootTest
@AutoConfigureMockMvc
class OrchestrationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void returns200WithPartialResultStructure() throws Exception {
        String body = """
                {
                  "requestId": "http-1",
                  "deadlineMs": 600,
                  "calls": [
                    {"downstream": "inventory", "payload": "sku-1"},
                    {"downstream": "pricing", "payload": "sku-1"}
                  ]
                }
                """;

        mockMvc.perform(post("/api/orchestrate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.parts.length()").value(2))
                .andExpect(jsonPath("$.parts[0].downstream").value("inventory"))
                .andExpect(jsonPath("$.parts[0].status").value("OK"))
                .andExpect(jsonPath("$.parts[1].downstream").value("pricing"))
                .andExpect(jsonPath("$.parts[1].status").value("DEGRADED"))
                .andExpect(jsonPath("$.parts[1].reason").value("TIMEOUT"))
                .andExpect(jsonPath("$.parts[1].reasonMessage").isNotEmpty())
                .andExpect(jsonPath("$.degradedDownstreams[0]").value("pricing"))
                .andExpect(jsonPath("$.elapsedMs").isNumber())
                .andExpect(jsonPath("$.deadlineMs").value(600));
    }

    @Test
    void rejectsEmptyCallsWith400() throws Exception {
        mockMvc.perform(post("/api/orchestrate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"calls\":[]}"))
                .andExpect(status().isBadRequest());
    }
}
