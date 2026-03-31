<template>
  <div class="data-analysis">
    <a-card title="数据分析工作台">
      <template #extra>
        <a-space>
          <a-button @click="refreshActiveTab" :loading="reportsLoading || schedulesLoading">
            刷新
          </a-button>
          <a-tag color="blue">Phase 2-4</a-tag>
        </a-space>
      </template>

      <a-tabs v-model:activeKey="activeTab">
        <a-tab-pane key="reports" tab="分析报告">
          <a-row :gutter="16" class="toolbar-row">
            <a-col :xs="24" :md="8">
              <a-select
                v-model:value="reportFilters.table"
                allow-clear
                show-search
                placeholder="按表过滤"
                :options="tableOptions"
              />
            </a-col>
            <a-col :xs="12" :md="4">
              <a-select
                v-model:value="reportFilters.depth"
                allow-clear
                placeholder="深度"
                :options="depthOptions"
              />
            </a-col>
            <a-col :xs="12" :md="4">
              <a-select
                v-model:value="reportFilters.status"
                allow-clear
                placeholder="状态"
                :options="statusOptions"
              />
            </a-col>
            <a-col :xs="24" :md="8" style="text-align: right">
              <a-button @click="loadReports" :loading="reportsLoading">刷新报告</a-button>
            </a-col>
          </a-row>

          <a-empty v-if="!reportsLoading && filteredReports.length === 0" description="暂无分析报告" />

          <a-list v-else :data-source="filteredReports" :loading="reportsLoading" item-layout="vertical">
            <template #renderItem="{ item }">
              <a-list-item>
                <a-card size="small" class="report-card">
                  <template #title>
                    <a-space wrap>
                      <span>{{ item.table_names }}</span>
                      <a-tag :color="statusColor(item.status)">{{ item.status || 'unknown' }}</a-tag>
                      <a-tag>{{ item.depth || '-' }}</a-tag>
                    </a-space>
                  </template>
                  <template #extra>
                    <a-space>
                      <a-button size="small" @click="openReport(item.id)">查看详情</a-button>
                      <a-popconfirm
                        title="确认删除这份分析报告？"
                        ok-text="删除"
                        cancel-text="取消"
                        @confirm="deleteReport(item.id)"
                      >
                        <a-button size="small" danger>删除</a-button>
                      </a-popconfirm>
                    </a-space>
                  </template>

                  <div class="report-summary">{{ readableSummary(item) }}</div>
                  <a-space wrap class="report-metrics">
                    <a-tag color="processing">洞察 {{ item.insight_count || 0 }}</a-tag>
                    <a-tag color="warning">异常 {{ item.anomaly_count || 0 }}</a-tag>
                    <a-tag color="default">失败步骤 {{ item.failed_step_count || 0 }}</a-tag>
                    <span class="report-meta">{{ formatDuration(item.duration_ms) }}</span>
                    <span class="report-meta">{{ item.created_at || '-' }}</span>
                  </a-space>
                </a-card>
              </a-list-item>
            </template>
          </a-list>
        </a-tab-pane>

        <a-tab-pane key="ondemand" tab="即时分析">
          <a-form :model="analysisForm" layout="vertical">
            <a-row :gutter="16">
              <a-col :xs="24" :lg="8">
                <a-form-item label="目标表" required>
                  <a-select
                    v-model:value="analysisForm.tableName"
                    show-search
                    placeholder="选择已同步表"
                    :options="tableOptions"
                  />
                </a-form-item>
              </a-col>
              <a-col :xs="24" :lg="8">
                <a-form-item label="分析深度">
                  <a-radio-group v-model:value="analysisForm.depth">
                    <a-radio-button value="quick">Quick</a-radio-button>
                    <a-radio-button value="standard">Standard</a-radio-button>
                    <a-radio-button value="deep">Deep</a-radio-button>
                    <a-radio-button value="expert">Expert</a-radio-button>
                  </a-radio-group>
                </a-form-item>
              </a-col>
              <a-col :xs="24" :lg="8">
                <a-form-item label="AI 资源">
                  <a-select
                    v-model:value="analysisForm.resourceName"
                    allow-clear
                    placeholder="可选"
                    :options="resourceOptions"
                  />
                </a-form-item>
              </a-col>
            </a-row>

            <a-form-item>
              <a-button type="primary" :loading="analysisRunning" @click="runAnalysis">
                运行分析
              </a-button>
            </a-form-item>
          </a-form>

          <a-alert
            v-if="analysisForm.depth === 'expert'"
            class="expert-mode-note"
            type="info"
            show-icon
            message="Expert 模式通常需要 2-5 分钟，并会生成 conversation 与 reasoning 细节。"
          />

          <a-empty v-if="!analysisResult" description="选择表后运行分析，结果会显示在这里" />

          <a-card v-else size="small" class="analysis-result">
            <template #title>
              <a-space>
                <span>{{ analysisResult.table_names }}</span>
                <a-tag :color="statusColor(analysisResult.status)">{{ analysisResult.status || 'completed' }}</a-tag>
              </a-space>
            </template>
            <a-space
              v-if="analysisResult.depth === 'expert'"
              wrap
              class="report-metrics"
              style="margin-bottom: 12px"
            >
              <a-tag :color="confidenceColor(analysisResult.confidence_ratings?.overall)">
                {{ formatConfidence(analysisResult.confidence_ratings?.overall) }}
              </a-tag>
              <a-tag color="blue">Expert</a-tag>
            </a-space>
            <template v-if="isExpertReport(analysisResult)">
              <a-divider orientation="left">经营摘要</a-divider>
              <div class="business-summary-card">{{ expertExecutiveSummary(analysisResult) }}</div>

              <a-divider orientation="left">关键洞察</a-divider>
              <a-empty v-if="!expertTopInsights(analysisResult).length" description="暂无关键洞察" />
              <a-list v-else :data-source="expertTopInsights(analysisResult)" size="small">
                <template #renderItem="{ item }">
                  <a-list-item>
                    <a-space direction="vertical" size="small">
                      <strong>{{ displayInsightTitle(item) }}</strong>
                      <span>{{ displayInsightDetail(item) }}</span>
                    </a-space>
                  </a-list-item>
                </template>
              </a-list>

              <a-divider orientation="left">动作建议</a-divider>
              <a-empty v-if="!expertActionItems(analysisResult).length" description="暂无动作建议" />
              <a-list v-else :data-source="expertActionItems(analysisResult)" size="small">
                <template #renderItem="{ item }">
                  <a-list-item>
                    <a-space direction="vertical" size="small">
                      <strong>{{ displayInsightTitle(item) }}</strong>
                      <span>{{ displayInsightDetail(item) }}</span>
                    </a-space>
                  </a-list-item>
                </template>
              </a-list>

              <a-collapse ghost class="expert-detail-collapse">
                <a-collapse-panel key="analysis-result-detail" header="详细分析">
                  <a-divider orientation="left">根因分析</a-divider>
                  <a-empty v-if="!(analysisResult.root_causes || []).length" description="暂无根因分析" />
                  <a-list v-else :data-source="analysisResult.root_causes" size="small" bordered />

                  <a-divider orientation="left">证据链</a-divider>
                  <a-empty v-if="!(analysisResult.evidence_chains || []).length" description="暂无证据链" />
                  <a-list v-else :data-source="analysisResult.evidence_chains" size="small">
                    <template #renderItem="{ item }">
                      <a-list-item>
                        <div class="evidence-card">
                          <strong>{{ displayFindingTitle(item.finding) }}</strong>
                          <div class="inline-metric" v-if="displayFindingDetail(item)">{{ displayFindingDetail(item) }}</div>
                          <div class="inline-metric">Hypotheses: {{ (item.hypotheses || []).length }}</div>
                          <div class="inline-metric">Assessments: {{ (item.assessments || []).length }}</div>
                          <div class="inline-metric">Follow-ups: {{ (item.follow_ups || []).length }}</div>
                        </div>
                      </a-list-item>
                    </template>
                  </a-list>

                  <a-divider orientation="left">分析过程</a-divider>
                  <a-empty v-if="!(analysisResult.conversation_chain || []).length" description="暂无分析过程" />
                  <a-collapse v-else>
                    <a-collapse-panel
                      v-for="(round, index) in analysisResult.conversation_chain || []"
                      :key="round.round || index"
                      :header="`Round ${round.round || index + 1}`"
                    >
                      <pre class="sql-preview">{{ formatJson(round.strategist_output || {}) }}</pre>
                      <div class="round-result-list" v-if="(round.results || []).length">
                        <div v-for="(result, resultIndex) in round.results || []" :key="result.title || resultIndex" class="evidence-card">
                          <strong>{{ result.title || `Query ${resultIndex + 1}` }}</strong>
                          <div class="inline-metric">Rows: {{ result.row_count ?? 0 }}</div>
                          <div class="inline-metric">Status: {{ result.success ? 'success' : 'failed' }}</div>
                          <div class="inline-metric" v-if="result.error_message">Error: {{ result.error_message }}</div>
                        </div>
                      </div>
                    </a-collapse-panel>
                  </a-collapse>

                  <a-divider orientation="left">推理轨迹</a-divider>
                  <a-empty v-if="!(analysisResult.reasoning_traces || []).length" description="暂无推理轨迹" />
                  <a-collapse v-else ghost>
                    <a-collapse-panel
                      v-for="(trace, index) in analysisResult.reasoning_traces || []"
                      :key="trace.round || index"
                      :header="`Round ${trace.round || index + 1}`"
                    >
                      <pre class="reasoning-preview">{{ trace.trace || '-' }}</pre>
                    </a-collapse-panel>
                  </a-collapse>

                  <a-divider orientation="left">限制说明</a-divider>
                  <a-empty v-if="!(analysisResult.limitations || []).length" description="暂无限制说明" />
                  <a-list v-else :data-source="analysisResult.limitations" size="small" bordered />
                </a-collapse-panel>
              </a-collapse>
            </template>

            <template v-else>
              <p class="report-summary">{{ readableSummary(analysisResult) }}</p>

              <a-divider orientation="left">洞察</a-divider>
              <a-empty v-if="!(analysisResult.insights || []).length" description="暂无洞察" />
              <a-list v-else :data-source="analysisResult.insights" size="small">
                  <template #renderItem="{ item }">
                    <a-list-item>
                      <a-space direction="vertical" size="small">
                        <strong>{{ displayInsightTitle(item) }}</strong>
                        <span>{{ displayInsightDetail(item) }}</span>
                      </a-space>
                    </a-list-item>
                  </template>
                </a-list>

              <a-divider orientation="left">建议</a-divider>
              <a-empty v-if="!(analysisResult.recommendations || []).length" description="暂无建议" />
              <a-list v-else :data-source="analysisResult.recommendations" size="small" bordered />
            </template>
          </a-card>
        </a-tab-pane>

        <a-tab-pane key="schedules" tab="计划任务">
          <a-row :gutter="16" class="toolbar-row">
            <a-col :xs="24" :md="12">
              <a-alert
                type="info"
                show-icon
                message="计划任务支持多表 fan-out：每次执行会对所选每张表分别生成报告。"
              />
            </a-col>
            <a-col :xs="24" :md="12" style="text-align: right">
              <a-button type="primary" @click="openScheduleModal()">新建计划</a-button>
            </a-col>
          </a-row>

          <a-table
            :data-source="schedules"
            :columns="scheduleColumns"
            :loading="schedulesLoading"
            :row-key="(record: AnalysisSchedule) => record.id"
            size="small"
            :pagination="{ pageSize: 8 }"
          >
            <template #bodyCell="{ column, record }">
              <template v-if="column.key === 'tables'">
                <a-space wrap>
                  <a-tag v-for="tableName in record.tables" :key="tableName">{{ tableName }}</a-tag>
                </a-space>
              </template>
              <template v-else-if="column.key === 'schedule'">
                <span>{{ describeSchedule(record) }}</span>
              </template>
              <template v-else-if="column.key === 'enabled'">
                <a-switch :checked="record.enabled" @change="toggleSchedule(record.id)" />
              </template>
              <template v-else-if="column.key === 'actions'">
                <a-space>
                  <a-button size="small" @click="runSchedule(record.id)">立即执行</a-button>
                  <a-button size="small" @click="openScheduleModal(record)">编辑</a-button>
                  <a-popconfirm
                    title="确认删除该计划？"
                    ok-text="删除"
                    cancel-text="取消"
                    @confirm="removeSchedule(record.id)"
                  >
                    <a-button size="small" danger>删除</a-button>
                  </a-popconfirm>
                </a-space>
              </template>
            </template>
          </a-table>
        </a-tab-pane>
      </a-tabs>
    </a-card>

    <a-drawer
      v-model:open="reportDrawerOpen"
      title="分析报告详情"
      width="720"
      destroy-on-close
    >
      <a-skeleton v-if="reportDetailLoading" active />
      <template v-else-if="selectedReport">
        <a-space wrap style="margin-bottom: 12px">
          <a-tag>{{ selectedReport.table_names }}</a-tag>
          <a-tag :color="statusColor(selectedReport.status)">{{ selectedReport.status || '-' }}</a-tag>
          <a-tag>{{ selectedReport.depth || '-' }}</a-tag>
          <span>{{ selectedReport.created_at || '-' }}</span>
        </a-space>
        <a-space
          v-if="selectedReport.depth === 'expert'"
          wrap
          class="report-metrics"
          style="margin-bottom: 12px"
        >
          <a-tag :color="confidenceColor(selectedReport.confidence_ratings?.overall)">
            {{ formatConfidence(selectedReport.confidence_ratings?.overall) }}
          </a-tag>
          <a-tag color="blue">Expert</a-tag>
        </a-space>
        <template v-if="isExpertReport(selectedReport)">
          <a-divider orientation="left">经营摘要</a-divider>
          <div class="business-summary-card">{{ expertExecutiveSummary(selectedReport) }}</div>

          <a-divider orientation="left">关键洞察</a-divider>
          <a-empty v-if="!expertTopInsights(selectedReport).length" description="暂无关键洞察" />
          <a-list v-else :data-source="expertTopInsights(selectedReport)" size="small" bordered>
            <template #renderItem="{ item }">
              <a-list-item>
                <a-space direction="vertical" size="small">
                  <strong>{{ displayInsightTitle(item) }}</strong>
                  <span>{{ displayInsightDetail(item) }}</span>
                </a-space>
              </a-list-item>
            </template>
          </a-list>

          <a-divider orientation="left">动作建议</a-divider>
          <a-empty v-if="!expertActionItems(selectedReport).length" description="暂无动作建议" />
          <a-list v-else :data-source="expertActionItems(selectedReport)" size="small" bordered>
            <template #renderItem="{ item }">
              <a-list-item>
                <a-space direction="vertical" size="small">
                  <strong>{{ displayInsightTitle(item) }}</strong>
                  <span>{{ displayInsightDetail(item) }}</span>
                </a-space>
              </a-list-item>
            </template>
          </a-list>

          <a-collapse ghost class="expert-detail-collapse">
            <a-collapse-panel key="report-detail-analysis" header="详细分析">
              <a-divider orientation="left">根因分析</a-divider>
              <a-empty v-if="!(selectedReport.root_causes || []).length" description="暂无根因分析" />
              <a-list v-else :data-source="selectedReport.root_causes" size="small" bordered />

              <a-divider orientation="left">证据链</a-divider>
              <a-empty v-if="!(selectedReport.evidence_chains || []).length" description="暂无证据链" />
              <a-list v-else :data-source="selectedReport.evidence_chains" size="small">
                <template #renderItem="{ item }">
                  <a-list-item>
                    <div class="evidence-card">
                      <strong>{{ displayFindingTitle(item.finding) }}</strong>
                      <div class="inline-metric" v-if="displayFindingDetail(item)">{{ displayFindingDetail(item) }}</div>
                      <div class="inline-metric">Hypotheses: {{ (item.hypotheses || []).length }}</div>
                      <div class="inline-metric">Assessments: {{ (item.assessments || []).length }}</div>
                      <div class="inline-metric">Follow-ups: {{ (item.follow_ups || []).length }}</div>
                    </div>
                  </a-list-item>
                </template>
              </a-list>

              <a-divider orientation="left">分析过程</a-divider>
              <a-empty v-if="!(selectedReport.conversation_chain || []).length" description="暂无分析过程" />
              <a-collapse v-else>
                <a-collapse-panel
                  v-for="(round, index) in selectedReport.conversation_chain || []"
                  :key="round.round || index"
                  :header="`Round ${round.round || index + 1}`"
                >
                  <pre class="sql-preview">{{ formatJson(round.strategist_output || {}) }}</pre>
                  <div class="round-result-list" v-if="(round.results || []).length">
                    <div v-for="(result, resultIndex) in round.results || []" :key="result.title || resultIndex" class="evidence-card">
                      <strong>{{ result.title || `Query ${resultIndex + 1}` }}</strong>
                      <div class="inline-metric">Rows: {{ result.row_count ?? 0 }}</div>
                      <div class="inline-metric">Status: {{ result.success ? 'success' : 'failed' }}</div>
                      <div class="inline-metric" v-if="result.error_message">Error: {{ result.error_message }}</div>
                    </div>
                  </div>
                </a-collapse-panel>
              </a-collapse>

              <a-divider orientation="left">推理轨迹</a-divider>
              <a-empty v-if="!(selectedReport.reasoning_traces || []).length" description="暂无推理轨迹" />
              <a-collapse v-else ghost>
                <a-collapse-panel
                  v-for="(trace, index) in selectedReport.reasoning_traces || []"
                  :key="trace.round || index"
                  :header="`Round ${trace.round || index + 1}`"
                >
                  <pre class="reasoning-preview">{{ trace.trace || '-' }}</pre>
                </a-collapse-panel>
              </a-collapse>

              <a-divider orientation="left">限制说明</a-divider>
              <a-empty v-if="!(selectedReport.limitations || []).length" description="暂无限制说明" />
              <a-list v-else :data-source="selectedReport.limitations" size="small" bordered />

              <a-divider orientation="left">执行步骤</a-divider>
              <a-collapse>
                <a-collapse-panel
                  v-for="(step, index) in selectedReport.steps || []"
                  :key="step.title || index"
                  :header="step.title || `Step ${index + 1}`"
                >
                  <p><strong>问题：</strong> {{ step.question || '-' }}</p>
                  <pre class="sql-preview">{{ step.sql || '-' }}</pre>
                </a-collapse-panel>
              </a-collapse>
            </a-collapse-panel>
          </a-collapse>
        </template>

        <template v-else>
          <p class="report-summary">{{ readableSummary(selectedReport) }}</p>

          <a-divider orientation="left">洞察</a-divider>
          <a-list :data-source="selectedReport.insights || []" size="small" bordered>
            <template #renderItem="{ item }">
              <a-list-item>
                <a-space direction="vertical" size="small">
                  <strong>{{ displayInsightTitle(item) }}</strong>
                  <span>{{ displayInsightDetail(item) }}</span>
                </a-space>
              </a-list-item>
            </template>
          </a-list>

          <a-divider orientation="left">步骤</a-divider>
          <a-collapse>
            <a-collapse-panel
              v-for="(step, index) in selectedReport.steps || []"
              :key="step.title || index"
              :header="step.title || `Step ${index + 1}`"
            >
              <p><strong>问题：</strong> {{ step.question || '-' }}</p>
              <pre class="sql-preview">{{ step.sql || '-' }}</pre>
            </a-collapse-panel>
          </a-collapse>
        </template>
      </template>
    </a-drawer>

    <a-modal
      v-model:open="scheduleModalOpen"
      :title="scheduleForm.id ? '编辑分析计划' : '新建分析计划'"
      :confirm-loading="scheduleSaving"
      width="760px"
      @ok="saveSchedule"
    >
      <a-form :model="scheduleForm" layout="vertical">
        <a-row :gutter="16">
          <a-col :xs="24" :md="12">
            <a-form-item label="计划名称" required>
              <a-input v-model:value="scheduleForm.name" placeholder="例如：每日经营概览" />
            </a-form-item>
          </a-col>
          <a-col :xs="24" :md="12">
            <a-form-item label="时区">
              <a-input v-model:value="scheduleForm.timezone" placeholder="UTC / Asia/Shanghai / America/New_York" />
            </a-form-item>
          </a-col>
        </a-row>

        <a-form-item label="分析表" required>
          <a-select
            v-model:value="scheduleForm.tables"
            mode="multiple"
            show-search
            :options="tableOptions"
            placeholder="选择 1 张或多张表"
          />
        </a-form-item>

        <a-row :gutter="16">
          <a-col :xs="24" :md="8">
            <a-form-item label="调度类型">
              <a-select v-model:value="scheduleForm.schedule_type" :options="scheduleTypeOptions" />
            </a-form-item>
          </a-col>
          <a-col :xs="12" :md="4">
            <a-form-item label="小时">
              <a-input-number v-model:value="scheduleForm.schedule_hour" :min="0" :max="23" style="width: 100%" />
            </a-form-item>
          </a-col>
          <a-col :xs="12" :md="4">
            <a-form-item label="分钟">
              <a-input-number v-model:value="scheduleForm.schedule_minute" :min="0" :max="59" style="width: 100%" />
            </a-form-item>
          </a-col>
          <a-col :xs="12" :md="4">
            <a-form-item label="周几">
              <a-input-number v-model:value="scheduleForm.schedule_day_of_week" :min="1" :max="7" style="width: 100%" />
            </a-form-item>
          </a-col>
          <a-col :xs="12" :md="4">
            <a-form-item label="日期">
              <a-input-number v-model:value="scheduleForm.schedule_day_of_month" :min="1" :max="31" style="width: 100%" />
            </a-form-item>
          </a-col>
        </a-row>

        <a-row :gutter="16">
          <a-col :xs="24" :md="8">
            <a-form-item label="分析深度">
              <a-select v-model:value="scheduleForm.depth" :options="depthOptions" />
            </a-form-item>
            <a-alert
              v-if="scheduleForm.depth === 'expert'"
              class="expert-mode-note"
              type="warning"
              show-icon
              message="Expert 模式单次计划任务通常需要 2-5 分钟。"
            />
          </a-col>
          <a-col :xs="24" :md="8">
            <a-form-item label="AI 资源">
              <a-select v-model:value="scheduleForm.resource_name" allow-clear :options="resourceOptions" />
            </a-form-item>
          </a-col>
          <a-col :xs="24" :md="8">
            <a-form-item label="启用状态">
              <a-switch v-model:checked="scheduleForm.enabled" />
            </a-form-item>
          </a-col>
        </a-row>

        <a-divider orientation="left">推送渠道</a-divider>
        <a-row :gutter="16">
          <a-col :xs="24" :md="8">
            <a-form-item label="推送到 WebSocket">
              <a-switch v-model:checked="scheduleForm.delivery.websocket" />
            </a-form-item>
          </a-col>
          <a-col :xs="24" :md="8">
            <a-form-item label="Webhook 格式">
              <a-select
                v-model:value="scheduleForm.delivery.format"
                allow-clear
                :options="webhookFormatOptions"
              />
            </a-form-item>
          </a-col>
        </a-row>
        <a-row :gutter="16">
          <a-col :xs="24" :md="12">
            <a-form-item label="Webhook URL">
              <a-input v-model:value="scheduleForm.delivery.webhook_url" placeholder="https://hooks.example.com/..." />
            </a-form-item>
          </a-col>
          <a-col :xs="24" :md="12">
            <a-form-item label="Webhook Token">
              <a-input-password
                v-model:value="scheduleForm.delivery.webhook_token"
                placeholder="可选 Bearer token"
              />
            </a-form-item>
          </a-col>
        </a-row>
      </a-form>
    </a-modal>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { message } from 'ant-design-vue';
import type {
  AnalysisDepth,
  AnalysisReport,
  AnalysisSchedule,
  AnalysisScheduleRequest,
  AnalysisScheduleUpdateRequest,
} from '../api/doris';
import { dorisApi } from '../api/doris';
import { extractApiErrorMessage } from '../api/errors';
import { normalizeLLMResource } from '../api/llm-resources';

const activeTab = ref('reports');
const reports = ref<AnalysisReport[]>([]);
const reportsLoading = ref(false);
const reportDrawerOpen = ref(false);
const reportDetailLoading = ref(false);
const selectedReport = ref<AnalysisReport | null>(null);
const analysisRunning = ref(false);
const analysisResult = ref<AnalysisReport | null>(null);
const schedules = ref<AnalysisSchedule[]>([]);
const schedulesLoading = ref(false);
const scheduleSaving = ref(false);
const scheduleModalOpen = ref(false);
const resources = ref<any[]>([]);
const tables = ref<any[]>([]);

const reportFilters = ref({
  table: undefined as string | undefined,
  depth: undefined as string | undefined,
  status: undefined as string | undefined,
});

const analysisForm = ref({
  tableName: undefined as string | undefined,
  depth: 'standard' as AnalysisDepth,
  resourceName: undefined as string | undefined,
});

const buildScheduleForm = () => ({
  id: undefined as string | undefined,
  name: '',
  tables: [] as string[],
  depth: 'standard' as AnalysisDepth,
  resource_name: undefined as string | undefined,
  schedule_type: 'daily' as 'hourly' | 'daily' | 'weekly' | 'monthly',
  schedule_hour: 8,
  schedule_minute: 0,
  schedule_day_of_week: 1,
  schedule_day_of_month: 1,
  timezone: 'UTC',
  enabled: true,
  delivery: {
    websocket: false,
    format: 'generic' as 'generic' | 'slack' | 'dingtalk',
    webhook_url: '',
    webhook_token: '',
  },
});

const scheduleForm = ref(buildScheduleForm());

const depthOptions = [
  { label: 'Quick', value: 'quick' },
  { label: 'Standard', value: 'standard' },
  { label: 'Deep', value: 'deep' },
  { label: 'Expert', value: 'expert' },
];

const statusOptions = [
  { label: 'Completed', value: 'completed' },
  { label: 'Partial', value: 'partial' },
  { label: 'Failed', value: 'failed' },
];

const scheduleTypeOptions = [
  { label: 'Hourly', value: 'hourly' },
  { label: 'Daily', value: 'daily' },
  { label: 'Weekly', value: 'weekly' },
  { label: 'Monthly', value: 'monthly' },
];

const webhookFormatOptions = [
  { label: 'Generic JSON', value: 'generic' },
  { label: 'Slack', value: 'slack' },
  { label: 'DingTalk', value: 'dingtalk' },
];

const scheduleColumns = [
  { title: '名称', dataIndex: 'name', key: 'name' },
  { title: '表', dataIndex: 'tables', key: 'tables' },
  { title: '调度', key: 'schedule' },
  { title: '下次执行', dataIndex: 'next_run_at', key: 'next_run_at', width: 180 },
  { title: '启用', dataIndex: 'enabled', key: 'enabled', width: 100 },
  { title: '操作', key: 'actions', width: 220 },
];

const tableOptions = computed(() =>
  tables.value.map((table) => ({
    label: table.display_name ? `${table.display_name} (${table.table_name})` : table.table_name,
    value: table.table_name,
  })),
);

const resourceOptions = computed(() =>
  resources.value.map((resource) => ({
    label: `${resource.name} (${resource.provider || 'AI'})`,
    value: resource.name,
  })),
);

const filteredReports = computed(() =>
  reports.value.filter((report) => {
    const tablesText = report.table_names || '';
    const matchesTable = !reportFilters.value.table || tablesText.split(',').includes(reportFilters.value.table);
    const matchesDepth = !reportFilters.value.depth || report.depth === reportFilters.value.depth;
    const matchesStatus = !reportFilters.value.status || report.status === reportFilters.value.status;
    return matchesTable && matchesDepth && matchesStatus;
  }),
);

const loadResources = async () => {
  if (resources.value.length > 0) return;
  try {
    const response = await dorisApi.llm.list();
    resources.value = (response.data.resources || []).map((resource: any) => normalizeLLMResource(resource));
  } catch (error: any) {
    message.error('加载 AI 资源失败: ' + extractApiErrorMessage(error));
  }
};

const loadTables = async () => {
  if (tables.value.length > 0) return;
  try {
    const response = await dorisApi.tableRegistry.list();
    tables.value = response.data.tables || [];
  } catch (error: any) {
    message.error('加载表列表失败: ' + extractApiErrorMessage(error));
  }
};

const loadReports = async () => {
  reportsLoading.value = true;
  try {
    const response = await dorisApi.analysis.listReports({ limit: 50, offset: 0 });
    reports.value = response.data.reports || [];
  } catch (error: any) {
    message.error('加载分析报告失败: ' + extractApiErrorMessage(error));
  } finally {
    reportsLoading.value = false;
  }
};

const openReport = async (reportId: string) => {
  reportDrawerOpen.value = true;
  reportDetailLoading.value = true;
  try {
    const response = await dorisApi.analysis.getReport(reportId, true);
    selectedReport.value = response.data;
  } catch (error: any) {
    message.error('加载报告详情失败: ' + extractApiErrorMessage(error));
  } finally {
    reportDetailLoading.value = false;
  }
};

const deleteReport = async (reportId: string) => {
  try {
    await dorisApi.analysis.deleteReport(reportId);
    message.success('报告已删除');
    if (selectedReport.value?.id === reportId) {
      reportDrawerOpen.value = false;
      selectedReport.value = null;
    }
    await loadReports();
  } catch (error: any) {
    message.error('删除报告失败: ' + extractApiErrorMessage(error));
  }
};

const runAnalysis = async () => {
  if (!analysisForm.value.tableName) {
    message.warning('请选择目标表');
    return;
  }

  analysisRunning.value = true;
  try {
    const response = await dorisApi.analysis.analyzeTable(
      analysisForm.value.tableName,
      analysisForm.value.depth,
      analysisForm.value.resourceName,
    );
    analysisResult.value = response.data;
    message.success('分析完成');
    await loadReports();
  } catch (error: any) {
    message.error('运行分析失败: ' + extractApiErrorMessage(error));
  } finally {
    analysisRunning.value = false;
  }
};

const loadSchedules = async () => {
  schedulesLoading.value = true;
  try {
    const response = await dorisApi.analysis.listSchedules();
    schedules.value = response.data.schedules || [];
  } catch (error: any) {
    message.error('加载计划任务失败: ' + extractApiErrorMessage(error));
  } finally {
    schedulesLoading.value = false;
  }
};

const openScheduleModal = (schedule?: AnalysisSchedule) => {
  if (!schedule) {
    scheduleForm.value = buildScheduleForm();
  } else {
    const firstWebhook = (schedule.delivery?.channels || []).find((channel) => channel.type === 'webhook');
    const hasWebSocket = (schedule.delivery?.channels || []).some((channel) => channel.type === 'websocket');
    scheduleForm.value = {
      id: schedule.id,
      name: schedule.name,
      tables: [...schedule.tables],
      depth: schedule.depth,
      resource_name: schedule.resource_name,
      schedule_type: schedule.schedule_type,
      schedule_hour: schedule.schedule_hour,
      schedule_minute: schedule.schedule_minute,
      schedule_day_of_week: schedule.schedule_day_of_week,
      schedule_day_of_month: schedule.schedule_day_of_month,
      timezone: schedule.timezone,
      enabled: schedule.enabled,
      delivery: {
        websocket: hasWebSocket,
        format: (firstWebhook?.format as 'generic' | 'slack' | 'dingtalk') || 'generic',
        webhook_url:
          firstWebhook?.webhook_url && firstWebhook.webhook_url !== '***configured***'
            ? firstWebhook.webhook_url
            : '',
        webhook_token:
          firstWebhook?.webhook_token && firstWebhook.webhook_token !== '***configured***'
            ? firstWebhook.webhook_token
            : '',
      },
    };
  }
  scheduleModalOpen.value = true;
};

const buildDeliveryConfig = () => {
  const channels: any[] = [];
  if (scheduleForm.value.delivery.websocket) {
    channels.push({ type: 'websocket' });
  }
  if (scheduleForm.value.delivery.webhook_url.trim()) {
    channels.push({
      type: 'webhook',
      format: scheduleForm.value.delivery.format,
      webhook_url: scheduleForm.value.delivery.webhook_url.trim(),
      webhook_token: scheduleForm.value.delivery.webhook_token.trim() || undefined,
    });
  }
  return channels.length ? { channels } : undefined;
};

const saveSchedule = async () => {
  if (!scheduleForm.value.name.trim()) {
    message.warning('请输入计划名称');
    return;
  }
  if (!scheduleForm.value.tables.length) {
    message.warning('请至少选择一张表');
    return;
  }

  const payload: AnalysisScheduleRequest | AnalysisScheduleUpdateRequest = {
    name: scheduleForm.value.name.trim(),
    tables: scheduleForm.value.tables,
    depth: scheduleForm.value.depth,
    resource_name: scheduleForm.value.resource_name,
    schedule_type: scheduleForm.value.schedule_type,
    schedule_hour: scheduleForm.value.schedule_hour,
    schedule_minute: scheduleForm.value.schedule_minute,
    schedule_day_of_week: scheduleForm.value.schedule_day_of_week,
    schedule_day_of_month: scheduleForm.value.schedule_day_of_month,
    timezone: scheduleForm.value.timezone.trim() || 'UTC',
    enabled: scheduleForm.value.enabled,
    delivery: buildDeliveryConfig(),
  };

  scheduleSaving.value = true;
  try {
    if (scheduleForm.value.id) {
      await dorisApi.analysis.updateSchedule(scheduleForm.value.id, payload);
      message.success('计划已更新');
    } else {
      await dorisApi.analysis.createSchedule(payload as AnalysisScheduleRequest);
      message.success('计划已创建');
    }
    scheduleModalOpen.value = false;
    await loadSchedules();
  } catch (error: any) {
    message.error('保存计划失败: ' + extractApiErrorMessage(error));
  } finally {
    scheduleSaving.value = false;
  }
};

const toggleSchedule = async (scheduleId: string) => {
  try {
    await dorisApi.analysis.toggleSchedule(scheduleId);
    await loadSchedules();
  } catch (error: any) {
    message.error('切换计划失败: ' + extractApiErrorMessage(error));
  }
};

const runSchedule = async (scheduleId: string) => {
  try {
    const response = await dorisApi.analysis.runNow(scheduleId);
    message.success(`已触发 ${response.data.count || 0} 份分析`);
    await Promise.all([loadReports(), loadSchedules()]);
  } catch (error: any) {
    message.error('执行计划失败: ' + extractApiErrorMessage(error));
  }
};

const removeSchedule = async (scheduleId: string) => {
  try {
    await dorisApi.analysis.deleteSchedule(scheduleId);
    message.success('计划已删除');
    await loadSchedules();
  } catch (error: any) {
    message.error('删除计划失败: ' + extractApiErrorMessage(error));
  }
};

const refreshActiveTab = async () => {
  if (activeTab.value === 'reports') {
    await loadReports();
    return;
  }
  if (activeTab.value === 'schedules') {
    await loadSchedules();
  }
};

const describeSchedule = (schedule: AnalysisSchedule) => {
  const hh = String(schedule.schedule_hour ?? 0).padStart(2, '0');
  const mm = String(schedule.schedule_minute ?? 0).padStart(2, '0');
  if (schedule.schedule_type === 'hourly') {
    return `每小时 ${mm} 分 (${schedule.timezone})`;
  }
  if (schedule.schedule_type === 'daily') {
    return `每日 ${hh}:${mm} (${schedule.timezone})`;
  }
  if (schedule.schedule_type === 'weekly') {
    return `每周 ${schedule.schedule_day_of_week} ${hh}:${mm} (${schedule.timezone})`;
  }
  return `每月 ${schedule.schedule_day_of_month} 日 ${hh}:${mm} (${schedule.timezone})`;
};

const statusColor = (status?: string) => {
  if (status === 'completed') return 'green';
  if (status === 'partial') return 'orange';
  if (status === 'failed') return 'red';
  return 'default';
};

const confidenceColor = (score?: number | null) => {
  if (typeof score !== 'number') return 'default';
  if (score >= 0.8) return 'green';
  if (score >= 0.6) return 'gold';
  return 'red';
};

const formatConfidence = (score?: number | null) => {
  if (typeof score !== 'number') return 'Confidence -';
  return `Confidence ${(score * 100).toFixed(0)}%`;
};

const formatJson = (value: unknown) => {
  if (value == null) return '-';
  return JSON.stringify(value, null, 2);
};

const parseStructuredValue = (value: any): Record<string, any> => {
  if (!value) return {};
  if (typeof value === 'object') return value;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return typeof parsed === 'object' && parsed ? parsed : { detail: value };
    } catch {
      return { detail: value };
    }
  }
  return { detail: String(value) };
};

const joinInsightParts = (payload: Record<string, any>) => {
  const parts = [
    payload.detail,
    payload.description,
    payload.quantification,
    payload.business_impact,
    payload.evidence,
    payload.recommendation,
  ]
    .filter((value, index, array) => typeof value === 'string' && value.trim() && array.indexOf(value) === index)
    .map((value) => String(value).trim());
  return parts.join('；');
};

const displayInsightTitle = (item: any) => {
  const payload = parseStructuredValue(item);
  return payload.title || payload.category || payload.headline || '洞察';
};

const displayInsightDetail = (item: any) => {
  const payload = parseStructuredValue(item);
  const detail = joinInsightParts(payload);
  if (detail) return detail;
  if (typeof item === 'string') return item;
  return '暂无明细';
};

const displayFindingTitle = (finding: any) => {
  const payload = parseStructuredValue(finding);
  return payload.title || payload.category || payload.headline || payload.description || '关键发现';
};

const displayFindingDetail = (item: any) => {
  const payload = parseStructuredValue(item?.finding);
  const detail = item?.detail || joinInsightParts(payload);
  return detail || '';
};

const isExpertReport = (report: AnalysisReport | null) => (report?.depth || '').toLowerCase() === 'expert';

const sanitizeSummaryText = (value?: string | null) =>
  (value || '')
    .split(/[。！？!?]/)
    .map((part) => part.trim())
    .filter(
      (part) =>
        part &&
        !part.toLowerCase().includes('descriptive -> diagnostic -> predictive') &&
        !part.toLowerCase().includes('methodology') &&
        !part.includes('方法论'),
    )
    .slice(0, 2)
    .join('；');

const normalizeNarrativeItems = (items: any[] | undefined, defaultPrefix: string, limit: number = 3) =>
  (items || [])
    .map((item, index) => {
      const payload = parseStructuredValue(item);
      const title =
        payload.title ||
        payload.category ||
        payload.headline ||
        payload.theme ||
        `${defaultPrefix} ${index + 1}`;
      const detail = joinInsightParts(payload) || (typeof item === 'string' ? item.trim() : '');
      return {
        title,
        detail: detail || title,
        severity: payload.severity,
        urgency: payload.urgency,
      };
    })
    .filter((item) => item.title || item.detail)
    .slice(0, limit);

const expertTopInsights = (report: AnalysisReport | null) => {
  if (!report) return [];
  return normalizeNarrativeItems((report.top_insights as any[]) || (report.insights as any[]), '关键洞察');
};

const expertActionItems = (report: AnalysisReport | null) => {
  if (!report) return [];
  return normalizeNarrativeItems((report.action_items as any[]) || (report.recommendations as any[]), '动作建议');
};

const expertExecutiveSummary = (report: AnalysisReport | null) => {
  if (!report) return '暂无经营摘要';
  const sanitized = sanitizeSummaryText(report.executive_summary || report.summary);
  if (sanitized) return sanitized + '。';
  const insightItems = expertTopInsights(report);
  if (insightItems.length) {
    return (
      insightItems
        .slice(0, 2)
        .map((item) => `${item.title}：${item.detail}`)
        .join('；')
        .slice(0, 120) + '。'
    );
  }
  const actionItems = expertActionItems(report);
  const firstAction = actionItems[0];
  if (firstAction) {
    return `${firstAction.title}：${firstAction.detail}`.slice(0, 120) + '。';
  }
  return '暂无经营摘要';
};

const readableSummary = (report: AnalysisReport | null) => {
  if (!report) return '暂无摘要';
  if (isExpertReport(report)) {
    return expertExecutiveSummary(report);
  }
  const rawSummary = (report.summary || '').trim();
  const sanitized = sanitizeSummaryText(rawSummary);
  if (sanitized) return sanitized + '。';

  const insightItems = (report.insights || []).slice(0, 2);
  if (insightItems.length) {
    return (
      insightItems
        .map((item) => `${displayInsightTitle(item)}：${displayInsightDetail(item)}`)
        .join('；')
        .slice(0, 120) + '。'
    );
  }
  return rawSummary || '暂无摘要';
};

const formatDuration = (durationMs?: number) => {
  if (!durationMs) return '耗时 -';
  return `耗时 ${(durationMs / 1000).toFixed(1)}s`;
};

onMounted(async () => {
  await Promise.all([loadResources(), loadTables(), loadReports(), loadSchedules()]);
});
</script>

<style scoped>
.data-analysis {
  padding: 24px;
}

.toolbar-row {
  margin-bottom: 16px;
}

.report-card {
  border-radius: 10px;
  background: linear-gradient(180deg, #ffffff 0%, #fafcff 100%);
}

.report-summary {
  margin: 0 0 12px;
  color: #24324a;
  line-height: 1.6;
}

.business-summary-card {
  padding: 14px 16px;
  border: 1px solid #d9e3f0;
  border-radius: 10px;
  background: linear-gradient(180deg, #f8fbff 0%, #ffffff 100%);
  color: #24324a;
  line-height: 1.8;
}

.report-metrics {
  row-gap: 8px;
}

.report-meta {
  color: #667085;
  font-size: 12px;
}

.analysis-result {
  margin-top: 8px;
}

.expert-mode-note {
  margin-bottom: 16px;
}

.expert-detail-collapse {
  margin-top: 16px;
}

.evidence-card {
  width: 100%;
  padding: 12px;
  border: 1px solid #d9e3f0;
  border-radius: 8px;
  background: #f8fbff;
}

.inline-metric {
  margin-top: 4px;
  color: #475467;
  font-size: 12px;
}

.round-result-list {
  display: grid;
  gap: 12px;
  margin-top: 12px;
}

.reasoning-preview {
  margin: 0;
  padding: 12px;
  overflow-x: auto;
  white-space: pre-wrap;
  border-radius: 8px;
  background: #fdf7f2;
  font-size: 12px;
  line-height: 1.6;
}

.sql-preview {
  margin: 0;
  padding: 12px;
  overflow-x: auto;
  border-radius: 8px;
  background: #f7f9fc;
  font-size: 12px;
}
</style>
