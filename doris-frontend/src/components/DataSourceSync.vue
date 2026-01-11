<template>
  <div class="datasource-sync">
    <a-row :gutter="24">
      <!-- 左侧：数据源配置 -->
      <a-col :span="10">
        <a-card title="数据源配置" :bordered="false">
          <a-form :model="formState" layout="vertical">
            <a-form-item label="数据源名称">
              <a-input v-model:value="formState.name" placeholder="如：生产数据库" />
            </a-form-item>
            <a-form-item label="主机地址 (Host)">
              <a-input v-model:value="formState.host" placeholder="如：192.168.1.100" />
            </a-form-item>
            <a-form-item label="端口 (Port)">
              <a-input-number v-model:value="formState.port" :min="1" :max="65535" style="width: 100%" />
            </a-form-item>
            <a-form-item label="用户名 (User)">
              <a-input v-model:value="formState.user" placeholder="数据库用户名" />
            </a-form-item>
            <a-form-item label="密码 (Password)">
              <a-input-password v-model:value="formState.password" placeholder="数据库密码" />
            </a-form-item>
            <a-form-item label="数据库名">
              <a-select 
                v-model:value="formState.database" 
                :options="databaseOptions"
                placeholder="先测试连接获取数据库列表"
                :disabled="databases.length === 0"
              />
            </a-form-item>
            <a-form-item>
              <a-space>
                <a-button type="primary" ghost @click="testConnection" :loading="testLoading">
                  测试连接
                </a-button>
                <a-button type="primary" @click="saveDataSource" :loading="saveLoading" :disabled="!formState.database">
                  保存数据源
                </a-button>
              </a-space>
            </a-form-item>
          </a-form>

          <!-- 已保存的数据源列表 -->
          <a-divider>已保存的数据源</a-divider>
          <a-list :data-source="datasources" :loading="listLoading" size="small">
            <template #renderItem="{ item }">
              <a-list-item>
                <a-list-item-meta>
                  <template #title>
                    <a @click="selectDataSource(item)">{{ item.name }}</a>
                  </template>
                  <template #description>
                    {{ item.host }}:{{ item.port }} / {{ item.database_name }}
                  </template>
                </a-list-item-meta>
                <template #actions>
                  <a-button type="link" size="small" @click="selectDataSource(item)">选择</a-button>
                  <a-popconfirm title="确定删除此数据源？" @confirm="deleteDataSource(item.id)">
                    <a-button type="link" danger size="small">删除</a-button>
                  </a-popconfirm>
                </template>
              </a-list-item>
            </template>
          </a-list>
        </a-card>
      </a-col>

      <!-- 右侧：表格同步 -->
      <a-col :span="14">
        <a-card title="表格管理" :bordered="false">
          <template #extra>
            <a-tag v-if="selectedDatasource" color="blue">
              当前: {{ selectedDatasource.name }}
            </a-tag>
          </template>

          <div v-if="!selectedDatasource" style="text-align: center; padding: 40px; color: #999">
            请先选择或创建一个数据源
          </div>

          <div v-else>
            <!-- 表格列表 -->
            <a-table
              :dataSource="remoteTables"
              :columns="tableColumns"
              :loading="tablesLoading"
              :row-selection="rowSelection"
              :row-key="(record: any) => record.name"
              size="small"
              :pagination="{ pageSize: 10 }"
            >
              <template #bodyCell="{ column, record }">
                <template v-if="column.key === 'sync_status'">
                  <a-tag v-if="isTableSynced(record.name)" color="green">已同步</a-tag>
                  <a-tag v-else>未同步</a-tag>
                </template>
                <template v-if="column.key === 'row_count'">
                  <a-tag color="blue">{{ record.row_count || 0 }} 行</a-tag>
                </template>
                <template v-if="column.key === 'ai_enabled'">
                  <a-switch
                    :checked="getTableAIEnabled(record.name)"
                    @change="(checked: boolean) => toggleTableAI(record.name, checked)"
                    checked-children="开"
                    un-checked-children="关"
                  />
                </template>
                <template v-if="column.key === 'actions'">
                  <a-space>
                    <a-button type="link" size="small" @click="previewTable(record.name)">
                      <eye-outlined /> 预览
                    </a-button>
                    <a-button type="link" size="small" @click="openScheduleConfig(record.name)">
                      <setting-outlined /> 配置
                    </a-button>
                  </a-space>
                </template>
                <template v-if="column.key === 'schedule'">
                  <span>{{ getTableScheduleDescription(record.name) }}</span>
                </template>
              </template>
            </a-table>

            <a-divider />

            <!-- 同步按钮 -->
            <a-button
              type="primary"
              size="large"
              block
              @click="startSync"
              :loading="syncLoading"
              :disabled="selectedTables.length === 0"
            >
              <sync-outlined /> 同步选中表 ({{ selectedTables.length }} 张)
            </a-button>

            <!-- 同步结果 -->
            <a-alert
              v-if="syncResult"
              :type="syncResult.success ? 'success' : 'error'"
              :message="syncResult.success ? '同步完成' : '同步失败'"
              style="margin-top: 16px"
              show-icon
              closable
            >
              <template #description>
                <div v-if="syncResult.success">
                  <p style="margin-bottom: 8px">{{ syncResultDescription }}</p>
                  <a-collapse v-if="syncResult.results && syncResult.results.length > 0" :bordered="false" size="small">
                    <a-collapse-panel key="1" header="查看详情">
                      <a-list :data-source="syncResult.results" size="small">
                        <template #renderItem="{ item }">
                          <a-list-item>
                            <a-list-item-meta>
                              <template #title>
                                <a-tag :color="item.success ? 'green' : 'red'">
                                  {{ item.source_table }}
                                </a-tag>
                              </template>
                              <template #description>
                                <span v-if="item.success">
                                  同步 {{ item.rows_synced || 0 }} 行
                                  <a-tag v-if="item.table_created" color="blue" size="small">新建表</a-tag>
                                  <a-tag v-if="item.incremental" color="cyan" size="small">增量同步</a-tag>
                                </span>
                                <span v-else style="color: #f5222d">{{ item.error }}</span>
                              </template>
                            </a-list-item-meta>
                          </a-list-item>
                        </template>
                      </a-list>
                    </a-collapse-panel>
                  </a-collapse>
                </div>
                <div v-else>{{ syncResultDescription }}</div>
              </template>
            </a-alert>

            <!-- 同步进行中提示 -->
            <a-alert
              v-if="syncLoading"
              type="info"
              message="正在同步..."
              description="大表同步可能需要几分钟时间，请耐心等待"
              style="margin-top: 16px"
              show-icon
            >
              <template #icon><loading-outlined spin /></template>
            </a-alert>
          </div>
        </a-card>
      </a-col>
    </a-row>

    <!-- 表格预览弹窗 -->
    <a-modal
      v-model:open="previewModalVisible"
      :title="'表格预览: ' + previewTableName"
      width="90%"
      :footer="null"
    >
      <a-spin :spinning="previewLoading">
        <div v-if="previewData">
          <a-descriptions title="表结构" bordered size="small" :column="4" style="margin-bottom: 16px">
            <a-descriptions-item v-for="col in previewData.columns" :key="col.name" :label="col.name">
              {{ col.full_type }} <span v-if="col.comment" style="color: #999">{{ col.comment }}</span>
            </a-descriptions-item>
          </a-descriptions>

          <a-divider>数据预览 (共 {{ previewData.total_rows }} 行，显示前 {{ previewData.preview_rows }} 行)</a-divider>

          <a-table
            :dataSource="previewData.data"
            :columns="previewColumns"
            size="small"
            :scroll="{ x: 'max-content', y: 400 }"
            :pagination="false"
          />
        </div>
      </a-spin>
    </a-modal>

    <!-- 同步策略配置弹窗 -->
    <a-modal
      v-model:open="scheduleModalVisible"
      :title="'同步策略配置: ' + scheduleTableName"
      @ok="saveScheduleConfig"
      :confirm-loading="scheduleSaving"
    >
      <a-form :model="scheduleForm" layout="vertical">
        <a-form-item label="同步类型">
          <a-radio-group v-model:value="scheduleForm.schedule_type">
            <a-radio value="hourly">每小时</a-radio>
            <a-radio value="daily">每天</a-radio>
            <a-radio value="weekly">每周</a-radio>
            <a-radio value="monthly">每月</a-radio>
          </a-radio-group>
        </a-form-item>

        <!-- 每小时：选择分钟 -->
        <a-form-item v-if="scheduleForm.schedule_type === 'hourly'" label="执行分钟">
          <a-select v-model:value="scheduleForm.schedule_minute" style="width: 120px">
            <a-select-option v-for="m in 60" :key="m-1" :value="m-1">第 {{ m-1 }} 分钟</a-select-option>
          </a-select>
        </a-form-item>

        <!-- 每天：选择时间 -->
        <a-form-item v-if="scheduleForm.schedule_type === 'daily'" label="执行时间">
          <a-time-picker
            v-model:value="scheduleTimeValue"
            format="HH:mm"
            :minute-step="5"
          />
        </a-form-item>

        <!-- 每周：选择周几和时间 -->
        <a-form-item v-if="scheduleForm.schedule_type === 'weekly'" label="执行日期">
          <a-space>
            <a-select v-model:value="scheduleForm.schedule_day_of_week" style="width: 100px">
              <a-select-option :value="1">周一</a-select-option>
              <a-select-option :value="2">周二</a-select-option>
              <a-select-option :value="3">周三</a-select-option>
              <a-select-option :value="4">周四</a-select-option>
              <a-select-option :value="5">周五</a-select-option>
              <a-select-option :value="6">周六</a-select-option>
              <a-select-option :value="7">周日</a-select-option>
            </a-select>
            <a-time-picker
              v-model:value="scheduleTimeValue"
              format="HH:mm"
              :minute-step="5"
            />
          </a-space>
        </a-form-item>

        <!-- 每月：选择日期和时间 -->
        <a-form-item v-if="scheduleForm.schedule_type === 'monthly'" label="执行日期">
          <a-space>
            <a-select v-model:value="scheduleForm.schedule_day_of_month" style="width: 100px">
              <a-select-option v-for="d in 28" :key="d" :value="d">{{ d }} 号</a-select-option>
            </a-select>
            <a-time-picker
              v-model:value="scheduleTimeValue"
              format="HH:mm"
              :minute-step="5"
            />
          </a-space>
        </a-form-item>

        <a-form-item label="AI分析">
          <a-switch
            v-model:checked="scheduleForm.enabled_for_ai"
            checked-children="启用"
            un-checked-children="禁用"
          />
          <span style="margin-left: 8px; color: #999">启用后，AI问答会查询此表数据</span>
        </a-form-item>
      </a-form>
    </a-modal>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue';
import { message } from 'ant-design-vue';
import { SyncOutlined, EyeOutlined, SettingOutlined, LoadingOutlined } from '@ant-design/icons-vue';
import { dorisApi } from '../api/doris';
import dayjs, { Dayjs } from 'dayjs';

// 表单状态
const formState = ref({
  name: '',
  host: '',
  port: 3306,
  user: '',
  password: '',
  database: ''
});

// 数据
const databases = ref<string[]>([]);
const datasources = ref<any[]>([]);
const selectedDatasource = ref<any>(null);
const remoteTables = ref<any[]>([]);
const selectedTables = ref<string[]>([]);
const syncTasks = ref<any[]>([]);
const syncedTableNames = ref<Set<string>>(new Set());

const isTableSynced = (tableName: string): boolean => {
  return syncedTableNames.value.has(tableName);
};

// 同步结果
const syncResult = ref<any>(null);

// Loading 状态
const testLoading = ref(false);
const saveLoading = ref(false);
const listLoading = ref(false);
const tablesLoading = ref(false);
const syncLoading = ref(false);

// 表格列定义
const tableColumns = [
  { title: '表名', dataIndex: 'name', key: 'name' },
  { title: '状态', key: 'sync_status', width: 90 },
  { title: '行数', dataIndex: 'row_count', key: 'row_count', width: 100 },
  { title: 'AI分析', key: 'ai_enabled', width: 80 },
  { title: '操作', key: 'actions', width: 150 },
  { title: '同步策略', key: 'schedule', width: 150 },
];

// 预览相关
const previewModalVisible = ref(false);
const previewTableName = ref('');
const previewLoading = ref(false);
const previewData = ref<any>(null);
const previewColumns = computed(() => {
  if (!previewData.value?.columns) return [];
  return previewData.value.columns.map((col: any) => ({
    title: col.name,
    dataIndex: col.name,
    key: col.name,
    ellipsis: true,
  }));
});

// 同步策略配置相关
const scheduleModalVisible = ref(false);
const scheduleTableName = ref('');
const scheduleSaving = ref(false);
const scheduleForm = ref({
  schedule_type: 'daily',
  schedule_minute: 0,
  schedule_hour: 0,
  schedule_day_of_week: 1,
  schedule_day_of_month: 1,
  enabled_for_ai: true,
});
const scheduleTimeValue = ref<Dayjs>(dayjs().hour(0).minute(0));

// 监听时间选择器变化
watch(scheduleTimeValue, (val) => {
  if (val) {
    scheduleForm.value.schedule_hour = val.hour();
    scheduleForm.value.schedule_minute = val.minute();
  }
});

// 计算属性
const databaseOptions = computed(() =>
  databases.value.map(db => ({ label: db, value: db }))
);

const syncResultDescription = computed(() => {
  if (!syncResult.value) return '';
  if (syncResult.value.success) {
    return `成功同步 ${syncResult.value.success_count} 张表，共 ${syncResult.value.results?.reduce((sum: number, r: any) => sum + (r.rows_synced || 0), 0) || 0} 行数据`;
  }

  const formatError = (err: any): string => {
    if (!err) return '';
    if (typeof err === 'string') return err;
    if (typeof err === 'object') {
      return err.detail || err.error || err.message || JSON.stringify(err);
    }
    return String(err);
  };

  if (syncResult.value.error) {
    return formatError(syncResult.value.error);
  }

  if (syncResult.value.results && syncResult.value.results.length > 0) {
    const failed = syncResult.value.results.find((r: any) => !r.success);
    if (failed) {
      return `同步失败: ${formatError(failed.error) || '未知错误'}`;
    }
  }

  return `未知错误 (原始数据: ${JSON.stringify(syncResult.value).substring(0, 200)}...)`;
});

// 测试连接
const testConnection = async () => {
  if (!formState.value.host || !formState.value.user) {
    message.warning('请填写主机地址和用户名');
    return;
  }

  testLoading.value = true;
  try {
    const response = await dorisApi.datasource.test({
      host: formState.value.host,
      port: formState.value.port,
      user: formState.value.user,
      password: formState.value.password
    });

    if (response.data.success) {
      message.success('连接成功！');
      databases.value = response.data.databases || [];
    } else {
      message.error(response.data.message || '连接失败');
    }
  } catch (error: any) {
    message.error('连接失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    testLoading.value = false;
  }
};

// 保存数据源
const saveDataSource = async () => {
  if (!formState.value.name || !formState.value.database) {
    message.warning('请填写数据源名称和选择数据库');
    return;
  }

  saveLoading.value = true;
  try {
    await dorisApi.datasource.save(formState.value);
    message.success('数据源保存成功');
    loadDatasources();
    // 清空表单
    formState.value = { name: '', host: '', port: 3306, user: '', password: '', database: '' };
    databases.value = [];
  } catch (error: any) {
    message.error('保存失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    saveLoading.value = false;
  }
};

// 加载数据源列表
const loadDatasources = async () => {
  listLoading.value = true;
  try {
    const response = await dorisApi.datasource.list();
    datasources.value = response.data.datasources || [];
  } catch (error) {
    console.error('加载数据源失败', error);
  } finally {
    listLoading.value = false;
  }
};

// 选择数据源
const selectDataSource = async (ds: any) => {
  selectedDatasource.value = ds;
  selectedTables.value = [];
  syncResult.value = null;

  tablesLoading.value = true;
  try {
    const [tablesRes, tasksRes, registryRes] = await Promise.all([
      dorisApi.datasource.getTables(ds.id),
      dorisApi.syncTasks.list(),
      dorisApi.tableRegistry.list(),
    ]);
    remoteTables.value = tablesRes.data.tables || [];
    syncTasks.value = tasksRes.data.tasks || [];
    const registryTables = registryRes.data.tables || [];
    const synced = registryTables
      .filter((t: any) => t?.source_type === 'database_sync' && t?.table_name)
      .map((t: any) => t.table_name);
    syncedTableNames.value = new Set(synced);
  } catch (error: any) {
    message.error('获取表列表失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    tablesLoading.value = false;
  }
};

// 删除数据源
const deleteDataSource = async (dsId: string) => {
  try {
    await dorisApi.datasource.delete(dsId);
    message.success('数据源已删除');
    if (selectedDatasource.value?.id === dsId) {
      selectedDatasource.value = null;
      remoteTables.value = [];
    }
    loadDatasources();
  } catch (error: any) {
    message.error('删除失败: ' + (error.response?.data?.detail || error.message));
  }
};

// 表格选择变化
const onTableSelectChange = (keys: string[]) => {
  selectedTables.value = keys;
};

const rowSelection = computed(() => ({
  selectedRowKeys: selectedTables.value,
  onChange: onTableSelectChange,
  getCheckboxProps: (record: any) => ({
    disabled: isTableSynced(record?.name),
  }),
}));

// 获取表的AI启用状态
const getTableAIEnabled = (tableName: string): boolean => {
  const task = syncTasks.value.find((t: any) => t.source_table === tableName);
  return task?.enabled_for_ai ?? true;
};

// 切换表的AI启用状态
const toggleTableAI = async (tableName: string, enabled: boolean) => {
  const task = syncTasks.value.find((t: any) => t.source_table === tableName);
  if (task) {
    try {
      await dorisApi.syncTasks.toggleAI(task.id, enabled);
      task.enabled_for_ai = enabled;
      message.success(`${tableName} AI分析已${enabled ? '启用' : '禁用'}`);
    } catch (error: any) {
      message.error('操作失败: ' + (error.response?.data?.detail || error.message));
    }
  } else {
    message.info('请先配置此表的同步策略');
  }
};

// 获取表的同步策略描述
const getTableScheduleDescription = (tableName: string): string => {
  const task = syncTasks.value.find((t: any) => t.source_table === tableName);
  if (!task) return '--';

  const weekdays = ['', '周一', '周二', '周三', '周四', '周五', '周六', '周日'];
  const hour = task.schedule_hour ?? 0;
  const minute = task.schedule_minute ?? 0;
  const timeStr = `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;

  switch (task.schedule_type) {
    case 'hourly': return `每小时第${minute}分钟`;
    case 'daily': return `每天 ${timeStr}`;
    case 'weekly': return `每${weekdays[task.schedule_day_of_week || 1]} ${timeStr}`;
    case 'monthly': return `每月${task.schedule_day_of_month || 1}号 ${timeStr}`;
    default: return task.schedule_type;
  }
};

// 预览表格
const previewTable = async (tableName: string) => {
  if (!selectedDatasource.value) return;

  previewTableName.value = tableName;
  previewModalVisible.value = true;
  previewLoading.value = true;
  previewData.value = null;

  try {
    const response = await dorisApi.datasource.previewTable(
      selectedDatasource.value.id,
      tableName,
      100
    );
    previewData.value = response.data;
  } catch (error: any) {
    message.error('预览失败: ' + (error.response?.data?.detail || error.message));
    previewModalVisible.value = false;
  } finally {
    previewLoading.value = false;
  }
};

// 打开同步策略配置弹窗
const openScheduleConfig = (tableName: string) => {
  scheduleTableName.value = tableName;

  // 查找现有配置
  const task = syncTasks.value.find((t: any) => t.source_table === tableName);
  if (task) {
    scheduleForm.value = {
      schedule_type: task.schedule_type || 'daily',
      schedule_minute: task.schedule_minute || 0,
      schedule_hour: task.schedule_hour || 0,
      schedule_day_of_week: task.schedule_day_of_week || 1,
      schedule_day_of_month: task.schedule_day_of_month || 1,
      enabled_for_ai: task.enabled_for_ai ?? true,
    };
    scheduleTimeValue.value = dayjs().hour(task.schedule_hour || 0).minute(task.schedule_minute || 0);
  } else {
    // 默认配置
    scheduleForm.value = {
      schedule_type: 'daily',
      schedule_minute: 0,
      schedule_hour: 3,
      schedule_day_of_week: 1,
      schedule_day_of_month: 1,
      enabled_for_ai: true,
    };
    scheduleTimeValue.value = dayjs().hour(3).minute(0);
  }

  scheduleModalVisible.value = true;
};

// 保存同步策略配置
const saveScheduleConfig = async () => {
  if (!selectedDatasource.value) return;

  scheduleSaving.value = true;
  try {
    const existingTask = syncTasks.value.find((t: any) => t.source_table === scheduleTableName.value);

    if (existingTask) {
      // 更新现有任务
      await dorisApi.syncTasks.update(existingTask.id, {
        schedule_type: scheduleForm.value.schedule_type,
        schedule_minute: scheduleForm.value.schedule_minute,
        schedule_hour: scheduleForm.value.schedule_hour,
        schedule_day_of_week: scheduleForm.value.schedule_day_of_week,
        schedule_day_of_month: scheduleForm.value.schedule_day_of_month,
        enabled_for_ai: scheduleForm.value.enabled_for_ai,
      });
      message.success('同步策略已更新');
    } else {
      // 创建新任务
      await dorisApi.syncTasks.create({
        datasource_id: selectedDatasource.value.id,
        source_table: scheduleTableName.value,
        schedule_type: scheduleForm.value.schedule_type as any,
        schedule_minute: scheduleForm.value.schedule_minute,
        schedule_hour: scheduleForm.value.schedule_hour,
        schedule_day_of_week: scheduleForm.value.schedule_day_of_week,
        schedule_day_of_month: scheduleForm.value.schedule_day_of_month,
        enabled_for_ai: scheduleForm.value.enabled_for_ai,
      });
      message.success('同步策略已创建');
    }

    // 刷新任务列表
    const tasksRes = await dorisApi.syncTasks.list();
    syncTasks.value = tasksRes.data.tasks || [];

    scheduleModalVisible.value = false;
  } catch (error: any) {
    message.error('保存失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    scheduleSaving.value = false;
  }
};

// 开始同步（立即同步选中的表）
const startSync = async () => {
  if (!selectedDatasource.value || selectedTables.value.length === 0) {
    message.warning('请选择要同步的表');
    return;
  }

  const tablesToSync = selectedTables.value.filter((name) => !isTableSynced(name));
  if (tablesToSync.length === 0) {
    message.info('所选表已同步，请使用“配置”设置同步周期');
    return;
  }

  syncLoading.value = true;
  syncResult.value = null;

  try {
    const tables = tablesToSync.map(name => ({ source_table: name }));
    const response = await dorisApi.datasource.syncMultiple(selectedDatasource.value.id, tables);
    syncResult.value = response.data;

    if (response.data.success) {
      message.success(`同步完成！成功 ${response.data.success_count} 张表`);
    } else {
      message.warning(`同步部分失败：成功 ${response.data.success_count}，失败 ${response.data.fail_count}`);
    }

    selectedTables.value = [];
    const tasksRes = await dorisApi.syncTasks.list();
    syncTasks.value = tasksRes.data.tasks || [];
    const registryRes = await dorisApi.tableRegistry.list();
    const registryTables = registryRes.data.tables || [];
    const synced = registryTables
      .filter((t: any) => t?.source_type === 'database_sync' && t?.table_name)
      .map((t: any) => t.table_name);
    syncedTableNames.value = new Set(synced);
  } catch (error: any) {
    syncResult.value = { success: false, error: error.response?.data?.detail || error.message };
    message.error('同步失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    syncLoading.value = false;
  }
};

onMounted(() => {
  loadDatasources();
});
</script>

<style scoped>
.datasource-sync {
  padding: 24px;
}
</style>
