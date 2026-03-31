import axios, { AxiosHeaders } from 'axios';
import { applyGatewayAuthHeaders, resolveGatewayApiKey } from './auth';

// 使用相对路径,通过 Nginx 代理到后端
// 开发环境: http://localhost:8000
// 生产环境(Docker): /api (由 Nginx 代理)
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '';

const api = axios.create({
  baseURL: API_BASE_URL,
  // 增加超时时间到 5 分钟，以支持大数据量同步
  timeout: 300000,
  headers: {
    'Content-Type': 'application/json',
  },
});

api.interceptors.request.use((config) => {
  const apiKey = resolveGatewayApiKey({
    envApiKey: import.meta.env.VITE_SMATRIX_API_KEY,
  });

  const nextHeaders = applyGatewayAuthHeaders(
    { ...(config.headers as Record<string, string | undefined> | undefined) },
    apiKey,
  );
  const axiosHeaders = AxiosHeaders.from(config.headers);
  for (const [key, value] of Object.entries(nextHeaders)) {
    if (typeof value === 'string' && value) {
      axiosHeaders.set(key, value);
    } else {
      axiosHeaders.delete(key);
    }
  }
  config.headers = axiosHeaders;
  return config;
});

export interface ExecuteRequest {
  action: string;
  table?: string;
  column?: string;
  params?: Record<string, any>;
}

export interface QueryCatalogField {
  field_name: string;
  display_name: string;
  description?: string;
  field_type?: string;
  semantic?: string;
  semantic_label?: string;
  enum_values?: string[];
  value_range?: [number, number] | null;
}

export interface QueryCatalogRelationship {
  id?: string;
  related_table_name: string;
  related_display_name: string;
  relation_type?: string;
  relation_type_label?: string;
  relation_label?: string;
  relation_description?: string;
  source_field_name: string;
  source_field_display_name: string;
  target_field_name: string;
  target_field_display_name: string;
}

export interface QueryCatalogTable {
  table_name: string;
  display_name: string;
  description?: string;
  source_type?: string;
  fields: QueryCatalogField[];
  relationships: QueryCatalogRelationship[];
}

export interface LLMConfigRequest {
  resource_name: string;
  provider_type: string;
  endpoint: string;
  model_name: string;
  api_key?: string;
  temperature?: number;
  max_tokens?: number;
}

export interface NaturalQueryRequest {
  query: string;
  resource_name?: string;
  table_names?: string[];
  api_key?: string;
  model?: string;
  base_url?: string;
}

export interface DataSourceTestRequest {
  host: string;
  port: number;
  user: string;
  password: string;
  database?: string;
}

export interface DataSourceSaveRequest {
  name: string;
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

export interface SyncTableRequest {
  source_table: string;
  target_table?: string;
}

export interface ScheduleSyncRequest {
  datasource_id: string;
  source_table: string;
  target_table?: string;
  schedule_type: 'hourly' | 'daily' | 'weekly' | 'monthly';
  schedule_minute?: number;
  schedule_hour?: number;
  schedule_day_of_week?: number;
  schedule_day_of_month?: number;
  enabled_for_ai?: boolean;
}

export interface UpdateSyncTaskRequest {
  schedule_type?: string;
  schedule_minute?: number;
  schedule_hour?: number;
  schedule_day_of_week?: number;
  schedule_day_of_month?: number;
  enabled_for_ai?: boolean;
}

export interface UpdateTableRegistryRequest {
  display_name?: string;
  description?: string;
}

export interface AnalysisInsight {
  title: string;
  detail: string;
  severity?: string;
}

export interface AnalysisReport {
  id: string;
  table_names: string;
  trigger_type?: string;
  depth?: string;
  schedule_id?: string | null;
  history_id?: string | null;
  summary?: string;
  insights?: AnalysisInsight[];
  anomalies?: any[];
  recommendations?: string[];
  failed_step_count?: number;
  insight_count?: number;
  anomaly_count?: number;
  status?: string;
  duration_ms?: number;
  created_at?: string;
  steps?: any[];
}

export interface AnalysisDeliveryChannel {
  type: 'webhook' | 'websocket';
  format?: 'generic' | 'slack' | 'dingtalk';
  webhook_url?: string;
  webhook_token?: string;
}

export interface AnalysisDeliveryConfig {
  channels: AnalysisDeliveryChannel[];
}

export interface AnalysisScheduleRequest {
  name: string;
  tables: string[];
  depth?: 'quick' | 'standard' | 'deep';
  resource_name?: string;
  schedule_type: 'hourly' | 'daily' | 'weekly' | 'monthly';
  schedule_hour?: number;
  schedule_minute?: number;
  schedule_day_of_week?: number;
  schedule_day_of_month?: number;
  timezone?: string;
  delivery?: AnalysisDeliveryConfig;
  enabled?: boolean;
}

export interface AnalysisScheduleUpdateRequest {
  name?: string;
  tables?: string[];
  depth?: 'quick' | 'standard' | 'deep';
  resource_name?: string;
  schedule_type?: 'hourly' | 'daily' | 'weekly' | 'monthly';
  schedule_hour?: number;
  schedule_minute?: number;
  schedule_day_of_week?: number;
  schedule_day_of_month?: number;
  timezone?: string;
  delivery?: AnalysisDeliveryConfig;
  enabled?: boolean;
}

export interface AnalysisSchedule {
  id: string;
  name: string;
  tables: string[];
  depth: 'quick' | 'standard' | 'deep';
  resource_name?: string;
  schedule_type: 'hourly' | 'daily' | 'weekly' | 'monthly';
  schedule_hour: number;
  schedule_minute: number;
  schedule_day_of_week: number;
  schedule_day_of_month: number;
  timezone: string;
  delivery?: AnalysisDeliveryConfig;
  enabled: boolean;
  last_run_at?: string | null;
  next_run_at?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export type UploadImportMode = 'replace' | 'append';

export const dorisApi = {
  // 健康检查
  health: () => api.get('/api/health'),

  // 获取所有表
  getTables: () => api.get('/api/tables'),

  // 获取面向业务语义的查询目录
  queryCatalog: {
    list: () => api.get('/api/query/catalog'),
  },

  // 获取表结构
  getTableSchema: (tableName: string) => api.get(`/api/tables/${tableName}/schema`),

  // 执行操作
  execute: (data: ExecuteRequest) => api.post('/api/execute', data),

  // Excel 上传预览
  previewExcel: (file: File, rows: number = 10) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('rows', rows.toString());
    return api.post('/api/upload/preview', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  // Excel 上传导入
  uploadExcel: (
    file: File,
    tableName: string,
    columnMapping?: Record<string, string>,
    createTable: boolean = true,
    importMode: UploadImportMode = 'replace',
  ) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('table_name', tableName);
    if (columnMapping) {
      formData.append('column_mapping', JSON.stringify(columnMapping));
    }
    formData.append('create_table', createTable.toString());
    formData.append('import_mode', importMode);
    return api.post('/api/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  // LLM 配置管理
  llm: {
    create: (data: LLMConfigRequest) => api.post('/api/llm/config', data),
    list: () => api.get('/api/llm/config'),
    test: (resourceName: string) => api.post(`/api/llm/config/${resourceName}/test`),
    delete: (resourceName: string) => api.delete(`/api/llm/config/${resourceName}`),
  },

  // 自然语言查询 (Text-to-SQL)
  naturalQuery: (data: NaturalQueryRequest) => api.post('/api/query/natural', data),

  // 数据分析
  analysis: {
    analyzeTable: (tableName: string, depth: 'quick' | 'standard' | 'deep' = 'standard', resourceName?: string) =>
      api.post(`/api/analysis/table/${tableName}`, { depth, resource_name: resourceName }),
    replayHistory: (historyId: string, resourceName?: string) =>
      api.post(`/api/analysis/replay/${historyId}`, { resource_name: resourceName }),
    listReports: (params?: { table_names?: string; limit?: number; offset?: number }) =>
      api.get('/api/analysis/reports', { params }),
    getReport: (id: string) => api.get(`/api/analysis/reports/${id}`),
    deleteReport: (id: string) => api.delete(`/api/analysis/reports/${id}`),
    latestReport: (tableName: string) => api.get(`/api/analysis/reports/latest/${tableName}`),
    listSchedules: () => api.get('/api/analysis/schedules'),
    createSchedule: (data: AnalysisScheduleRequest) => api.post('/api/analysis/schedules', data),
    updateSchedule: (id: string, data: AnalysisScheduleUpdateRequest) =>
      api.put(`/api/analysis/schedules/${id}`, data),
    deleteSchedule: (id: string) => api.delete(`/api/analysis/schedules/${id}`),
    toggleSchedule: (id: string) => api.post(`/api/analysis/schedules/${id}/toggle`),
    runNow: (id: string) => api.post(`/api/analysis/schedules/${id}/run`),
  },

  // 数据源管理
  datasource: {
    // 测试连接
    test: (data: DataSourceTestRequest) => api.post('/api/datasource/test', data),
    // 保存数据源
    save: (data: DataSourceSaveRequest) => api.post('/api/datasource', data),
    // 获取所有数据源
    list: () => api.get('/api/datasource'),
    // 删除数据源
    delete: (dsId: string) => api.delete(`/api/datasource/${dsId}`),
    // 获取数据源表列表
    getTables: (dsId: string) => api.get(`/api/datasource/${dsId}/tables`),
    // 预览表数据
    previewTable: (dsId: string, tableName: string, limit: number = 100) =>
      api.get(`/api/datasource/${dsId}/tables/${tableName}/preview?limit=${limit}`),
    // 同步单个表
    syncTable: (dsId: string, data: SyncTableRequest) =>
      api.post(`/api/datasource/${dsId}/sync`, data),
    // 批量同步
    syncMultiple: (dsId: string, tables: SyncTableRequest[]) =>
      api.post(`/api/datasource/${dsId}/sync-multiple`, { tables }),
  },

  // 同步任务管理
  syncTasks: {
    // 创建定时同步任务
    create: (data: ScheduleSyncRequest) => api.post('/api/sync/schedule', data),
    // 更新同步任务
    update: (taskId: string, data: UpdateSyncTaskRequest) => api.put(`/api/sync/tasks/${taskId}`, data),
    // 切换AI启用状态
    toggleAI: (taskId: string, enabled: boolean) =>
      api.put(`/api/sync/tasks/${taskId}/toggle-ai?enabled=${enabled}`),
    // 获取所有任务
    list: () => api.get('/api/sync/tasks'),
    // 获取AI启用的表
    getAIEnabledTables: () => api.get('/api/sync/ai-enabled-tables'),
    // 删除任务
    delete: (taskId: string) => api.delete(`/api/sync/tasks/${taskId}`),
  },

  // 元数据分析
  metadata: {
    // 分析表格
    analyze: (tableName: string, sourceType: string = 'manual') =>
      api.post(`/api/tables/${tableName}/analyze?source_type=${sourceType}`),
    // 获取表格元数据
    get: (tableName: string) => api.get(`/api/tables/${tableName}/metadata`),
    // 获取所有元数据
    list: () => api.get('/api/metadata'),
  },


  // ??????????????????
  tableRegistry: {
    list: () => api.get('/api/table-registry'),
    update: (tableName: string, data: UpdateTableRegistryRequest) =>
      api.put(`/api/table-registry/${tableName}`, data),
    delete: (tableName: string, options?: { dropPhysical?: boolean; cleanupHistory?: boolean }) => {
      const params = new URLSearchParams({
        drop_physical: String(options?.dropPhysical ?? true),
        cleanup_history: String(options?.cleanupHistory ?? true),
      });
      return api.delete(`/api/table-registry/${tableName}?${params.toString()}`);
    },
  },
};
