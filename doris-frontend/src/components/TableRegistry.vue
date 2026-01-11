<template>
  <div class="table-registry">
    <a-card title="已同步表格">
      <a-row :gutter="16">
        <a-col :span="12">
          <a-input-search
            v-model:value="searchText"
            placeholder="搜索表名/显示名"
            allow-clear
          />
        </a-col>
        <a-col :span="12" style="text-align: right">
          <a-button @click="loadRegistry" :loading="loading">刷新</a-button>
        </a-col>
      </a-row>

      <a-table
        :dataSource="filteredTables"
        :columns="columns"
        :loading="loading"
        :row-key="(record: any) => record.table_name"
        size="small"
        :pagination="{ pageSize: 10 }"
        style="margin-top: 16px"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'display_name'">
            <span>{{ record.display_name || record.table_name }}</span>
          </template>
          <template v-else-if="column.key === 'description'">
            <span>{{ record.description || '-' }}</span>
            <a-tooltip v-if="!record.description && record.auto_description" title="LLM 描述">
              <info-circle-outlined style="margin-left: 6px; color: #999" />
            </a-tooltip>
          </template>
          <template v-else-if="column.key === 'source_type'">
            <a-tag v-if="record.source_type === 'excel'" color="blue">Excel</a-tag>
            <a-tag v-else-if="record.source_type === 'database_sync'" color="green">数据库同步</a-tag>
            <a-tag v-else>未知</a-tag>
          </template>
          <template v-else-if="column.key === 'actions'">
            <a-button type="link" size="small" @click="openEdit(record)">编辑</a-button>
          </template>
        </template>
      </a-table>
    </a-card>

    <a-modal
      v-model:open="editVisible"
      title="编辑表信息"
      @ok="saveEdit"
      :confirm-loading="saving"
    >
      <a-form :model="editForm" layout="vertical">
        <a-form-item label="表名(实际)">
          <a-input v-model:value="editForm.table_name" disabled />
        </a-form-item>
        <a-form-item label="显示名">
          <a-input v-model:value="editForm.display_name" placeholder="用于展示/检索" />
        </a-form-item>
        <a-form-item label="描述">
          <a-textarea v-model:value="editForm.description" :rows="4" placeholder="表格内容描述，便于 AI 理解" />
          <div v-if="editForm.auto_description" class="auto-desc">
            <span>LLM 描述：</span>{{ editForm.auto_description }}
          </div>
        </a-form-item>
      </a-form>
    </a-modal>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue';
import { message } from 'ant-design-vue';
import { InfoCircleOutlined } from '@ant-design/icons-vue';
import { dorisApi } from '../api/doris';

const loading = ref(false);
const saving = ref(false);
const tables = ref<any[]>([]);
const searchText = ref('');

const editVisible = ref(false);
const editForm = ref({
  table_name: '',
  display_name: '',
  description: '',
  auto_description: '',
});

const columns = [
  { title: '表名', dataIndex: 'table_name', key: 'table_name' },
  { title: '显示名', dataIndex: 'display_name', key: 'display_name' },
  { title: '描述', dataIndex: 'description', key: 'description', ellipsis: true },
  { title: '来源', dataIndex: 'source_type', key: 'source_type', width: 120 },
  { title: '分析时间', dataIndex: 'analyzed_at', key: 'analyzed_at', width: 180 },
  { title: '操作', key: 'actions', width: 80 },
];

const filteredTables = computed(() => {
  if (!searchText.value) return tables.value;
  const keyword = searchText.value.toLowerCase();
  return tables.value.filter((item) => {
    const tableName = (item.table_name || '').toLowerCase();
    const displayName = (item.display_name || '').toLowerCase();
    return tableName.includes(keyword) || displayName.includes(keyword);
  });
});

const loadRegistry = async () => {
  loading.value = true;
  try {
    const response = await dorisApi.tableRegistry.list();
    tables.value = response.data.tables || [];
  } catch (error: any) {
    message.error('加载同步表失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    loading.value = false;
  }
};

const openEdit = (record: any) => {
  editForm.value = {
    table_name: record.table_name,
    display_name: record.display_name || '',
    description: record.description || '',
    auto_description: record.auto_description || '',
  };
  editVisible.value = true;
};

const saveEdit = async () => {
  if (!editForm.value.table_name) return;
  saving.value = true;
  try {
    await dorisApi.tableRegistry.update(editForm.value.table_name, {
      display_name: editForm.value.display_name,
      description: editForm.value.description,
    });
    message.success('表信息已更新');
    editVisible.value = false;
    await loadRegistry();
  } catch (error: any) {
    message.error('更新失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    saving.value = false;
  }
};

onMounted(() => {
  loadRegistry();
});
</script>

<style scoped>
.table-registry {
  padding: 24px;
}

.auto-desc {
  margin-top: 8px;
  color: #999;
  font-size: 12px;
}
</style>
