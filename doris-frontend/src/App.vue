<template>
  <a-config-provider :theme="{ token: { colorPrimary: '#1890ff' } }">
    <a-layout style="min-height: 100vh">
      <a-layout-header style="background: #001529; padding: 0">
        <div style="display: flex; align-items: center; padding: 0 24px">
          <h1 style="color: white; margin: 0; font-size: 20px">
            Doris 数据中台
          </h1>
          <a-space style="margin-left: auto">
            <a-badge :status="healthStatus" :text="healthText" />
          </a-space>
        </div>
      </a-layout-header>

      <a-layout>
        <a-layout-sider width="200" style="background: #fff">
          <a-menu
            v-model:selected-keys="selectedKeys"
            mode="inline"
            style="height: 100%; border-right: 0"
          >
            <a-menu-item key="datasource">
              <cloud-sync-outlined />
              <span>数据源同步</span>
            </a-menu-item>
            <a-menu-item key="registry">
              <table-outlined />
              <span>已同步表格</span>
            </a-menu-item>
            <a-menu-item key="upload">
              <upload-outlined />
              <span>Excel 上传</span>
            </a-menu-item>
            <a-menu-item key="llm">
              <api-outlined />
              <span>LLM 配置</span>
            </a-menu-item>
            <a-menu-item key="natural">
              <robot-outlined />
              <span>AI 问答</span>
            </a-menu-item>
            <a-menu-item key="query">
              <search-outlined />
              <span>数据查询</span>
            </a-menu-item>
          </a-menu>
        </a-layout-sider>

        <a-layout-content style="background: #f0f2f5; padding: 24px; min-height: 280px">
          <DataSourceSync v-if="selectedKeys[0] === 'datasource'" />
          <TableRegistry v-if="selectedKeys[0] === 'registry'" />
          <ExcelUpload v-if="selectedKeys[0] === 'upload'" />
          <LLMConfig v-if="selectedKeys[0] === 'llm'" />
          <NaturalQuery v-if="selectedKeys[0] === 'natural'" />
          <DataQuery v-if="selectedKeys[0] === 'query'" />
        </a-layout-content>
      </a-layout>
    </a-layout>
  </a-config-provider>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { UploadOutlined, ApiOutlined, SearchOutlined, RobotOutlined, CloudSyncOutlined, TableOutlined } from '@ant-design/icons-vue';
import ExcelUpload from './components/ExcelUpload.vue';
import LLMConfig from './components/LLMConfig.vue';
import NaturalQuery from './components/NaturalQuery.vue';
import DataQuery from './components/DataQuery.vue';
import DataSourceSync from './components/DataSourceSync.vue';
import TableRegistry from './components/TableRegistry.vue';
import { dorisApi } from './api/doris';

const selectedKeys = ref(['datasource']);
const healthStatus = ref<'success' | 'error' | 'default'>('default');
const healthText = ref('检查中...');

const checkHealth = async () => {
  try {
    const response = await dorisApi.health();
    if (response.data.doris_connected) {
      healthStatus.value = 'success';
      healthText.value = 'Doris 已连接';
    } else {
      healthStatus.value = 'error';
      healthText.value = 'Doris 未连接';
    }
  } catch (error) {
    healthStatus.value = 'error';
    healthText.value = 'API 服务异常';
  }
};

onMounted(() => {
  checkHealth();
  setInterval(checkHealth, 30000);
});
</script>

<style>
#app {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial,
    'Noto Sans', sans-serif;
}
</style>
