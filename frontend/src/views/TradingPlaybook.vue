<template>
  <div class="trading-playbook">
    <header class="toolbar panel">
      <div class="toolbar-title">
        <div class="title-line">
          <h3>交易预案</h3>
          <el-tag type="info" effect="plain">北京时间</el-tag>
        </div>
        <p>仅生成预案与提醒，不会自动下单/交易。人工确认只会启用行动提醒。</p>
      </div>
      <div class="toolbar-actions">
        <label>预案目标交易日</label>
        <el-date-picker
          v-model="targetPlanDate"
          type="date"
          value-format="YYYY-MM-DD"
          :clearable="false"
          :disabled="revisionSaving"
          @change="loadPlanDomain"
        />
        <el-button
          type="primary"
          plain
          :loading="refreshing"
          :disabled="reviewSaving || revisionSaving"
          @click="loadAll"
        >
          刷新全部
        </el-button>
      </div>
    </header>

    <el-alert
      v-if="planActionError"
      class="page-alert"
      type="error"
      :title="`预案操作失败：${planActionError}`"
      :closable="false"
      show-icon
    />
    <el-alert
      v-if="store.plansError"
      class="page-alert"
      type="error"
      :title="`预案加载失败：${store.plansError}`"
      :closable="false"
      show-icon
    />
    <el-alert
      v-if="isDegraded"
      class="page-alert"
      type="warning"
      title="数据不完整，当前版本仅供观察"
      :description="qualityDescription"
      :closable="false"
      show-icon
    />

    <div class="overview-grid">
      <section class="panel market-panel" v-loading="store.plansLoading">
        <div class="section-header">
          <div>
            <h4>市场状态</h4>
            <span>当前选中版本的市场判断与数据权限</span>
          </div>
          <el-tag :type="permissionTagType">{{ actionPermissionLabel }}</el-tag>
        </div>
        <el-descriptions v-if="selectedPlan" :column="3" border>
          <el-descriptions-item label="风格">
            {{ marketValueLabel('style') }}
          </el-descriptions-item>
          <el-descriptions-item label="窗口">
            {{ marketValueLabel('window') }}
          </el-descriptions-item>
          <el-descriptions-item label="风险权限">
            {{ riskPermissionSummary(selectedPlan.risk_settings_json) }}
          </el-descriptions-item>
          <el-descriptions-item label="来源交易日">
            {{ selectedPlan.source_trade_date }}
          </el-descriptions-item>
          <el-descriptions-item label="目标交易日">
            {{ selectedPlan.target_trade_date }}
          </el-descriptions-item>
          <el-descriptions-item label="数据时间">
            {{ formatChinaDateTime(selectedPlan.generated_at) }}
          </el-descriptions-item>
        </el-descriptions>
        <el-empty v-else-if="planState === 'empty'" description="该目标交易日暂无预案" />
      </section>

      <section class="panel timeline-panel" v-loading="store.plansLoading">
        <div class="section-header">
          <div>
            <h4>版本时间轴</h4>
            <span>14:40、15:30、08:50、09:26 版本均保留审计记录</span>
          </div>
          <el-tag effect="plain">{{ plans.length }} 个版本</el-tag>
        </div>
        <el-timeline v-if="plans.length" class="plan-timeline">
          <el-timeline-item
            v-for="plan in plans"
            :key="plan.id"
            :timestamp="formatChinaDateTime(plan.generated_at)"
            :type="plan.id === selectedPlan?.id ? 'primary' : undefined"
            :hollow="plan.id !== selectedPlan?.id"
          >
            <button
              class="timeline-version"
              :class="{ selected: plan.id === selectedPlan?.id }"
              type="button"
              :disabled="revisionSaving"
              @click="selectedPlanId = plan.id"
            >
              <strong>{{ stageLabel(plan.stage) }} · v{{ plan.version_no }}</strong>
              <span>{{ statusLabel(plan.status) }}</span>
              <span class="timeline-change">版本变化：{{ readable(plan.change_summary_json) }}</span>
            </button>
          </el-timeline-item>
        </el-timeline>
        <el-empty v-else-if="planState === 'empty'" description="暂无版本记录" />
      </section>
    </div>

    <section class="panel action-panel" v-loading="store.plansLoading">
      <div class="section-header action-header">
        <div>
          <h4>正式行动计划</h4>
          <span>最多三只候选；确认预案只启用提醒，不代表执行交易</span>
        </div>
        <div class="section-actions">
          <el-button
            v-if="selectedPlan?.status === 'draft'"
            plain
            :disabled="Boolean(planActionLoading) || revisionSaving"
            @click="openRevisionDialog"
          >
            修订预案
          </el-button>
          <el-button
            v-if="selectedPlan?.status === 'draft'"
            type="primary"
            :disabled="!canConfirm || Boolean(planActionLoading) || revisionSaving"
            :loading="planActionLoading === 'confirm'"
            @click="confirmSelectedPlan"
          >
            启用行动提醒
          </el-button>
          <el-button
            v-if="canCancel"
            type="danger"
            plain
            :disabled="Boolean(planActionLoading) || revisionSaving"
            :loading="planActionLoading === 'cancel'"
            @click="cancelSelectedPlan"
          >
            取消预案
          </el-button>
        </div>
      </div>
      <div v-if="candidates.length" class="candidate-grid">
        <el-card v-for="item in candidates" :key="item.id" shadow="never" class="candidate-card">
          <template #header>
            <div class="candidate-title">
              <div>
                <h5>{{ item.stock_name }}（{{ item.stock_code }}）</h5>
                <span>{{ item.theme_name || '未标注方向' }} · {{ item.action_trade_date }}</span>
              </div>
              <el-tag :type="candidatePermissionType(item.risk_level)">
                {{ isObservation ? '仅供观察' : riskLabel(item.risk_level) }}
              </el-tag>
            </div>
          </template>
          <div class="candidate-meta">
            <span>主模式：{{ item.primary_mode_key }}</span>
            <span>角色：{{ item.role }}</span>
            <span>排名：{{ item.rank }}</span>
            <span>参考仓位：{{ percent(item.position_reference) }}</span>
          </div>
          <dl class="condition-list">
            <div><dt>触发</dt><dd>{{ readable(item.entry_trigger_json) }}</dd></div>
            <div><dt>失效</dt><dd>{{ readable(item.invalidation_json) }}</dd></div>
            <div><dt>退出</dt><dd>{{ readable(item.exit_trigger_json) }}</dd></div>
            <div><dt>证据</dt><dd>{{ readable(item.evidence_json) }}</dd></div>
          </dl>
        </el-card>
      </div>
      <div v-else-if="!store.plansLoading && selectedPlan" class="no-action-plan" role="status">
        <div class="no-action-title">
          <div>
            <h5>{{ isDegraded ? '仅观察 / 空仓预案' : '观望 / 空仓预案' }}</h5>
            <p>{{ selectedPlan.target_trade_date }} 目标日不新开仓，等待下一次系统确认。</p>
          </div>
          <el-tag type="warning" effect="dark">目标日仓位 0%</el-tag>
        </div>
        <dl class="no-action-details">
          <div>
            <dt>结论</dt>
            <dd>当前没有满足完整触发条件的行动候选，维持观望和空仓。</dd>
          </div>
          <div>
            <dt>原因</dt>
            <dd>{{ noActionReason }}</dd>
          </div>
          <div>
            <dt>禁止动作</dt>
            <dd>不追涨，不临盘增加计划外交易，不把单股异动当成模式确认。</dd>
          </div>
          <div>
            <dt>重新评估</dt>
            <dd>{{ noActionReviewText }}</dd>
          </div>
        </dl>
      </div>
      <el-empty v-else-if="!store.plansLoading" description="该目标交易日暂无预案" />
    </section>

    <section class="panel" v-loading="store.plansLoading">
      <div class="section-header">
        <div>
          <h4>全模式雷达</h4>
          <span>展示全部模式的命中、等待、失效与数据不足状态</span>
        </div>
        <el-tag effect="plain">{{ modeRadarRows.length }} 条</el-tag>
      </div>
      <el-table v-if="modeRadarRows.length" :data="modeRadarRows" stripe>
        <el-table-column prop="mode_name" label="模式" min-width="160">
          <template #default="{ row }">{{ tradingModeLabel(row, ruleModeNames) }}</template>
        </el-table-column>
        <el-table-column prop="status" label="状态" width="110">
          <template #default="{ row }">
            <el-tag :type="radarStatusType(row.status)" effect="plain">
              {{ radarStatusLabel(row.status) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="stock_name" label="候选" min-width="130">
          <template #default="{ row }">{{ radarCandidateLabel(row) }}</template>
        </el-table-column>
        <el-table-column label="证据/原因" min-width="280">
          <template #default="{ row }">{{ radarEvidenceSummary(row) }}</template>
        </el-table-column>
      </el-table>
      <el-empty v-else-if="!store.plansLoading" description="当前版本暂无模式雷达数据" />
    </section>

    <section class="panel inbox-panel" v-loading="store.alertsLoading">
      <div class="section-header">
        <div>
          <h4>独立提醒</h4>
          <span>仅显示交易预案通道，未进入全局涨停播报、语音或桌面通知</span>
        </div>
        <div class="section-actions">
          <el-radio-group v-model="alertFilter" size="small">
            <el-radio-button value="unread">未读（{{ store.unreadCount }}）</el-radio-button>
            <el-radio-button value="all">全部</el-radio-button>
          </el-radio-group>
          <el-button size="small" :loading="store.alertsLoading" @click="loadInbox">刷新</el-button>
        </div>
      </div>
      <el-alert
        v-if="store.alertsError"
        type="error"
        :title="`提醒加载失败：${store.alertsError}`"
        :closable="false"
        show-icon
      />
      <el-table v-if="visibleAlerts.length" :data="visibleAlerts" stripe>
        <el-table-column label="状态" width="88">
          <template #default="{ row }">
            <el-tag :type="row.acknowledged_at ? 'info' : 'warning'">
              {{ row.acknowledged_at ? '已读' : '未读' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="类型" width="130">
          <template #default="{ row }">{{ alertTypeLabel(row.event_type) }}</template>
        </el-table-column>
        <el-table-column prop="message" label="提醒内容" min-width="280" show-overflow-tooltip />
        <el-table-column label="触发时间" width="180">
          <template #default="{ row }">{{ formatChinaDateTime(row.triggered_at) }}</template>
        </el-table-column>
        <el-table-column label="操作" width="90" fixed="right">
          <template #default="{ row }">
            <el-button
              link
              type="primary"
              :disabled="Boolean(row.acknowledged_at) || acknowledgingAlertIds.has(row.id)"
              :loading="acknowledgingAlertIds.has(row.id)"
              @click="acknowledge(row.id)"
            >
              {{ row.acknowledged_at ? '已确认' : '确认已读' }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
      <el-empty v-else-if="!store.alertsLoading" :description="alertFilter === 'unread' ? '暂无未读提醒' : '暂无提醒记录'" />
    </section>

    <section class="panel review-panel" v-loading="store.reviewsLoading || reviewPlanLoading">
      <div class="section-header review-header">
        <div>
          <h4>执行复盘</h4>
          <span>复盘日独立于预案目标日；只记录真实人工执行，不推测账户盈亏</span>
        </div>
        <div class="section-actions">
          <label>复盘交易日</label>
          <el-date-picker
            v-model="reviewDate"
            type="date"
            value-format="YYYY-MM-DD"
            :clearable="false"
            :disabled="reviewSaving"
            @change="loadReviewDomain"
          />
          <el-button size="small" :disabled="reviewSaving" @click="loadReviewDomain">刷新复盘</el-button>
        </div>
      </div>
      <el-alert
        v-if="store.reviewsError || reviewPlanError"
        type="error"
        :title="`复盘加载失败：${store.reviewsError || reviewPlanError}`"
        :closable="false"
        show-icon
      />
      <el-table v-if="reviewRows.length" :data="reviewRows" stripe @row-click="selectReviewRow">
        <el-table-column prop="plan_version_id" label="预案版本" width="110" />
        <el-table-column label="信号复盘" min-width="220">
          <template #default="{ row }">{{ readable(row.signal_review_json) }}</template>
        </el-table-column>
        <el-table-column label="纪律合规" min-width="200">
          <template #default="{ row }">{{ readable(row.plan_compliance_json) }}</template>
        </el-table-column>
        <el-table-column label="结果快照" min-width="200">
          <template #default="{ row }">{{ readable(row.outcome_snapshot_json) }}</template>
        </el-table-column>
        <el-table-column label="数据质量" min-width="160">
          <template #default="{ row }">{{ readable(row.data_quality_json) }}</template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">{{ row.finalized_at ? '已终版' : '待校正' }}</template>
        </el-table-column>
      </el-table>
      <el-empty v-else-if="reviewState === 'empty'" description="该复盘交易日暂无执行复盘" />

      <div v-if="reviewEditorReady" class="execution-editor">
        <div class="editor-toolbar">
          <div>
            <h5>人工执行记录 · 预案 #{{ selectedReviewPlanId }}</h5>
            <span>点击上方复盘行可切换版本；时间按北京时间保存</span>
          </div>
          <el-switch
            v-model="restrictReviewToPlan"
            :disabled="reviewSaving"
            active-text="随请求提交 plan_id"
            inactive-text="按候选自动定位"
          />
        </div>

        <div v-if="reviewPlan?.candidates?.length" class="execution-list">
          <div v-for="candidate in reviewPlan.candidates" :key="candidate.id" class="execution-row">
            <div class="execution-stock">
              <strong>{{ candidate.stock_name }}（{{ candidate.stock_code }}）</strong>
              <span>{{ candidate.primary_mode_key }}</span>
            </div>
            <el-switch
              v-model="plannedDrafts[String(candidate.id)].executed"
              :disabled="reviewSaving"
              active-text="已执行"
              inactive-text="未执行"
            />
            <el-input-number
              v-model="plannedDrafts[String(candidate.id)].execution_price"
              :disabled="reviewSaving || !plannedDrafts[String(candidate.id)].executed"
              :min="0.01"
              :precision="2"
              controls-position="right"
              placeholder="成交价"
            />
            <el-input-number
              v-model="plannedDrafts[String(candidate.id)].quantity"
              :disabled="reviewSaving || !plannedDrafts[String(candidate.id)].executed"
              :min="1"
              :step="100"
              controls-position="right"
              placeholder="数量"
            />
            <el-time-picker
              v-model="plannedDrafts[String(candidate.id)].executed_time"
              :disabled="reviewSaving || !plannedDrafts[String(candidate.id)].executed"
              value-format="HH:mm:ss"
              format="HH:mm:ss"
              placeholder="北京时间"
            />
            <el-input
              v-model="plannedDrafts[String(candidate.id)].manual_note"
              :disabled="reviewSaving"
              placeholder="执行备注（可选）"
            />
          </div>
        </div>
        <el-empty v-else description="该复盘版本没有可编辑的计划内候选" :image-size="64" />

        <div class="unplanned-header">
          <div>
            <h5>计划外执行</h5>
            <span>计划外记录必须结构化填写，不会被包装成计划信号</span>
          </div>
          <el-button size="small" plain :disabled="reviewSaving" @click="addUnplannedExecution">新增计划外记录</el-button>
        </div>
        <div v-for="(item, index) in unplannedDrafts" :key="item.key" class="unplanned-row">
          <el-input v-model="item.stock_code" :disabled="reviewSaving" maxlength="6" placeholder="六位股票代码" />
          <el-input v-model="item.stock_name" :disabled="reviewSaving" placeholder="股票名称" />
          <el-input-number v-model="item.execution_price" :disabled="reviewSaving" :min="0.01" :precision="2" placeholder="成交价" />
          <el-input-number v-model="item.quantity" :disabled="reviewSaving" :min="1" :step="100" placeholder="数量" />
          <el-time-picker v-model="item.executed_time" :disabled="reviewSaving" value-format="HH:mm:ss" format="HH:mm:ss" placeholder="北京时间" />
          <el-input v-model="item.manual_note" :disabled="reviewSaving" placeholder="计划外原因/备注" />
          <el-button link type="danger" :disabled="reviewSaving" @click="removeUnplannedExecution(index)">删除</el-button>
        </div>
        <div class="editor-footer">
          <span v-if="!restrictReviewToPlan">仅在候选可唯一定位复盘时可省略 plan_id。</span>
          <el-button type="primary" :loading="reviewSaving" :disabled="reviewSaving" @click="saveExecutionReview">保存执行记录</el-button>
        </div>
      </div>
    </section>

    <section class="panel settings-panel" v-loading="store.settingsLoading">
      <div class="section-header">
        <div>
          <h4>独立提醒设置</h4>
          <span>只影响交易预案模块，不修改现有涨停播报设置</span>
        </div>
        <el-button type="primary" plain :loading="settingsSaving" :disabled="!store.settings" @click="saveSettings">
          保存设置
        </el-button>
      </div>
      <el-alert
        v-if="store.settingsError || settingsActionError"
        type="error"
        :title="`设置加载或保存失败：${store.settingsError || settingsActionError}`"
        :closable="false"
        show-icon
      />
      <el-form v-if="store.settings" class="settings-form" label-position="top">
        <el-form-item label="交易预案服务">
          <el-switch v-model="settingsDraft.enabled" active-text="启用" inactive-text="停用" />
        </el-form-item>
        <el-form-item label="项目内提醒">
          <el-switch v-model="settingsDraft.in_app_enabled" active-text="启用" inactive-text="停用" />
        </el-form-item>
        <el-form-item label="试错仓位参考（%）">
          <el-input-number v-model="settingsDraft.trial_position_pct" :min="0" :max="100" :precision="1" />
        </el-form-item>
        <el-form-item label="确认仓位参考上限（%）">
          <el-input-number v-model="settingsDraft.confirmed_position_pct" :min="0" :max="100" :precision="1" />
        </el-form-item>
        <el-form-item label="单票刚性止损（%）">
          <el-input-number v-model="settingsDraft.hard_stop_pct" :min="0.1" :max="20" :precision="1" />
        </el-form-item>
        <el-form-item label="正式行动候选上限">
          <el-input-number v-model="settingsDraft.max_action_candidates" :min="1" :max="3" :step="1" :precision="0" />
        </el-form-item>
        <el-form-item label="个人微信提醒">
          <div class="wechat-setting">
            <el-tag
              :type="personalWechatStatus?.enabled ? 'success' : personalWechatStatus?.configured ? 'warning' : 'info'"
            >
              {{ personalWechatStatus?.enabled ? '已启用' : personalWechatStatus?.configured ? '待启用' : '未绑定' }}
            </el-tag>
            <span v-if="personalWechatStatus?.enabled">
              WxPusher {{ personalWechatStatus.recipient_masked }}，将在 {{ personalWechatStatus.schedule.join('、') }} 推送。
            </span>
            <span v-else-if="personalWechatStatus?.configured">
              个人微信凭证已安全配置，请启用服务器推送开关。
            </span>
            <span v-else>
              使用个人微信扫描官方二维码获取 SPT，再由服务器安全配置；网页不会保存或显示完整凭证。
            </span>
            <el-link
              v-if="personalWechatStatus?.setup_qr_url"
              :href="personalWechatStatus.setup_qr_url"
              target="_blank"
              type="primary"
            >
              打开个人微信绑定二维码
            </el-link>
          </div>
          <el-alert
            v-if="personalWechatStatusError"
            type="error"
            :title="`个人微信状态加载失败：${personalWechatStatusError}`"
            :closable="false"
            show-icon
          />
        </el-form-item>
      </el-form>
      <el-empty v-else-if="!store.settingsLoading && !store.settingsError" description="暂无设置数据" />
    </section>

    <section class="panel obsidian-panel" v-loading="store.obsidianStatusLoading">
      <div class="section-header">
        <div>
          <h4>Obsidian 同步</h4>
          <span>只导出、不会从 Obsidian 回写；交易预案仍需要人工确认，也不会自动交易。</span>
        </div>
        <div class="section-actions">
          <el-button
            type="primary"
            plain
            :loading="store.obsidianExporting"
            :disabled="!canExportObsidian || store.obsidianExporting"
            @click="exportObsidian"
          >
            导出到 Obsidian
          </el-button>
          <el-button
            :disabled="!obsidianDashboardUri"
            @click="openObsidianDashboard"
          >
            打开交易预案 Dashboard
          </el-button>
        </div>
      </div>
      <el-alert
        v-if="store.obsidianError"
        class="obsidian-alert"
        type="error"
        :title="`Obsidian 同步失败：${store.obsidianError}`"
        :closable="false"
        show-icon
      />
      <template v-if="store.obsidianStatus">
        <div v-if="store.obsidianVaultStatus" class="obsidian-readiness">
          <el-tag :type="store.obsidianVaultStatus.enabled ? 'success' : 'info'">
            导出服务：{{ store.obsidianVaultStatus.enabled ? '已启用' : '未启用' }}
          </el-tag>
          <el-tag :type="store.obsidianVaultStatus.vault_configured ? 'success' : 'warning'">
            导出配置：{{ store.obsidianVaultStatus.vault_configured ? '就绪' : '未配置' }}
          </el-tag>
          <el-tag :type="store.obsidianVaultStatus.vault_exists ? 'success' : 'warning'">
            Vault：{{ store.obsidianVaultStatus.vault_exists ? '可用' : '不存在' }}
          </el-tag>
        </div>
        <el-descriptions class="obsidian-history" :column="3" border>
          <el-descriptions-item label="上次交易日">
            {{ store.obsidianStatus.last_trade_date || '-' }}
          </el-descriptions-item>
          <el-descriptions-item label="上次阶段">
            {{ store.obsidianStatus.last_phase || '-' }}
          </el-descriptions-item>
          <el-descriptions-item label="上次成功">
            {{ formatChinaDateTime(store.obsidianStatus.last_success_at) }}
          </el-descriptions-item>
        </el-descriptions>
        <div class="obsidian-queue-counts" aria-label="Obsidian 导出队列状态">
          <div>
            <strong>{{ store.obsidianStatus.pending_count }}</strong>
            <span>待重试</span>
          </div>
          <div>
            <strong>{{ store.obsidianStatus.paused_count }}</strong>
            <span>已暂停</span>
          </div>
          <div>
            <strong>{{ store.obsidianStatus.failed_count }}</strong>
            <span>失败</span>
          </div>
        </div>
        <div class="obsidian-files">
          <strong>最近导出文件</strong>
          <ul v-if="store.obsidianStatus.recent_files.length">
            <li v-for="file in store.obsidianStatus.recent_files" :key="file">{{ file }}</li>
          </ul>
          <span v-else>暂无导出记录</span>
        </div>
        <p v-if="!canExportObsidian" class="obsidian-hint">
          导出不可用：请先启用并配置 Obsidian，确认 Vault 已存在。
        </p>
        <p v-if="!obsidianDashboardUri" class="obsidian-hint">
          Dashboard 打开不可用：请检查 Vault 状态与安全的相对 Dashboard 路径。
        </p>
      </template>
      <el-empty
        v-else-if="!store.obsidianStatusLoading && !store.obsidianError"
        description="暂无 Obsidian 同步状态"
      />
    </section>

    <section class="panel rules-panel" v-loading="rulesLoading">
      <div class="section-header">
        <div>
          <h4>规则来源</h4>
          <span>规范化规则保留版本、自动化级别、原文字稿与摘录</span>
        </div>
        <el-button size="small" :loading="rulesLoading" @click="loadRules">刷新规则</el-button>
      </div>
      <el-alert
        v-if="rulesError"
        type="error"
        :title="`规则加载失败：${rulesError}`"
        :closable="false"
        show-icon
      />
      <el-table v-if="rules.length" :data="rules" stripe>
        <el-table-column prop="name" label="模式" min-width="160" />
        <el-table-column prop="mode_key" label="模式键" min-width="180" show-overflow-tooltip />
        <el-table-column label="版本" width="80"><template #default="{ row }">v{{ row.version }}</template></el-table-column>
        <el-table-column prop="family" label="家族" width="110" />
        <el-table-column prop="window" label="窗口" min-width="150" />
        <el-table-column label="自动化" width="110">
          <template #default="{ row }">{{ automationLabel(row.automation_level) }}</template>
        </el-table-column>
        <el-table-column prop="description" label="规范化说明" min-width="260" show-overflow-tooltip />
        <el-table-column label="文字稿依据" min-width="300">
          <template #default="{ row }">
            <div v-if="row.source_refs_json?.length" class="source-refs">
              <div v-for="(source, index) in row.source_refs_json" :key="`${row.id}-${index}`">
                <strong>{{ source.source_key }}</strong>
                <span>{{ source.excerpt }}</span>
              </div>
            </div>
            <span v-else>-</span>
          </template>
        </el-table-column>
      </el-table>
      <el-empty v-else-if="rulesState === 'empty'" description="暂无规则来源数据" />
    </section>

    <el-dialog
      v-model="revisionDialogVisible"
      title="确认前修订"
      width="min(920px, 94vw)"
      :close-on-click-modal="false"
      :close-on-press-escape="!revisionSaving"
      :show-close="!revisionSaving"
    >
      <el-alert
        type="info"
        title="修订会创建可审计的草稿子版本，不会原地覆盖，也不会自动下单/交易。"
        :closable="false"
        show-icon
      />
      <el-alert
        v-if="revisionError"
        class="revision-error"
        type="error"
        :title="revisionError"
        :closable="false"
        show-icon
      />
      <el-form label-position="top" class="revision-form" :disabled="revisionSaving">
        <el-form-item label="修订说明（必填）">
          <el-input
            v-model="revisionChangeNote"
            maxlength="500"
            show-word-limit
            placeholder="说明为什么要调整候选或条件"
          />
        </el-form-item>
        <div v-for="item in revisionDrafts" :key="item.candidate_id" class="revision-candidate">
          <div class="revision-candidate-title">
            <strong>{{ item.stock_name }}（{{ item.stock_code }}）</strong>
            <span>{{ item.primary_mode_key }}</span>
          </div>
          <div class="revision-current">
            <p>当前触发：{{ readable(item.current_entry) }}</p>
            <p>当前失效：{{ readable(item.current_invalidation) }}</p>
            <p>当前退出：{{ readable(item.current_exit) }}</p>
          </div>
          <div class="revision-fields">
            <el-form-item label="行动交易日（留空表示不修改）">
              <el-date-picker
                v-model="item.action_trade_date"
                type="date"
                value-format="YYYY-MM-DD"
                placeholder="不修改"
                clearable
              />
            </el-form-item>
            <el-form-item label="人工备注（留空表示不修改）">
              <el-input v-model="item.manual_note" maxlength="500" placeholder="补充审计说明" />
            </el-form-item>
          </div>
          <el-form-item label="触发条件增量 JSON">
            <el-input
              v-model="item.entry_trigger_text"
              type="textarea"
              :rows="2"
              placeholder='例如 {"label":"突破确认","price_gte":12.3}'
            />
          </el-form-item>
          <el-form-item label="失效条件增量 JSON">
            <el-input
              v-model="item.invalidation_text"
              type="textarea"
              :rows="2"
              placeholder='例如 {"change_pct_lte":-3}'
            />
          </el-form-item>
          <el-form-item label="退出条件增量 JSON">
            <el-input
              v-model="item.exit_trigger_text"
              type="textarea"
              :rows="2"
              placeholder='例如 {"change_pct_lte":-5}'
            />
          </el-form-item>
        </div>
        <p class="revision-help">
          支持 label、reference_price、price_gte、price_lte、change_pct_gte、change_pct_lte、sealed、open_count_gte；未知字段会被拒绝。
        </p>
      </el-form>
      <template #footer>
        <el-button :disabled="revisionSaving" @click="revisionDialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="revisionSaving" :disabled="revisionSaving" @click="submitRevision">创建修订版本</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script lang="ts">
interface TradingDashboardStatus {
  dashboard_openable: boolean
  dashboard_path: string
}

interface IntelligenceVaultStatus {
  enabled: boolean
  vault_configured: boolean
  vault_exists: boolean
  vault_path: string
}

interface ObsidianExportResultSummary {
  written_files: string[]
  skipped_files: string[]
  pending_files: string[]
  failed_files: string[]
  git_status: Record<string, unknown> | null
  error_summary: string | null
}

const OBSIDIAN_CONTROL_CHARACTERS = /[\u0000-\u001f\u007f]/

function safeDashboardPath(path: string) {
  if (
    !path ||
    OBSIDIAN_CONTROL_CHARACTERS.test(path) ||
    path.startsWith('/') ||
    path.startsWith('\\') ||
    /^[a-zA-Z]:/.test(path) ||
    path.includes('\\')
  ) return null
  const segments = path.split('/')
  if (segments.some(segment => !segment || segment === '.' || segment === '..')) return null
  return path
}

function vaultNameFromPath(path: string) {
  if (!path || OBSIDIAN_CONTROL_CHARACTERS.test(path)) return null
  const normalized = path.replace(/[\\/]+$/, '')
  const name = normalized.split(/[\\/]/).at(-1) || ''
  if (!name || name === '.' || name === '..' || /^[a-zA-Z]:$/.test(name)) return null
  return name
}

export function buildObsidianDashboardUri(
  status: TradingDashboardStatus | null | undefined,
  vaultStatus: IntelligenceVaultStatus | null | undefined
) {
  if (
    !status?.dashboard_openable ||
    !vaultStatus?.enabled ||
    !vaultStatus.vault_configured ||
    !vaultStatus.vault_exists
  ) return null
  const vault = vaultNameFromPath(vaultStatus.vault_path)
  const file = safeDashboardPath(status.dashboard_path)
  if (!vault || !file) return null
  return `obsidian://open?vault=${encodeURIComponent(vault)}&file=${encodeURIComponent(file)}`
}

export function describeObsidianExportResult(result: ObsidianExportResultSummary) {
  const counts = [
    `写入 ${result.written_files.length}`,
    `跳过 ${result.skipped_files.length}`,
    `待重试 ${result.pending_files.length}`,
    `失败 ${result.failed_files.length}`
  ].join('，')
  const git = describeObsidianGitStatus(result.git_status)
  const partial = result.pending_files.length > 0 ||
    result.failed_files.length > 0 ||
    Boolean(result.error_summary) ||
    git.warning
  const message = [counts, git.message]
  if (result.error_summary) message.push(`错误摘要：${result.error_summary}`)
  return {
    level: partial ? 'warning' as const : 'success' as const,
    message: message.join('。')
  }
}

function describeObsidianGitStatus(status: Record<string, unknown> | null | undefined) {
  if (!status || typeof status !== 'object' || Array.isArray(status)) {
    return { warning: true, message: 'Git：状态缺失' }
  }
  const state = typeof status.state === 'string' ? status.state.trim() : ''
  if (!state) return { warning: true, message: 'Git：状态缺失' }

  const rawDetail = typeof status.error === 'string' && status.error.trim()
    ? status.error.trim()
    : typeof status.reason === 'string' && status.reason.trim()
      ? status.reason.trim()
      : ''
  const reasonLabels: Record<string, string> = {
    no_written_files: '没有新增文件',
    content_identical: '内容未变化',
    content_changed: '内容已变化，等待提交',
    previous_write_uncertain: '上次写入状态不确定'
  }
  const detail = reasonLabels[rawDetail] || rawDetail
  const withDetail = (message: string) => detail ? `${message}（${detail}）` : message

  if (status.error) {
    return { warning: true, message: withDetail('Git：处理失败') }
  }
  if (state === 'git_complete') {
    if (status.committed === true) return { warning: false, message: 'Git：提交完成' }
    if (status.enabled === false) return { warning: false, message: withDetail('Git：处理完成，自动提交未启用') }
    return { warning: false, message: withDetail('Git：处理完成，未产生新提交') }
  }
  if (state === 'not_attempted') {
    const message = status.enabled === false ? 'Git：未执行，自动提交未启用' : 'Git：未执行'
    return { warning: false, message: withDetail(message) }
  }
  if (state === 'not_needed') {
    return { warning: false, message: withDetail('Git：无需提交') }
  }
  const warningLabels: Record<string, string> = {
    git_error: 'Git：提交失败',
    git_pending: 'Git：待处理',
    git_store_pending: 'Git：状态待保存并重试',
    write_in_progress: 'Git：写入处理中',
    write_failed: 'Git：写入失败',
    lease_claimed: 'Git：任务处理中'
  }
  const warning = warningLabels[state]
  if (warning) return { warning: true, message: withDetail(warning) }
  return { warning: true, message: `Git：未知状态 ${state}` }
}
</script>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import {
  cancelTradingPlan,
  confirmTradingPlan,
  getLatestTradingPlanTargetDate,
  getTradingPlaybookPersonalWechatStatus,
  getTradingPlan,
  getTradingRules,
  reviseTradingPlan,
  updateTradingExecutionReview,
  updateTradingPlaybookSettings
} from '@/api/trading-playbook'
import { useTradingPlaybookStore } from '@/stores/trading-playbook'
import type {
  TradingExecutionReview,
  TradingExecutionReviewUpdate,
  TradingModeRule,
  TradingPlanCandidate,
  TradingPlanStage,
  TradingPlanVersion,
  TradingPlaybookPersonalWechatStatus
} from '@/types/trading-playbook'
import {
  buildManualExecutionUpdate,
  buildSettingsUpdate,
  canEnableActionAlerts,
  chinaToday,
  collectionState,
  filterTradingAlerts,
  formatChinaDateTime,
  isObservationOnly,
  marketStateLabel,
  radarCandidateLabel,
  radarEvidenceSummary,
  radarStatusLabel,
  radarStatusType,
  riskPermissionSummary,
  tradingModeLabel,
  type ManualExecutionDraft,
  type TradingAlertFilter,
  type UnplannedExecutionDraft
} from '@/views/trading-playbook/presentation'
import {
  canEditReview,
  buildPlanRevision,
  createPlanRevisionController,
  createConcurrentIdGuard,
  createPlanMutationController,
  createReviewSaveController,
  createReviewDomainController,
  runAndClearErrorOnSuccess,
  type CandidateRevisionDraft
} from '@/views/trading-playbook/interactions'

interface UnplannedExecutionRow extends UnplannedExecutionDraft {
  key: number
}

interface CandidateRevisionRow extends CandidateRevisionDraft {
  stock_code: string
  stock_name: string
  primary_mode_key: string
}

interface ReviewSaveSnapshot {
  tradeDate: string
  planId?: number
  payload: TradingExecutionReviewUpdate
}

const store = useTradingPlaybookStore()
const targetPlanDate = ref(chinaToday())
const reviewDate = ref(chinaToday())
const selectedPlanId = ref<number | null>(null)
const selectedReviewPlanId = ref<number | null>(null)
const alertFilter = ref<TradingAlertFilter>('unread')
const refreshing = ref(false)

const rules = ref<TradingModeRule[]>([])
const rulesLoading = ref(false)
const rulesError = ref<string | null>(null)
let rulesRequestId = 0

const reviewPlan = ref<TradingPlanVersion | null>(null)
const reviewPlanLoading = ref(false)
const reviewPlanError = ref<string | null>(null)

const planActionLoading = ref<'confirm' | 'cancel' | null>(null)
const planActionError = ref<string | null>(null)
const acknowledgingAlertIds = ref<Set<number>>(new Set())
const reviewSaving = ref(false)
const settingsSaving = ref(false)
const settingsActionError = ref<string | null>(null)
const personalWechatStatus = ref<TradingPlaybookPersonalWechatStatus | null>(null)
const personalWechatStatusError = ref<string | null>(null)
const revisionDialogVisible = ref(false)
const revisionSaving = ref(false)
const revisionError = ref<string | null>(null)
const revisionChangeNote = ref('')
const revisionDrafts = ref<CandidateRevisionRow[]>([])
const restrictReviewToPlan = ref(true)

const plannedDrafts = ref<Record<string, ManualExecutionDraft>>({})
const unplannedDrafts = ref<UnplannedExecutionRow[]>([])
let unplannedKey = 0

const settingsDraft = ref({
  enabled: true,
  in_app_enabled: true,
  trial_position_pct: 10,
  confirmed_position_pct: 30,
  hard_stop_pct: 5,
  max_action_candidates: 3,
  wechat_enabled: false
})

const plans = computed(() => store.plans)
const selectedPlan = computed(() => (
  plans.value.find(item => item.id === selectedPlanId.value) || store.activePlan
))
const candidates = computed(() => (selectedPlan.value?.candidates || []).slice(0, 3))
const canConfirm = computed(() => canEnableActionAlerts(selectedPlan.value))
const canCancel = computed(() => ['draft', 'active'].includes(selectedPlan.value?.status || ''))
const isObservation = computed(() => isObservationOnly(selectedPlan.value))
const isDegraded = computed(() => Boolean(selectedPlan.value) && isObservation.value)
const qualityDescription = computed(() => {
  const quality = selectedPlan.value?.data_quality_json
  const warnings = quality?.warnings || []
  return warnings.length ? warnings.join('；') : `数据质量：${quality?.status || 'missing'}`
})
const actionPermissionLabel = computed(() => {
  if (isObservation.value) return '仅供观察'
  if (selectedPlan.value?.status === 'draft') return '待启用行动提醒'
  if (['confirmed', 'active'].includes(selectedPlan.value?.status || '')) return '行动提醒已启用'
  return '提醒未启用'
})
const permissionTagType = computed(() => (
  isObservation.value ? 'warning' : selectedPlan.value?.status === 'active' ? 'success' : 'info'
))
const modeRadarRows = computed(() => selectedPlan.value?.mode_radar_json || [])
const ruleModeNames = computed(() => Object.fromEntries(
  rules.value
    .filter(rule => Boolean(rule.mode_key && rule.name))
    .map(rule => [rule.mode_key, rule.name])
))
const matchedModeCount = computed(() => modeRadarRows.value.filter(row => row.status === 'matched').length)
const waitingModeCount = computed(() => modeRadarRows.value.filter(row => (
  row.status === 'waiting' || row.status === 'manual_review'
)).length)
const noActionReason = computed(() => {
  if (isDegraded.value) return `数据完整性未通过：${qualityDescription.value}`
  return [
    `市场风格为${marketValueLabel('style')}，当前窗口为${marketValueLabel('window')}`,
    `${modeRadarRows.value.length} 个模式中命中 ${matchedModeCount.value} 个，等待确认或人工复核 ${waitingModeCount.value} 个`
  ].join('；')
})
const noActionReviewText = computed(() => {
  if (selectedPlan.value?.stage === 'auction') {
    return '09:26 竞价确认后仍无命中则继续空仓；只有新版本出现明确候选时才重新评估。'
  }
  if (selectedPlan.value?.stage === 'overnight') {
    return '等待 09:26 竞价确认；只有数据完整且模式命中后才生成候选，否则继续空仓。'
  }
  return '08:50 刷新隔夜信息，09:26 再做竞价确认；只有数据完整且模式命中后才生成候选，否则继续空仓。'
})
const visibleAlerts = computed(() => filterTradingAlerts(store.alerts, alertFilter.value))
const reviewRows = computed(() => store.reviews)
const selectedReview = computed(() => (
  reviewRows.value.find(item => item.plan_version_id === selectedReviewPlanId.value) || null
))
const reviewEditorReady = computed(() => canEditReview(
  selectedReview.value,
  reviewPlan.value,
  reviewPlanLoading.value
))

const planState = computed(() => collectionState(store.plansLoading, store.plansError, plans.value))
const reviewState = computed(() => collectionState(store.reviewsLoading, store.reviewsError, reviewRows.value))
const rulesState = computed(() => collectionState(rulesLoading.value, rulesError.value, rules.value))
const canExportObsidian = computed(() => Boolean(
  store.obsidianVaultStatus?.enabled &&
  store.obsidianVaultStatus.vault_configured &&
  store.obsidianVaultStatus.vault_exists
))
const obsidianDashboardUri = computed(() => buildObsidianDashboardUri(
  store.obsidianStatus,
  store.obsidianVaultStatus
))

const reviewController = createReviewDomainController<TradingExecutionReview, TradingPlanVersion>({
  async loadReviews(tradeDate) {
    await store.loadReviews(tradeDate)
    return [...store.reviews]
  },
  loadPlan: getTradingPlan,
  update(patch) {
    if ('selectedPlanId' in patch) selectedReviewPlanId.value = patch.selectedPlanId ?? null
    if ('reviewPlan' in patch) {
      reviewPlan.value = patch.reviewPlan ?? null
      if (!patch.reviewPlan) {
        plannedDrafts.value = {}
        unplannedDrafts.value = []
      }
    }
    if ('reviewPlanLoading' in patch) reviewPlanLoading.value = Boolean(patch.reviewPlanLoading)
    if ('reviewPlanError' in patch) reviewPlanError.value = patch.reviewPlanError ?? null
  },
  onPlanLoaded(plan, review) {
    hydrateExecutionDrafts(plan, review)
  }
})

const reviewSaveController = createReviewSaveController<ReviewSaveSnapshot>({
  save(snapshot) {
    return updateTradingExecutionReview(
      snapshot.tradeDate,
      snapshot.payload,
      snapshot.planId
    )
  },
  reload(snapshot) {
    return reviewController.load(snapshot.tradeDate)
  },
  success() {
    ElMessage.success('人工执行记录已保存')
  },
  failure(error) {
    ElMessage.error(`执行记录保存失败：${errorMessage(error)}`)
  },
  updatePending(pending) {
    reviewSaving.value = pending
  }
})

const planMutationController = createPlanMutationController({
  confirm: planId => confirmTradingPlan(planId, 'local-user'),
  cancel: cancelTradingPlan,
  reload: loadPlanDomain,
  success(kind) {
    ElMessage.success(
      kind === 'confirm'
        ? '预案已确认，行动级提醒已启用'
        : '预案已取消，相关行动提醒已停用'
    )
  },
  failure(error) {
    planActionError.value = errorMessage(error)
  },
  updatePending(kind) {
    planActionLoading.value = kind
  }
})

const acknowledgeGuard = createConcurrentIdGuard(activeIds => {
  acknowledgingAlertIds.value = new Set(activeIds)
})

const planRevisionController = createPlanRevisionController({
  revise: reviseTradingPlan,
  reload: loadPlanDomain,
  select(planId) {
    selectedPlanId.value = planId
  },
  success() {
    revisionDialogVisible.value = false
    revisionError.value = null
    ElMessage.success('修订子版本已创建，请核对后再启用行动提醒')
  },
  failure(error) {
    revisionError.value = errorMessage(error)
  },
  updatePending(pending) {
    revisionSaving.value = pending
  }
})

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}

function marketValue(key: string) {
  const value = selectedPlan.value?.market_state_json?.[key]
  return typeof value === 'string' || typeof value === 'number' ? String(value) : '-'
}

function marketValueLabel(key: 'style' | 'window') {
  return marketStateLabel(key, marketValue(key))
}

function readable(value: unknown) {
  if (value === null || value === undefined || value === '') return '-'
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value)
  try {
    return JSON.stringify(value, null, 0)
  } catch {
    return '-'
  }
}

function percent(value: number) {
  if (!Number.isFinite(value)) return '-'
  return `${value}%`
}

const stageLabels: Record<TradingPlanStage, string> = {
  preclose: '14:40 提前预案',
  after_close: '15:30 正式预案',
  overnight: '08:50 隔夜刷新',
  auction: '09:26 竞价确认'
}

function stageLabel(stage: TradingPlanStage) {
  return stageLabels[stage] || stage
}

function statusLabel(status: TradingPlanVersion['status']) {
  return ({
    draft: '草稿',
    confirmed: '已确认',
    active: '行动提醒已启用',
    superseded: '已被新版本替代',
    expired: '已过期/取消'
  } as Record<string, string>)[status] || status
}

function riskLabel(risk: TradingPlanCandidate['risk_level']) {
  return ({ avoid: '回避', watch: '观察', trial: '试错', confirmed: '确认' } as Record<string, string>)[risk] || risk
}

function candidatePermissionType(risk: TradingPlanCandidate['risk_level']) {
  if (isObservation.value || risk === 'watch') return 'warning'
  if (risk === 'avoid') return 'danger'
  if (risk === 'confirmed') return 'success'
  return 'info'
}

function alertTypeLabel(type: string) {
  return ({
    plan_ready: '预案就绪',
    confirmation_required: '待人工确认',
    watch: '观察',
    entry_triggered: '入场条件触发',
    confirmation_triggered: '确认条件触发',
    invalidated: '逻辑失效',
    risk_warning: '风险警告',
    exit_triggered: '退出条件触发',
    review_ready: '复盘就绪'
  } as Record<string, string>)[type] || type
}

function automationLabel(level: TradingModeRule['automation_level']) {
  return ({ automatic: '自动匹配', assisted: '辅助判断', manual_only: '仅人工' } as Record<string, string>)[level] || level
}

async function loadPlanDomain() {
  planActionError.value = null
  try {
    await store.loadPlans(targetPlanDate.value)
    if (!plans.value.some(item => item.id === selectedPlanId.value)) {
      selectedPlanId.value = store.activePlan?.id || plans.value[0]?.id || null
    }
  } catch {
    // The store exposes the latest request error without allowing stale responses to win.
  }
}

async function loadInbox() {
  try {
    await store.loadAlerts(false)
  } catch {
    // The canonical store retains WebSocket arrivals and exposes the request error.
  }
}

async function loadRules() {
  const requestId = ++rulesRequestId
  rulesLoading.value = true
  rulesError.value = null
  try {
    const response = await getTradingRules()
    if (requestId !== rulesRequestId) return
    rules.value = response.items
  } catch (error) {
    if (requestId !== rulesRequestId) return
    rules.value = []
    rulesError.value = errorMessage(error)
  } finally {
    if (requestId === rulesRequestId) rulesLoading.value = false
  }
}

async function loadReviewDomain() {
  if (reviewSaving.value) return
  await reviewController.load(reviewDate.value)
}

function executionTime(value: unknown) {
  if (typeof value !== 'string') return undefined
  return value.match(/T(\d{2}:\d{2}:\d{2})/)?.[1]
}

function hydrateExecutionDrafts(plan: TradingPlanVersion, review: TradingExecutionReview | null) {
  const saved = review?.manual_execution_json || {}
  plannedDrafts.value = Object.fromEntries(plan.candidates.map(candidate => {
    const value = saved[String(candidate.id)]
    const item = value && typeof value === 'object' && !Array.isArray(value)
      ? value as Record<string, unknown>
      : {}
    return [String(candidate.id), {
      executed: item.executed === true,
      execution_price: typeof item.execution_price === 'number' ? item.execution_price : undefined,
      quantity: typeof item.quantity === 'number' ? item.quantity : undefined,
      executed_time: executionTime(item.executed_at),
      manual_note: typeof item.manual_note === 'string' ? item.manual_note : ''
    }]
  }))
  const savedUnplanned = Array.isArray(saved._unplanned) ? saved._unplanned : []
  unplannedDrafts.value = savedUnplanned.map(value => {
    const item = value && typeof value === 'object' ? value as Record<string, unknown> : {}
    return {
      key: ++unplannedKey,
      stock_code: typeof item.stock_code === 'string' ? item.stock_code : '',
      stock_name: typeof item.stock_name === 'string' ? item.stock_name : '',
      execution_price: typeof item.execution_price === 'number' ? item.execution_price : undefined,
      quantity: typeof item.quantity === 'number' ? item.quantity : undefined,
      executed_time: executionTime(item.executed_at),
      manual_note: typeof item.manual_note === 'string' ? item.manual_note : ''
    }
  })
}

function selectReviewRow(row: TradingExecutionReview) {
  if (reviewSaving.value) return
  if (selectedReviewPlanId.value === row.plan_version_id) return
  void reviewController.select(row.plan_version_id)
}

function addUnplannedExecution() {
  if (reviewSaving.value) return
  unplannedDrafts.value.push({ key: ++unplannedKey, stock_code: '', stock_name: '' })
}

function removeUnplannedExecution(index: number) {
  if (reviewSaving.value) return
  unplannedDrafts.value.splice(index, 1)
}

async function confirmSelectedPlan() {
  if (planActionLoading.value || revisionSaving.value) return
  const plan = selectedPlan.value
  if (!plan || !canEnableActionAlerts(plan)) return
  planActionError.value = null
  await planMutationController.run('confirm', plan.id)
}

async function cancelSelectedPlan() {
  if (planActionLoading.value || revisionSaving.value) return
  const plan = selectedPlan.value
  if (!plan || !['draft', 'active'].includes(plan.status)) return
  planActionError.value = null
  await planMutationController.run('cancel', plan.id)
}

function openRevisionDialog() {
  const plan = selectedPlan.value
  if (!plan || plan.status !== 'draft' || planActionLoading.value || revisionSaving.value) return
  revisionChangeNote.value = ''
  revisionError.value = null
  revisionDrafts.value = plan.candidates.slice(0, 3).map(candidate => ({
    candidate_id: candidate.id,
    stock_code: candidate.stock_code,
    stock_name: candidate.stock_name,
    primary_mode_key: candidate.primary_mode_key,
    current_entry: candidate.entry_trigger_json,
    current_invalidation: candidate.invalidation_json,
    current_exit: candidate.exit_trigger_json,
    action_trade_date: undefined,
    manual_note: '',
    entry_trigger_text: '',
    invalidation_text: '',
    exit_trigger_text: ''
  }))
  revisionDialogVisible.value = true
}

async function submitRevision() {
  const plan = selectedPlan.value
  if (!plan || plan.status !== 'draft' || revisionSaving.value || planActionLoading.value) return
  revisionError.value = null
  let revision
  try {
    revision = buildPlanRevision(revisionChangeNote.value, revisionDrafts.value, {
      source_trade_date: plan.source_trade_date,
      target_trade_date: plan.target_trade_date
    })
  } catch (error) {
    revisionError.value = errorMessage(error)
    return
  }
  await planRevisionController.run(plan.id, revision)
}

async function acknowledge(alertId: number) {
  await acknowledgeGuard.run(alertId, async () => {
    try {
      await store.acknowledgeAlert(alertId)
    } catch (error) {
      ElMessage.error(`提醒确认失败：${errorMessage(error)}`)
    }
  })
}

async function saveExecutionReview() {
  if (reviewSaving.value || !reviewEditorReady.value) return
  const review = selectedReview.value
  const plan = reviewPlan.value
  if (!review || !plan || !canEditReview(review, plan, reviewPlanLoading.value)) return
  const reviewPlanId = review.plan_version_id
  await reviewSaveController.run(() => {
    const tradeDate = reviewDate.value
    return {
      tradeDate,
      planId: restrictReviewToPlan.value ? reviewPlanId : undefined,
      payload: buildManualExecutionUpdate(
        tradeDate,
        plannedDrafts.value,
        unplannedDrafts.value
      )
    }
  })
}

function syncSettingsDraft() {
  if (!store.settings) return
  settingsDraft.value = {
    enabled: store.settings.enabled,
    in_app_enabled: store.settings.in_app_enabled,
    trial_position_pct: store.settings.trial_position_pct,
    confirmed_position_pct: store.settings.confirmed_position_pct,
    hard_stop_pct: store.settings.hard_stop_pct,
    max_action_candidates: store.settings.max_action_candidates,
    wechat_enabled: false
  }
}

async function loadSettings() {
  try {
    await runAndClearErrorOnSuccess(
      () => store.loadSettings(),
      () => { settingsActionError.value = null }
    )
    syncSettingsDraft()
    return true
  } catch {
    // The store exposes the latest settings error.
    return false
  }
}

async function loadPersonalWechatStatus() {
  personalWechatStatusError.value = null
  try {
    personalWechatStatus.value = await getTradingPlaybookPersonalWechatStatus()
  } catch (error) {
    personalWechatStatus.value = null
    personalWechatStatusError.value = errorMessage(error)
  }
}

async function saveSettings() {
  settingsSaving.value = true
  settingsActionError.value = null
  try {
    const patch = buildSettingsUpdate(settingsDraft.value)
    await updateTradingPlaybookSettings(patch)
    if (!await loadSettings()) {
      throw new Error(store.settingsError || '设置重新加载失败')
    }
    ElMessage.success('独立提醒与风控设置已保存')
  } catch (error) {
    settingsActionError.value = errorMessage(error)
  } finally {
    settingsSaving.value = false
  }
}

async function exportObsidian() {
  if (!canExportObsidian.value || store.obsidianExporting) return
  try {
    const result = await store.exportToObsidian(targetPlanDate.value)
    const feedback = describeObsidianExportResult(result)
    if (feedback.level === 'warning') {
      ElMessage.warning(`Obsidian 导出部分完成：${feedback.message}`)
    } else {
      ElMessage.success(`Obsidian 导出完成：${feedback.message}`)
    }
  } catch (error) {
    ElMessage.error(`导出到 Obsidian 失败：${store.obsidianError || errorMessage(error)}`)
  }
}

function openObsidianDashboard() {
  const uri = obsidianDashboardUri.value
  if (!uri) {
    ElMessage.warning('Dashboard 暂不可打开，请检查 Obsidian 与 Vault 配置。')
    return
  }
  window.location.href = uri
}

async function loadAll() {
  refreshing.value = true
  await Promise.allSettled([
    loadPlanDomain(),
    loadReviewDomain(),
    loadInbox(),
    loadSettings(),
    loadPersonalWechatStatus(),
    loadRules(),
    store.loadObsidianStatus()
  ])
  refreshing.value = false
}

async function selectLatestPlanTargetDate() {
  try {
    const latest = await getLatestTradingPlanTargetDate()
    if (latest.target_trade_date) targetPlanDate.value = latest.target_trade_date
  } catch {
    // Keep Beijing today as a safe fallback when discovery is unavailable.
  }
}

watch(selectedPlan, plan => {
  if (plan && selectedPlanId.value === null) selectedPlanId.value = plan.id
})

onMounted(async () => {
  await selectLatestPlanTargetDate()
  await loadAll()
})
</script>

<style lang="scss" scoped>
.trading-playbook {
  --panel-border: #e5e7eb;
  --muted: #64748b;
  display: grid;
  gap: 16px;
  color: #172033;
}

.panel {
  min-width: 0;
  padding: 18px;
  border: 1px solid var(--panel-border);
  border-radius: 12px;
  background: #fff;
  box-shadow: 0 4px 14px rgba(15, 23, 42, 0.04);
}

.toolbar,
.section-header,
.candidate-title,
.editor-toolbar,
.unplanned-header,
.editor-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}

.toolbar-title,
.section-header > div:first-child,
.editor-toolbar > div,
.unplanned-header > div {
  min-width: 0;
}

.title-line {
  display: flex;
  align-items: center;
  gap: 10px;
}

h3,
h4,
h5,
p {
  margin: 0;
}

h3 {
  font-size: 22px;
}

h4 {
  font-size: 17px;
}

h5 {
  font-size: 15px;
}

.toolbar-title p,
.section-header span,
.editor-toolbar span,
.unplanned-header span,
.editor-footer span,
.candidate-title span,
.execution-stock span {
  display: block;
  margin-top: 5px;
  color: var(--muted);
  font-size: 12px;
  line-height: 1.5;
}

.toolbar-actions,
.section-actions {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.toolbar-actions label,
.section-actions label {
  color: var(--muted);
  font-size: 13px;
  white-space: nowrap;
}

.page-alert {
  border-radius: 10px;
}

.overview-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.45fr) minmax(300px, 0.55fr);
  gap: 16px;
}

.section-header {
  margin-bottom: 16px;
}

.market-panel {
  overflow-x: auto;
}

.plan-timeline {
  max-height: 260px;
  margin: 4px 0 0;
  overflow-y: auto;
}

.timeline-version {
  width: 100%;
  padding: 8px 10px;
  border: 1px solid transparent;
  border-radius: 8px;
  background: transparent;
  color: inherit;
  text-align: left;
  cursor: pointer;

  span {
    display: block;
    margin-top: 4px;
    color: var(--muted);
    font-size: 12px;
  }

  &.selected {
    border-color: #bfdbfe;
    background: #eff6ff;
    color: #1d4ed8;
  }
}

.candidate-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 14px;
}

.candidate-card {
  border-color: #dbe4f0;
}

.no-action-plan {
  padding: 18px;
  border: 1px solid #f5c56b;
  border-radius: 12px;
  background: linear-gradient(135deg, #fffbeb 0%, #fff 100%);
}

.no-action-title {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;

  p {
    margin-top: 6px;
    color: var(--muted);
    font-size: 13px;
  }
}

.no-action-details {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  margin: 16px 0 0;

  div {
    padding: 12px;
    border-radius: 9px;
    background: rgba(255, 255, 255, 0.82);
  }

  dt {
    color: #92400e;
    font-size: 13px;
    font-weight: 700;
  }

  dd {
    margin: 6px 0 0;
    color: #334155;
    font-size: 13px;
    line-height: 1.6;
  }
}

.candidate-title {
  align-items: flex-start;
}

.candidate-meta {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  margin-bottom: 12px;

  span {
    padding: 4px 8px;
    border-radius: 999px;
    background: #f1f5f9;
    color: #475569;
    font-size: 12px;
  }
}

.condition-list {
  display: grid;
  gap: 9px;
  margin: 0;

  div {
    display: grid;
    grid-template-columns: 42px minmax(0, 1fr);
    gap: 8px;
  }

  dt {
    color: #475569;
    font-weight: 600;
  }

  dd {
    margin: 0;
    color: #334155;
    font-size: 13px;
    line-height: 1.55;
    overflow-wrap: anywhere;
  }
}

.execution-editor {
  margin-top: 18px;
  padding-top: 18px;
  border-top: 1px solid var(--panel-border);
}

.execution-list {
  display: grid;
  gap: 10px;
  margin-top: 14px;
}

.execution-row {
  display: grid;
  grid-template-columns: minmax(170px, 1.2fr) auto minmax(120px, 0.7fr) minmax(120px, 0.7fr) minmax(145px, 0.8fr) minmax(180px, 1fr);
  align-items: center;
  gap: 10px;
  padding: 12px;
  border: 1px solid var(--panel-border);
  border-radius: 10px;
}

.execution-stock {
  min-width: 0;
}

.unplanned-header {
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px dashed #cbd5e1;
}

.unplanned-row {
  display: grid;
  grid-template-columns: 130px 130px 130px 130px 155px minmax(180px, 1fr) auto;
  gap: 10px;
  margin-top: 10px;
  align-items: center;
}

.editor-footer {
  justify-content: flex-end;
  margin-top: 16px;
}

.settings-form {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0 22px;
}

.obsidian-alert {
  margin-bottom: 12px;
}

.obsidian-readiness {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 12px;
}

.obsidian-history {
  margin-bottom: 12px;
}

.obsidian-queue-counts {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;

  div {
    display: flex;
    align-items: baseline;
    gap: 8px;
    padding: 10px 12px;
    border-radius: 8px;
    background: #f8fafc;
  }

  strong {
    font-size: 20px;
  }

  span {
    color: var(--muted);
    font-size: 12px;
  }
}

.obsidian-files {
  display: grid;
  gap: 6px;
  color: #334155;
  font-size: 12px;

  ul {
    display: grid;
    gap: 4px;
    margin: 0;
    padding-left: 18px;
  }

  li,
  span {
    color: var(--muted);
    overflow-wrap: anywhere;
  }
}

.obsidian-hint {
  margin-top: 9px;
  color: #a16207;
  font-size: 12px;
  line-height: 1.5;
}

.wechat-setting {
  display: flex;
  align-items: center;
  gap: 10px;

  span {
    color: var(--muted);
    font-size: 12px;
  }
}

.source-refs {
  display: grid;
  gap: 7px;

  div {
    display: grid;
    gap: 2px;
  }

  strong {
    color: #334155;
    font-size: 12px;
  }

  span {
    color: var(--muted);
    font-size: 12px;
    line-height: 1.45;
  }
}

.revision-error {
  margin-top: 12px;
}

.revision-form {
  margin-top: 16px;
}

.revision-candidate {
  margin-top: 14px;
  padding: 14px;
  border: 1px solid var(--panel-border);
  border-radius: 10px;
  background: #f8fafc;
}

.revision-candidate-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;

  span {
    color: var(--muted);
    font-size: 12px;
  }
}

.revision-current {
  display: grid;
  gap: 5px;
  margin: 10px 0 14px;

  p {
    color: #475569;
    font-size: 12px;
    line-height: 1.5;
    overflow-wrap: anywhere;
  }
}

.revision-fields {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;
}

.revision-help {
  margin-top: 12px;
  color: var(--muted);
  font-size: 12px;
  line-height: 1.55;
}

:deep(.el-table) {
  --el-table-header-bg-color: #f8fafc;
}

@media (max-width: 1180px) {
  .overview-grid,
  .candidate-grid {
    grid-template-columns: 1fr;
  }

  .execution-row,
  .unplanned-row {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .settings-form {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .revision-fields {
    grid-template-columns: minmax(0, 1fr);
  }
}

@media (max-width: 767px) {
  .trading-playbook {
    gap: 10px;
  }

  .panel {
    padding: 13px;
    border-radius: 9px;
  }

  .toolbar,
  .section-header,
  .editor-toolbar,
  .unplanned-header,
  .editor-footer {
    align-items: stretch;
    flex-direction: column;
  }

  .toolbar-actions,
  .section-actions {
    width: 100%;
  }

  .toolbar-actions :deep(.el-date-editor),
  .review-header :deep(.el-date-editor) {
    flex: 1;
    width: auto;
    min-width: 150px;
  }

  .action-header .section-actions :deep(.el-button) {
    flex: 1;
  }

  .overview-grid {
    grid-template-columns: minmax(0, 1fr);
    gap: 10px;
  }

  .market-panel :deep(.el-descriptions__body) {
    min-width: 620px;
  }

  .execution-row,
  .unplanned-row,
  .settings-form,
  .no-action-details {
    grid-template-columns: minmax(0, 1fr);
  }

  .no-action-title {
    flex-direction: column;
  }

  .inbox-panel,
  .review-panel,
  .rules-panel {
    overflow-x: auto;
  }

  .wechat-setting {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>
