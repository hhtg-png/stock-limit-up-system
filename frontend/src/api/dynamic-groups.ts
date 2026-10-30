import axios from 'axios'
import { groupDefinition } from '@/utils/dynamicGroups'
import type { DynamicCurrent, DynamicDefinition, DynamicDirectory, DynamicField, DynamicGroup, DynamicIntraday, DynamicMemberQuery, DynamicPreview, DynamicSelection, DynamicSelectionDetail, DynamicTemplate, DynamicTrend } from '@/types/dynamic-groups'

export const dynamicGroupsApi = axios.create({ baseURL: '/api/v1/dynamic-groups', timeout: 30000 })
export const getDynamicFields = async () => (await dynamicGroupsApi.get<{ fields: DynamicField[]; templates: DynamicTemplate[] }>('/fields')).data
export const getDynamicGroups = async (params: { q?: string; folder?: string; state?: string; sort?: string; offset?: number; limit?: number }) => (await dynamicGroupsApi.get<DynamicDirectory>('/', { params: { ...params, folder: params.folder === '__unfiled__' ? '' : params.folder || undefined } })).data
export const createDynamicGroup = async (definition: DynamicDefinition) => (await dynamicGroupsApi.post<DynamicGroup>('/', groupDefinition(definition))).data
export const updateDynamicGroup = async (id: number, definition: DynamicDefinition) => (await dynamicGroupsApi.put<DynamicGroup>(`/${id}`, groupDefinition(definition))).data
export const archiveDynamicGroup = async (id: number) => (await dynamicGroupsApi.delete(`/${id}`)).data
export const previewDynamicGroup = async (definition: DynamicDefinition) => (await dynamicGroupsApi.post<DynamicPreview>('/preview', groupDefinition(definition))).data
export const refreshDynamicGroup = async (id: number) => (await dynamicGroupsApi.post(`/${id}/refresh`)).data
export const removeDynamicMember = async (id: number, code: string) => (await dynamicGroupsApi.delete(`/${id}/members/${encodeURIComponent(code)}`)).data
export const getDynamicCurrent = async (id: number, params: DynamicMemberQuery = {}) => (await dynamicGroupsApi.get<DynamicCurrent>(`/${id}/current`, { params: { offset: 0, limit: 50, ...params } })).data
export const getDynamicDates = async (id: number) => (await dynamicGroupsApi.get<{ dates: string[] }>(`/${id}/dates`)).data
export const getDynamicSelections = async (id: number, tradeDate: string, offset = 0) => (await dynamicGroupsApi.get<{ items: DynamicSelection[]; total: number }>(`/${id}/selections`, { params: { trade_date: tradeDate, offset, limit: 100 } })).data
export const getDynamicSelection = async (id: number, selectionId: number, params: DynamicMemberQuery = {}) => (await dynamicGroupsApi.get<DynamicSelectionDetail>(`/${id}/selections/${selectionId}`, { params: { offset: 0, limit: 50, ...params } })).data
export const getDynamicTrend = async (id: number, selectionId: number) => (await dynamicGroupsApi.get<DynamicTrend>(`/${id}/trend`, { params: { selection_id: selectionId, window_days: 0 } })).data
export const getDynamicIntraday = async (id: number, tradeDate?: string) => (await dynamicGroupsApi.get<DynamicIntraday>(`/${id}/intraday`, { params: { trade_date: tradeDate } })).data
