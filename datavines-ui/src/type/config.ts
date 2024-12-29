export type TConfigTableItem = {
    id?: any,
    varKey?: string,
    workspaceId?: any;
    updateTime?: string,
    updater?: string,
    varValue?: string,
    type?: 'flink' | 'spark',
    flinkConfig?: any,
}
