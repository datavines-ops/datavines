import {ColumnsType} from "antd/lib/table";
import {TJobsInstanceTableData, TJobsInstanceTableItem} from "@/type/JobsInstance";
import {defaultRender} from "utils/helper";
import React, {Suspense, useState} from "react";
import {useIntl} from "react-intl";
import {SearchForm, Title} from "@/component";
import {Form, Layout, Menu, Spin, Table} from "antd";
import {$http} from "@/http";
import {MenuFoldOutlined, MenuUnfoldOutlined} from "@ant-design/icons";
import AsideMenu,{ MenuItem } from "component/Menu/MenuAside";
import {IF, useWatch} from '@/common';
import {useInstanceResult} from "./useInstanceResult";
import {useInstanceErrorDataModal} from "./useInstanceErrorDataModal";
import {useLogger} from "./useLogger";
import querystring from "querystring";
import { base64Decode } from '@/utils/base64';
import ContentLayout from '@/component/ContentLayout';
import Jobs from "view/Main/HomeDetail/Jobs";

const JobHistoryInstance = () => {
    const intl = useIntl();
    const form = Form.useForm()[0];

    const [loading, setLoading] = useState(false);
    const { Render: RenderErrorDataModal, show: showErrorDataModal } = useInstanceErrorDataModal({});
    const { Render: RenderResultModal, show: showResultModal } = useInstanceResult({});
    const { Render: RenderLoggerModal, show: showLoggerModal } = useLogger({});
    const [decodedText, setDecodedText] = useState('');
    const [tableData, setTableData] = useState<TJobsInstanceTableData>({list: [], total: 0});
    const [pageParams, setPageParams] = useState({
        pageNumber: 1,
        pageSize: 10,
    });

    const getData = async (values: any = null) => {
        try {
            setLoading(true);
            let jobId = querystring.parse(base64Decode(window.location.href.split('?')[1] as string) || '').jobId;
            const res = (await $http.post('/history/job/execution/page', {
                jobId: jobId,
                ...pageParams,
                ...(values || form.getFieldsValue()),
            })) || [];
            console.log(res)
            setTableData({
                list: res?.records || [],
                total: res.total || 0,
            });
        } catch (error) {
            console.log(error)
        } finally {
            setLoading(false);
        }
    };

    useWatch([pageParams], () => {
        getData();
    }, {immediate: true});

    const onSearch = (_values: any) => {
        setPageParams({...pageParams, pageNumber: 1});
        getData();
    };
    const onChange = ({current, pageSize}: any) => {
        setPageParams({
            pageNumber: current,
            pageSize,
        });
        getData();
    };

    const {Header, Content, Sider} = Layout;
    const menus = [{
        path: '/history',
        exact: false,
        key: '/history',
        label: 'history',
        menuHide: true
    }] as MenuItem[];

    const [collapsed, setCollapsed] = useState(true);
    const onCollapse = (bool: boolean) => {
        setCollapsed(bool);
    };

    const columns: ColumnsType<TJobsInstanceTableItem> = [
        {
            title: intl.formatMessage({id: 'jobs_task_name'}),
            dataIndex: 'name',
            key: 'name',
            width: 300,
            render: (text: string) => defaultRender(text, 300),
        },
        {
            title: intl.formatMessage({id: 'jobs_task_schema_name'}),
            dataIndex: 'schemaName',
            key: 'schemaName',
            width: 100,
            render: (text: string) => defaultRender(text, 300),
        },
        {
            title: intl.formatMessage({id: 'jobs_task_table_name'}),
            dataIndex: 'tableName',
            key: 'tableName',
            width: 200,
            render: (text: string) => defaultRender(text, 300),
        },
        {
            title: intl.formatMessage({id: 'jobs_task_column_name'}),
            dataIndex: 'columnName',
            key: 'columnName',
            width: 200,
            render: (text: string) => defaultRender(text, 300),
        },
        {
            title: intl.formatMessage({id: 'jobs_task_metric_type'}),
            dataIndex: 'metricType',
            key: 'metricType',
            width: 200,
            render: (text: string) => defaultRender(text, 300),
        },
        {
            title: intl.formatMessage({id: 'jobs_task_type'}),
            dataIndex: 'jobType',
            key: 'jobType',
            width: 140,
            render: (text: string) => <div>{text}</div>,
        },
        {
            title: intl.formatMessage({id: 'jobs_task_status'}),
            dataIndex: 'status',
            key: 'status',
            width: 140,
            render: (text: string) => <div>{text}</div>,
        },
        {
            title: intl.formatMessage({id: 'jobs_task_check_status'}),
            dataIndex: 'checkState',
            key: 'checkState',
            width: 140,
            render: (text: string) => <div>{text}</div>,
        },
        {
            title: intl.formatMessage({id: 'jobs_task_start_time'}),
            dataIndex: 'startTime',
            key: 'startTime',
            width: 180,
            render: (text: string) => <div>{text || '--'}</div>,
        },
        {
            title: intl.formatMessage({id: 'jobs_task_end_time'}),
            dataIndex: 'endTime',
            key: 'endTime',
            width: 180,
            render: (text: string) => <div>{text || '--'}</div>,
        },
        {
            title: intl.formatMessage({ id: 'common_action' }),
            fixed: 'right',
            key: 'action',
            dataIndex: 'action',
            width: 300,
            render: (text: string, record: TJobsInstanceTableItem) => (
                <>
                    <a style={{ marginRight: 5 }} onClick={() => { onLog(record); }}>{intl.formatMessage({ id: 'jobs_task_log_btn' })}</a>
                    <a style={{ marginRight: 5 }} onClick={() => { onResult(record); }}>{intl.formatMessage({ id: 'jobs_task_result' })}</a>
                    <a style={{ marginRight: 5 }} onClick={() => { onErrorData(record); }}>{intl.formatMessage({ id: 'jobs_task_error_data' })}</a>
                </>
            ),
        },
    ];
    const onLog = (record: TJobsInstanceTableItem) => {
        showLoggerModal(record);
    };
    const onResult = (record: TJobsInstanceTableItem) => {
        showResultModal(record);
    };
    const onErrorData = (record: TJobsInstanceTableItem) => {
        showErrorDataModal(record);
    };
    return (
        <Layout>
            <Layout>
                <Sider
                    style={{
                        height: '100vh', overflow: 'auto', backgroundColor: '#fff',
                    }}
                    trigger={(
                        <div style={{position: 'absolute', right: 15, fontSize: 16}}>
                            {collapsed ? <MenuUnfoldOutlined/> : <MenuFoldOutlined/>}
                        </div>
                    )}
                    collapsed={collapsed}
                    onCollapse={onCollapse}
                >
                    <AsideMenu
                        menus={menus}
                    />
                </Sider>

                <Layout>
                    <ContentLayout>

                        <div style={{
                            height: '100%',
                        }}
                        >
                        <Title>
                            {intl.formatMessage({id: 'job_log_view_log'})}
                        </Title>

                        <div
                                className="dv-page-padding"
                                style={{
                                    padding: '0px 20px 0px 0px',
                                    height: 'auto',
                                }}>
                                {/* <Title isBack children={undefined} /> */}
                                <div>
                                    <div className="dv-flex-between">
                                        <SearchForm form={form} onSearch={onSearch}
                                                    placeholder={intl.formatMessage({id: 'common_search'})}/>
                                    </div>
                                </div>
                                <Table<TJobsInstanceTableItem>
                                    size="middle"
                                    loading={loading}
                                    rowKey="id"
                                    columns={columns}
                                    dataSource={tableData.list || []}
                                    onChange={onChange}
                                    // scroll={{ x: 1500 }}
                                    pagination={{
                                        size: 'small',
                                        total: tableData.total,
                                        showSizeChanger: true,
                                        current: pageParams.pageNumber,
                                        pageSize: pageParams.pageSize,
                                    }}
                                />
                                <RenderLoggerModal/>
                                <RenderErrorDataModal/>
                                <RenderResultModal/>
                            </div>
                        </div>
                    </ContentLayout>
                </Layout>
            </Layout>
        </Layout>
    );
}

export default JobHistoryInstance;