import React from 'react';
import { Form, Input, Select, FormInstance } from 'antd';
import { useIntl } from 'react-intl';
import { FormRender, IFormRender } from '@/common';

const { Option } = Select;

type InnerProps = {
    form: FormInstance,
    detail?: any
}

export const FlinkConfiguration = ({ form, detail }: InnerProps) => {
    const intl = useIntl();

    const schema: IFormRender = {
        name: 'flink-config-form',
        layout: 'vertical',
        formItemProps: {
            style: { marginBottom: 10 },
        },
        meta: [
            {
                label: intl.formatMessage({ id: 'dv_deploy_mode' }),
                name: 'deployMode',
                initialValue: detail?.deployMode,
                rules: [
                    {
                        required: true,
                        message: intl.formatMessage({ id: 'dv_deploy_mode_required' }),
                    },
                ],
                widget: (
                    <Select>
                        <Option value="local">{intl.formatMessage({ id: 'dv_flink_deploy_mode_local' })}</Option>
                        <Option value="yarn_session">{intl.formatMessage({ id: 'dv_flink_deploy_mode_yarn_session' })}</Option>
                        <Option value="yarn_per_job">{intl.formatMessage({ id: 'dv_flink_deploy_mode_yarn_per_job' })}</Option>
                        <Option value="yarn_application">{intl.formatMessage({ id: 'dv_flink_deploy_mode_yarn_application' })}</Option>
                    </Select>
                ),
            },
            {
                label: intl.formatMessage({ id: 'dv_flink_home' }),
                name: 'flinkHome',
                initialValue: detail?.flinkHome,
                rules: [
                    {
                        required: true,
                        message: intl.formatMessage({ id: 'dv_flink_home_required' }),
                    },
                ],
                widget: <Input autoComplete="off" />,
            },
            {
                label: intl.formatMessage({ id: 'dv_jobmanager_memory' }),
                name: 'jobmanagerMemory',
                initialValue: detail?.jobmanagerMemory,
                rules: [
                    {
                        required: true,
                        message: intl.formatMessage({ id: 'dv_jobmanager_memory_required' }),
                    },
                ],
                widget: <Input autoComplete="off" type="number" />,
            },
            {
                label: intl.formatMessage({ id: 'dv_taskmanager_memory' }),
                name: 'taskmanagerMemory',
                initialValue: detail?.taskmanagerMemory,
                rules: [
                    {
                        required: true,
                        message: intl.formatMessage({ id: 'dv_taskmanager_memory_required' }),
                    },
                ],
                widget: <Input autoComplete="off" type="number" />,
            },
        ],
    };

    return <FormRender form={form} {...schema} />;
};

export default FlinkConfiguration;
