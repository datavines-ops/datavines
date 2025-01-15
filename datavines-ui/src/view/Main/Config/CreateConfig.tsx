import React, { useRef, useState, useImperativeHandle } from 'react';
import {
    Input, ModalProps, Form, FormInstance, message, Select,
} from 'antd';
import { useIntl } from 'react-intl';
import {
    useModal, useImmutable, FormRender, IFormRender, usePersistFn, useLoading,
} from '@/common';
import { $http } from '@/http';
import { useSelector } from '@/store';
import { TConfigTableItem } from "@/type/config";
import { FlinkConfiguration } from './FlinkConfiguration';

type InnerProps = {
    form: FormInstance,
    detail?: TConfigTableItem | null,
    innerRef?: any
}
export const CreateConfigComponent = ({ form, detail, innerRef }: InnerProps) => {
    const intl = useIntl();
    const setBodyLoading = useLoading();
    const { workspaceId } = useSelector((r) => r.workSpaceReducer);
    const schema: IFormRender = {
        name: 'config-form',
        layout: 'vertical',
        formItemProps: {
            style: { marginBottom: 10 },
        },
        meta: [
            {
                label: intl.formatMessage({ id: 'config_var_key' }),
                name: 'varKey',
                initialValue: detail?.varKey,
                rules: [
                    {
                        required: true,
                        message: intl.formatMessage({ id: 'common_required_tip' }),
                    },
                ],
                widget: <Input autoComplete="off" />,
            },
            {
                label: intl.formatMessage({ id: 'config_var_value' }),
                name: 'varValue',
                initialValue: detail?.varValue,
                rules: [
                    {
                        required: true,
                        message: intl.formatMessage({ id: 'common_required_tip' }),
                    },
                ],
                widget: <Input autoComplete="off" />,
            },
            {
                label: intl.formatMessage({ id: 'config_type' }),
                name: 'type',
                initialValue: detail?.type || 'flink',
                rules: [
                    {
                        required: true,
                        message: intl.formatMessage({ id: 'common_required_tip' }),
                    },
                ],
                widget: (
                    <Select>
                        <Select.Option value="flink">Flink</Select.Option>
                        <Select.Option value="spark">Spark</Select.Option>
                    </Select>
                ),
            },
            {
                name: 'flinkConfig',
                shouldUpdate: true,
                noStyle: true,
                children: ({ getFieldValue }: { getFieldValue: (field: string) => string }) => {
                    const type = getFieldValue('type');
                    return type === 'flink' ? <FlinkConfiguration form={form} detail={detail?.flinkConfig} /> : null;
                },
            },
        ],
    };
    useImperativeHandle(innerRef, () => ({
        saveUpdate(hide?: () => any) {
            form.validateFields().then(async (values) => {
                try {
                    setBodyLoading(true);
                    const params = {
                        workspaceId,
                        ...values,
                    };
                    if (detail && detail.id) {
                        await $http.put('/config', { ...params, id: detail.id });
                    } else {
                        await $http.post('/config', params);
                    }
                    message.success(intl.formatMessage({ id: 'common_success' }));
                    if (hide) {
                        hide();
                    }
                } catch (error) {
                } finally {
                    setBodyLoading(false);
                }
            }).catch((err) => {
                console.log(err);
            });
        },
    }));
    return <FormRender {...schema} form={form} />;
};

export const useCreateConfig = (options: ModalProps) => {
    const [form] = Form.useForm();
    const intl = useIntl();
    const innerRef = useRef<any>();
    const [editInfo, setEditInfo] = useState<TConfigTableItem | null>(null);
    const editRef = useRef<TConfigTableItem | null>(null);
    editRef.current = editInfo;

    const onOk = usePersistFn(async () => {
        innerRef.current.saveUpdate(hide);
    });
    const {
        Render, hide, show, ...rest
    } = useModal<any>({
        title: intl.formatMessage({ id: 'create_config' }),
        onOk,
        ...(options || {}),
    });
    return {
        Render: useImmutable(() => (<Render><CreateConfigComponent innerRef={innerRef} form={form} detail={editRef.current} /></Render>)),
        show(data: TConfigTableItem | null) {
            setEditInfo(data);
            show(data);
        },
        ...rest,
    };
};
