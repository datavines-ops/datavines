import React, { useRef, useImperativeHandle, useState } from 'react';
import {
    ModalProps, Form, message, Button,
} from 'antd';
import {
    useModal, useImmutable, usePersistFn, FormRender, IFormRender, CustomSelect, useMount,
} from 'src/common';
import { useIntl } from 'react-intl';
import { useSelector } from '@/store';
import { $http } from '@/http';
import PageContainer from '../useAddEditJobsModal/PageContainer';

type InnerProps = {
    innerRef: any
    jobId?: any
    id?: any
}

const SelectSLAs = ({ innerRef, jobId, id }: InnerProps) => {
    const intl = useIntl();
    const [form] = Form.useForm();
    const { workspaceId } = useSelector((r) => r.workSpaceReducer);
    const [source, setSource] = useState([]);
    useImperativeHandle(innerRef, () => ({
        onSaveUpdate() {
            form.validateFields().then(async (values) => {
                try {
                    const params = {
                        jobId,
                        workspaceId,
                        ...values,
                    };
                    if (id) {
                        await $http.put('/sla/job', { ...params, id });
                    } else {
                        await $http.post('/sla/job', params);
                    }
                    message.success(intl.formatMessage({ id: 'common_success' }));
                } catch (error) {
                    console.log(error);
                }
            }).catch(() => {});
        },
    }));
    useMount(async () => {
        try {
            const params = {
                workspaceId,
                pageNumber: 1,
                pageSize: 99999,
            };
            const res = (await $http.get('/sla/page', params)) || {};
            setSource(res?.records || []);
        } catch (error) {
        }
    });
    const schema: IFormRender = {
        name: 'select-sla',
        layout: 'inline',
        meta: [
            {
                label: intl.formatMessage({ id: 'sla_select' }),
                name: 'slaId',
                rules: [
                    {
                        required: true,
                        message: intl.formatMessage({ id: 'common_required' }),
                    },
                ],
                initialValue: id || undefined,
                widget: <CustomSelect source={source} style={{ width: 300 }} sourceValueMap="id" sourceLabelMap="name" />,
            },
        ],
    };
    return <FormRender {...schema} form={form} />;
};

export const SelectSLAsComponent = ({ jobId, id }: { jobId: any, id: any}) => {
    const innerRef = useRef<any>();
    const onSave = () => {
        innerRef.current.onSaveUpdate();
    };
    return (
        <PageContainer
            footer={<Button type="primary" onClick={() => onSave()}>保存</Button>}
        >
            <div style={{ width: 'calc(100vw - 80px)' }}>
                <SelectSLAs jobId={jobId} id={id} innerRef={innerRef} />
            </div>
        </PageContainer>
    );
};

export const useSelectSLAsModal = (options: ModalProps) => {
    const innerRef = useRef<any>();
    const [detail, setDetail] = useState();
    const detailRef = useRef<any>();
    detailRef.current = detail;
    const onOk = usePersistFn(() => {
        innerRef.current.onSaveUpdate();
    });
    const { Render, show, ...rest } = useModal<any>({
        title: 'SLAs',
        width: 640,
        ...(options || {}),
        onOk,
    });
    return {
        Render: useImmutable(() => (<Render><SelectSLAs jobId={detailRef.current?.id} innerRef={innerRef} /></Render>)),
        show(record: any) {
            setDetail(record);
            show(null);
        },
        ...rest,
    };
};
