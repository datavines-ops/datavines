import React, { useMemo } from 'react';
import { useParams } from 'react-router-dom';
import { DvEditor } from '@Editor/index';
import { useIntl } from 'react-intl';
import { useSelector } from '@/store';
import { usePersistFn } from '@/common';
import { useAddEditJobsModal } from '../Jobs/useAddEditJobsModal';

const EditorData = () => {
    const intl = useIntl();
    const { Render: RenderJobsModal, show: showJobsModal } = useAddEditJobsModal({
        title: intl.formatMessage({ id: 'jobs_tabs_title' }),
        afterClose() {
        },
    });

    const params = useParams<{ id: string}>();
    const { loginInfo } = useSelector((r) => r.userReducer);
    const { workspaceId } = useSelector((r) => r.workSpaceReducer);
    const { locale } = useSelector((r) => r.commonReducer);
    const editorParams = useMemo(() => ({
        workspaceId,
        monacoConfig: { paths: { vs: '/monaco-editor/min/vs' } },
        baseURL: '/api/v1',
        headers: {
            Authorization: `Bearer ${loginInfo.token}`,
        },
    }), [workspaceId]);
    const onShowModal = usePersistFn((data: any) => {
        showJobsModal({
            id: params.id,
            record: data,
        });
    });
    return (
        <div style={{ height: 'calc(100vh - 70px)', background: '#fff' }}>
            <DvEditor {...editorParams} onShowModal={onShowModal} locale={locale} id={params.id} />
            <RenderJobsModal />
        </div>
    );
};

export default EditorData;
