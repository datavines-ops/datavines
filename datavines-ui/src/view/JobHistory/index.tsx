/* eslint-disable react/no-danger */
import React, {useRef, useState, useImperativeHandle} from 'react';
import { DownloadOutlined, SyncOutlined} from '@ant-design/icons';
import {usePersistFn, useMount,} from '@/common';
import {useIntl} from 'react-intl';
import {$http} from '@/http';
import {download} from '@/utils';
import querystring from "querystring";
import {base64Decode} from "utils/base64";

const JobHistory = () => {
    const intl = useIntl();
    const innerRef = useRef<any>();
    const dealMsg = (msg: string) => {
        if (msg) {
            return msg.replace(/\r\n/g, '<br>');
        }
        return '';
    };
    const [loading, setLoading] = useState(false);
    const [wholeLog, setWholeLog] = useState<{ offsetLine: number, msg: string }[]>([]);
    let executionId = querystring.parse(base64Decode(window.location.href.split('?')[1] as string) || '').executionId;
    const getData = async (offsetLine: number) => {
        try {
            setLoading(true);
            const res = (await $http.get('history/job/execution/queryLogWithOffsetLine', {
                taskId: executionId,
                offsetLine,
            })) || [];
            res.msg = dealMsg(res.msg);
            if (offsetLine === 0) {
                setWholeLog([res]);
            } else {
                setWholeLog([...wholeLog, res]);
            }
        } catch (error) {
        } finally {
            setLoading(false);
        }
    };

    const onDownload = usePersistFn(async () => {
        try {
            const blob = await $http.get('history/job/execution/download', { taskId: executionId }, {
                responseType: 'blob',
            });
            download(blob);
        } catch (error) {
        }
    });

    useMount(async () => {
        getData(0);
    });
    useImperativeHandle(innerRef, () => ({
        onRefresh() {
            getData(wholeLog[wholeLog.length - 1]?.offsetLine || 0);
        },
    }));
    return (
        <div className={"ant-modal-content"}>
            <div style={{position: 'fixed', padding: "10px 0", top: 0, left: 0, width: '100%', zIndex: 1000, display: 'flex', justifyContent: 'space-between', alignItems: 'center',fontSize:16,fontWeight:600,lineHeight:1.5}}>
                <span style={{marginLeft: 20}}>{intl.formatMessage({id: 'job_log_view_log'})}</span>
                <div style={{marginRight: 30,color:'#1677ff'}}>
                    <a
                        style={{marginRight: 10}}
                        onClick={() => {
                            innerRef.current.onRefresh();
                        }}
                    >
                        <SyncOutlined style={{marginRight: 5}}/>
                        {intl.formatMessage({id: 'job_log_refresh'})}
                    </a>
                    <a style={{marginRight: 10}} onClick={onDownload}>
                        <DownloadOutlined style={{marginRight: 5}}/>
                        {intl.formatMessage({id: 'job_log_download'})}
                    </a>

                </div>
            </div>
            <div style={{minHeight: 300, padding: "60px 20px",lineHeight:1.5,fontSize:14}}>
                {
                    wholeLog.map((item) => (
                        <div dangerouslySetInnerHTML={{__html: item.msg}}/>
                    ))
                }
                <div/>
            </div>
        </div>
    )

}
export default JobHistory;





