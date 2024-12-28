import React, { useState } from 'react';
import { Form, Input, Radio, InputNumber, Row, Col } from 'antd';
import { useIntl } from 'react-intl';

// Flink部署模式选项
const getFlinkDeployModes = (intl: any) => [
    { label: intl.formatMessage({ id: 'dv_flink_deploy_mode_local' }), value: 'local' },
    { label: intl.formatMessage({ id: 'dv_flink_deploy_mode_yarn_session' }), value: 'yarn-session' },
    { label: intl.formatMessage({ id: 'dv_flink_deploy_mode_yarn_per_job' }), value: 'yarn-per-job' },
    { label: intl.formatMessage({ id: 'dv_flink_deploy_mode_yarn_application' }), value: 'yarn-application' },
];

interface FlinkConfig {
    deployMode: string;
    taskManagerCount: number;
    taskManagerMemory: string;
    jobManagerMemory: string;
    parallelism: number;
    jobName: string;
    yarnQueue: string;
    others: string;
}

interface FlinkConfigProps {
    onChange?: (config: any) => void;
    initialValues?: any;
    engineType?: string;
}

const FlinkConfig: React.FC<FlinkConfigProps> = ({ onChange, initialValues = {}, engineType = 'flink_single_table' }) => {
    const intl = useIntl();
    const [form] = Form.useForm();
    const [deployMode, setDeployMode] = useState(initialValues.deployMode || 'local');

    const handleValuesChange = (changedValues: any, allValues: any) => {
        if (changedValues.deployMode) {
            setDeployMode(changedValues.deployMode);
        }

        // 构建配置对象
        const config = {
            env: {
                deployMode: allValues.deployMode,
                taskManagerCount: allValues.taskManagerCount,
                taskManagerMemory: allValues.taskManagerMemory + 'G',
                jobManagerMemory: allValues.jobManagerMemory + 'G',
                parallelism: allValues.parallelism,
                jobName: allValues.jobName,
                yarnQueue: allValues.yarnQueue,
                others: allValues.others
            }
        };

        onChange?.(config);
    };

    const deployModes = getFlinkDeployModes(intl);

    return (
        <Form
            form={form}
            layout="vertical"
            initialValues={{
                deployMode: initialValues.deployMode || 'local',
                taskManagerCount: initialValues.taskManagerCount || 2,
                taskManagerMemory: parseInt(initialValues.taskManagerMemory) || 2,
                jobManagerMemory: parseInt(initialValues.jobManagerMemory) || 1,
                parallelism: initialValues.parallelism || 1,
                jobName: initialValues.jobName || '',
                yarnQueue: initialValues.yarnQueue || '',
                others: initialValues.others || '--conf flink.yarn.maxAppAttempts=1'
            }}
            onValuesChange={handleValuesChange}
        >
            <Form.Item
                label={intl.formatMessage({ id: 'dv_deploy_mode' })}
                name="deployMode"
                rules={[{ required: true, message: intl.formatMessage({ id: 'dv_deploy_mode_required' }) }]}
            >
                <Radio.Group>
                    {deployModes.map(mode => (
                        <Radio key={mode.value} value={mode.value}>
                            {mode.label}
                        </Radio>
                    ))}
                </Radio.Group>
            </Form.Item>

            {deployMode !== 'local' && (
                <>
                    <Row gutter={30}>
                        <Col span={12}>
                            <Form.Item
                                label={intl.formatMessage({ id: 'dv_task_manager_count' })}
                                name="taskManagerCount"
                                rules={[{ required: true, message: intl.formatMessage({ id: 'common_required_tip' }) }]}
                            >
                                <InputNumber min={1} />
                            </Form.Item>
                        </Col>
                        <Col span={12}>
                            <Form.Item
                                label={intl.formatMessage({ id: 'dv_taskmanager_memory' })}
                                name="taskManagerMemory"
                                rules={[{ required: true, message: intl.formatMessage({ id: 'common_required_tip' }) }]}
                            >
                                <InputNumber min={1} addonAfter="G" />
                            </Form.Item>
                        </Col>
                        <Col span={12}>
                            <Form.Item
                                label={intl.formatMessage({ id: 'dv_jobmanager_memory' })}
                                name="jobManagerMemory"
                                rules={[{ required: true, message: intl.formatMessage({ id: 'common_required_tip' }) }]}
                            >
                                <InputNumber min={1} addonAfter="G" />
                            </Form.Item>
                        </Col>
                        <Col span={12}>
                            <Form.Item
                                label={intl.formatMessage({ id: 'dv_metric_actuator_parallelism' })}
                                name="parallelism"
                                rules={[{ required: true, message: intl.formatMessage({ id: 'common_required_tip' }) }]}
                            >
                                <InputNumber min={1} />
                            </Form.Item>
                        </Col>
                    </Row>

                    <Form.Item
                        label={intl.formatMessage({ id: 'dv_metric_actuator_job_name' })}
                        name="jobName"
                        rules={[{ required: true, message: intl.formatMessage({ id: 'common_required_tip' }) }]}
                    >
                        <Input />
                    </Form.Item>

                    <Form.Item
                        label={intl.formatMessage({ id: 'dv_metric_actuator_yarn_queue' })}
                        name="yarnQueue"
                        rules={[{ required: true, message: intl.formatMessage({ id: 'common_required_tip' }) }]}
                    >
                        <Input />
                    </Form.Item>

                    <Form.Item
                        label={intl.formatMessage({ id: 'dv_metric_actuator_executor_options' })}
                        name="others"
                        rules={[{ required: true, message: intl.formatMessage({ id: 'common_required_tip' }) }]}
                    >
                        <Input.TextArea rows={3} />
                    </Form.Item>
                </>
            )}
        </Form>
    );
};

export default FlinkConfig;
