import React, { useState } from 'react';
import { Form, Select } from 'antd';
import { useIntl } from 'react-intl';
import FlinkConfig from '../FlinkConfig';

const { Option } = Select;

// 执行引擎选项
const ENGINE_TYPES = [
    { label: 'Flink', value: 'flink_single_table' }
];

interface JobConfigProps {
    onChange?: (config: any) => void;
    initialValues?: any;
}

const JobConfig: React.FC<JobConfigProps> = ({ onChange, initialValues = {} }) => {
    const intl = useIntl();
    const [form] = Form.useForm();
    const [currentEngineType, setCurrentEngineType] = useState(initialValues.engineType || 'flink_single_table');

    const handleEngineTypeChange = (type: string) => {
        setCurrentEngineType(type);
        onChange?.({
            ...initialValues,
            engineType: type,
            engineConfig: {}
        });
    };

    const handleConfigChange = (config: any) => {
        onChange?.({
            ...initialValues,
            engineType: currentEngineType,
            engineConfig: config
        });
    };

    return (
        <div>
            <Form
                form={form}
                layout="vertical"
                initialValues={{
                    engineType: currentEngineType
                }}
            >
                <Form.Item
                    label={intl.formatMessage({ id: 'dv_metric_title_actuator_engine' })}
                    name="engineType"
                    rules={[{ required: true, message: intl.formatMessage({ id: 'common_required_tip' }) }]}
                >
                    <Select onChange={handleEngineTypeChange}>
                        {ENGINE_TYPES.map(engine => (
                            <Option key={engine.value} value={engine.value}>
                                {engine.label}
                            </Option>
                        ))}
                    </Select>
                </Form.Item>
            </Form>

            {currentEngineType === 'flink_single_table' && (
                <FlinkConfig
                    initialValues={initialValues?.engineConfig}
                    onChange={handleConfigChange}
                    engineType={currentEngineType}
                />
            )}
        </div>
    );
};

export default JobConfig;
