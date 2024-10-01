import React from 'react';
import { Input, InlineFieldRow, InlineField, InlineSwitch } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { MqttDataSourceOptions, MqttQuery } from './types';
import { handlerFactory } from 'handleEvent';

type Props = QueryEditorProps<DataSource, MqttQuery, MqttDataSourceOptions>;

export const QueryEditor = (props: Props) => {
  const { query, onChange, onRunQuery } = props;
  const handleEvent = handlerFactory(query, onChange);

  return (
    <>
      <InlineFieldRow>
        <InlineField label="Topic" labelWidth={8} grow>
          <Input
            name="topic"
            required
            placeholder='e.g. "home/bedroom/temperature"'
            value={query.topic}
            onBlur={onRunQuery}
            onChange={handleEvent('topic')}
          />
        </InlineField>
      </InlineFieldRow>
      <InlineFieldRow>
        <InlineField label="Downsample" labelWidth={11}>
          <InlineSwitch
            value={query.downsample !== undefined ? query.downsample : true}
            onChange={(event) => {
              const flag = event.currentTarget.checked;
              onChange({
                ...query,
                downsample: flag,
              });
              onRunQuery();
            }}
          />
        </InlineField>
      </InlineFieldRow>
    </>
  );
};
