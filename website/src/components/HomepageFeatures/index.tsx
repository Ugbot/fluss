/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import clsx from 'clsx';
import React from 'react';
import styles from './styles.module.css';
import Heading from '@theme/Heading';


type FeatureItem = {
  title: string;
  content: string;
  Svg: React.ComponentType<React.ComponentProps<'svg'>>;
};

const FeatureList: FeatureItem[] = [
    {
        title: 'Sub-Second Latency',
        content:
            'Supports streaming reads and writes with low latency like Kafka. Fluss + Flink can build high-throughput, low-latency streaming data warehouses for real-time applications.',
        Svg: require('@site/static/img/feature_real_time.svg').default
    },
    {
        title: 'Columnar Stream',
        content:
            'Fluss stores streaming data in columnar format. This improves streaming read perfromance to 10x and lowers networking costs by pushdown projections.',
        Svg: require('@site/static/img/feature_column.svg').default
    },
    {
        title: 'Unified Stream/Lakehouse',
        content:
            'Fluss unifies data streaming and data lakehouse by serving streaming data on top of lakehouse. This brings low latency to lakehouse and powerful analytics to streams.',
        Svg: require('@site/static/img/feature_lake.svg').default
    },
    {
        title: 'Real-Time Updates',
        content:
            'Primary-Key Table supports real-time streaming updates of large-scale data. It also supports partial-updates to enrich wide table with lower cost than join operations.',
        Svg: require('@site/static/img/feature_update.svg').default
    },
    {
        title: 'Changelog Tracking',
        content:
            'Updates generate comprehensive changelogs which can be directly consumed by streaming processors in real-time. This simplifies and reduces costs for streaming analytics.',
        Svg: require('@site/static/img/feature_changelog.svg').default
    },
    {
        title: 'Lookup Queries',
        content:
            'Fluss supports ultra-high QPS for point lookup on primary keys. This enables Fluss to serve as dimension tables, working with Flink for high-throughput lookup joins.',
        Svg: require('@site/static/img/feature_lookup.svg').default
    },
    // {
    //     title: 'Interactive Queries',
    //     content:
    //         'Fluss is queryable with query engines like Flink, enabling direct data analytics. This reduces development complexity and simplifies debugging.',
    //     Svg: require('@site/static/img/feature_query.svg').default
    // },
];

function Feature({ title, content, Svg }: FeatureItem) {
    return (
        <div className={clsx('col col--4')}>
            <div className={styles.core_features_icon}>
              <Svg className={styles.featureSvg} role="img" />
            </div>
            <div className={styles.core_features}>
                <div className={styles.core_features_title}>{title}</div>
                <div className={styles.core_features_content}>{content}</div>
            </div>
        </div>
    );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="text--center padding-horiz--md">
          <Heading as="h1">Key Features</Heading>
        </div>
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
