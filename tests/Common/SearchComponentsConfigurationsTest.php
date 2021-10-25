<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\ConfigurationMetadata;
use Keboola\StorageApi\Options\Components\SearchComponentConfigurationsOptions;
use Keboola\Test\ComponentsUtils\ComponentsConfigurationUtils;
use Keboola\Test\StorageApiTestCase;

class SearchComponentsConfigurationsTest extends StorageApiTestCase
{
    const TEST_METADATA = [
        [
            'key' => 'KBC.SomeEnity.metadataKey',
            'value' => 'some-value'
        ],
        [
            'key' => 'someMetadataKey',
            'value' => 'some-value'
        ]
    ];

    use ComponentsConfigurationUtils;

    public function setUp()
    {
        parent::setUp();

        $this->cleanupConfigurations();
        $this->initEvents();
    }

    public function testSearchThrowsErrorWhenIsCalledWithoutBranch()
    {
        try {
            $this->_client->searchComponents((new SearchComponentConfigurationsOptions()));
            $this->fail('should fail, not implemented without branch');
        } catch (ClientException $e) {
            $this->assertContains('Not implemented', $e->getMessage());
            $this->assertSame(501, $e->getCode());
        }
    }

    /**
     * @dataProvider provideBranchAwareComponentsClient
     */
    public function testSearchComponents(callable $getClient)
    {
        $client = $getClient($this);

        $components = new Components($client);

        $configurationNameMain1 = $this->generateUniqueNameForString('main-1');
        $configurationNameMain2 = $this->generateUniqueNameForString('main-2');
        // prepare three configs
        $transformationMain1Options = $this->createConfiguration(
            $components,
            'transformation',
            $configurationNameMain1,
            'Main 1'
        );
        $transformationMain2Options = $this->createConfiguration(
            $components,
            'transformation',
            $configurationNameMain2,
            'Main 2'
        );

        $wrDbMain1Options = $this->createConfiguration(
            $components,
            'wr-db',
            $configurationNameMain2,
            'Main 2'
        );

        // add metadata to all configs
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain2Options))
            ->setMetadata([
                [
                    'key' => 'KBC.SomeEnity.metadataKey',
                    'value' => 'some-value'
                ],
                [
                    'key' => 'transformationMain2Key',
                    'value' => 'some-value'
                ]
            ]);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $configurationMetadataOptions = (new ConfigurationMetadata($wrDbMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $components->addConfigurationMetadata($configurationMetadataOptions);
        //setup end

        // 1. test only componentId set
        $listConfigurationMetadata = $client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setComponentId('transformation'));
        self::assertCount(2, $listConfigurationMetadata);
        $this->assertMetadataHasKeys($listConfigurationMetadata[0]);
        $this->assertSearchResponseEquals([
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain1,
                'metadata' => self::TEST_METADATA,
            ],
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value'
                    ],
                    [
                        'key' => 'transformationMain2Key',
                        'value' => 'some-value'
                    ]
                ],
            ],
        ], $listConfigurationMetadata);

        // 2. test only configurationId set
        $listConfigurationMetadata = $client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setConfigurationId($configurationNameMain2));
        self::assertCount(2, $listConfigurationMetadata);

        $this->assertSearchResponseEquals([
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value'
                    ],
                    [
                        'key' => 'transformationMain2Key',
                        'value' => 'some-value'
                    ]
                ]
            ],
            [
                'idComponent' => 'wr-db',
                'configurationId' => $configurationNameMain2,
                'metadata' => self::TEST_METADATA,
            ],
        ], $listConfigurationMetadata);

        // 3. test only metadataKeys set
        $listConfigurationMetadata = $client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setMetadataKeys(['KBC.SomeEnity.metadataKey']));
        self::assertCount(3, $listConfigurationMetadata);

        $this->assertSearchResponseEquals([
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain1,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value'
                    ]
                ],
            ],
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value'
                    ]
                ]
            ],
            [
                'idComponent' => 'wr-db',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value'
                    ]
                ],
            ],
        ], $listConfigurationMetadata);

        // 4. test multiple metadataKeys sets
        $listConfigurationMetadata = $client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setMetadataKeys(['KBC.SomeEnity.metadataKey', 'transformationMain2Key']));
        self::assertCount(3, $listConfigurationMetadata);
        $this->assertSearchResponseEquals([
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain1,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value'
                    ]
                ],
            ],
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value'
                    ],
                    [
                        'key' => 'transformationMain2Key',
                        'value' => 'some-value'
                    ]
                ],
            ],
            [
                'idComponent' => 'wr-db',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value'
                    ],
                ],
            ],
        ], $listConfigurationMetadata);

        // 5. test with componentId, configurationId, metadataKeys sets returns exactly 1 configuration
        $listConfigurationMetadata = $client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain2)
            ->setMetadataKeys(['transformationMain2Key']));
        self::assertCount(1, $listConfigurationMetadata);

        $this->assertSearchResponseEquals([
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'transformationMain2Key',
                        'value' => 'some-value'
                    ]
                ]
            ]
        ], $listConfigurationMetadata);

        // 6. test return exactly 0 configurations
        $listConfigurationMetadata = $client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setComponentId('wr-db')
            ->setMetadataKeys(['transformationMain2Key']));
        self::assertCount(0, $listConfigurationMetadata);

        // 7. test include
        $listConfigurationMetadata = $client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain2)
            ->setMetadataKeys(['transformationMain2Key'])
            ->setInclude(['']));
        self::assertCount(1, $listConfigurationMetadata);
        self::assertArrayNotHasKey('metadata', $listConfigurationMetadata[0]);
    }

    private function filterIdAndTimestampFromMetadataArray(array $data)
    {
        $result = [];
        foreach ($data as $key => $item) {
            $result[$key] = [
                'idComponent' => $item['idComponent'],
                'configurationId' => $item['configurationId'],
            ];
            foreach ($item['metadata'] as $md) {
                $result[$key]['metadata'][] = [
                    'key' => $md['key'],
                    'value' => $md['value'],
                ];
            }
        }
        return $result;
    }

    /**
     * @dataProvider provideBranchAwareComponentsClient
     */
    public function testSearchComponentsEvent(callable $getClient)
    {
        $client = $getClient($this);

        $components = new Components($client);
        $configurationOptions = $this->createConfiguration(
            $components,
            'wr-db',
            'component-search-metadata-events-test',
            'Component metadata events'
        );
        $configurationMetadataOptions = (new ConfigurationMetadata($configurationOptions))
            ->setMetadata(self::TEST_METADATA);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setConfigurationId('component-search-metadata-events-test')
            ->setMetadataKeys(['KBC.SomeEnity.metadataKey']));

        $events = $this->listEvents($client, 'storage.componentsSearched');
        $event = reset($events);
        self::assertArrayHasKey('event', $event);
        self::assertEquals('storage.componentsSearched', $event['event']);
        self::assertArrayHasKey('message', $event);
        self::assertEquals('Components were searched', $event['message']);
        self::assertArrayHasKey('token', $event);
        self::assertEquals($this->tokenId, $event['token']['id']);
        self::assertArrayHasKey('params', $event);
        self::assertSame(
            [
                'idComponent' => null,
                'configurationId' => 'component-search-metadata-events-test',
                'metadataKeys' => ['KBC.SomeEnity.metadataKey'],
                'include' => ['filteredMetadata'],
            ],
            $event['params']
        );
    }


    private function assertMetadataHasKeys($listConfigurationMetadata)
    {
        self::assertArrayHasKey('idComponent', $listConfigurationMetadata);
        self::assertArrayHasKey('configurationId', $listConfigurationMetadata);
        self::assertArrayHasKey('metadata', $listConfigurationMetadata);
        self::assertArrayHasKey('id', $listConfigurationMetadata['metadata'][0]);
        self::assertArrayHasKey('key', $listConfigurationMetadata['metadata'][0]);
        self::assertArrayHasKey('value', $listConfigurationMetadata['metadata'][0]);
        self::assertArrayHasKey('timestamp', $listConfigurationMetadata['metadata'][0]);
    }

    private function sortResponse($expected, $sortKey)
    {
        $comparsion = function ($attrLeft, $attrRight) use ($sortKey) {
            if ($attrLeft[$sortKey] == $attrRight[$sortKey]) {
                return 0;
            }
            return $attrLeft[$sortKey] < $attrRight[$sortKey] ? -1 : 1;
        };
        return usort($expected, $comparsion);
    }

    private function assertSearchResponseEquals($expected, $actual)
    {
        $filteredListConfigurationMetadata = $this->filterIdAndTimestampFromMetadataArray($actual);
        $this->assertArrayEqualsSorted($expected, $filteredListConfigurationMetadata, 'configurationId');
    }
}
