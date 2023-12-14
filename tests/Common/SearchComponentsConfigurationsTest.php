<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\ConfigurationMetadata;
use Keboola\StorageApi\Options\Components\SearchComponentConfigurationsOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\Utils\ComponentsConfigurationUtils;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\EventTesterUtils;

class SearchComponentsConfigurationsTest extends StorageApiTestCase
{
    use ComponentsConfigurationUtils;
    use EventTesterUtils;

    const TEST_METADATA = [
        [
            'key' => 'KBC.SomeEnity.metadataKey',
            'value' => 'some-value',
        ],
        [
            'key' => 'someMetadataKey',
            'value' => 'some-value',
        ],
    ];

    /**
     * @var BranchAwareClient
     */
    private $client;

    public function setUp(): void
    {
        parent::setUp();

        $this->cleanupConfigurations($this->_client);

        $clientProvider = new ClientProvider($this);
        $this->client = $clientProvider->createBranchAwareClientForCurrentTest();

        $this->initEvents($this->client);
    }

    public function testSearchThrowsErrorWhenIsCalledWithoutBranch(): void
    {
        try {
            $this->_client->searchComponents((new SearchComponentConfigurationsOptions()));
            $this->fail('should fail, not implemented without branch');
        } catch (ClientException $e) {
            $this->assertStringContainsString('Not implemented', $e->getMessage());
            $this->assertSame(501, $e->getCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testSearchComponents(): void
    {
        $components = new Components($this->client);

        $configurationNameMain1 = $this->generateUniqueNameForString('main-1');
        $configurationNameMain2 = $this->generateUniqueNameForString('main-2');
        $configurationNameDeleted = $this->generateUniqueNameForString('deleted');
        // prepare three configs
        $transformationMain1Options = $this->createConfiguration(
            $components,
            'transformation',
            $configurationNameMain1,
            'Main 1',
        );
        $transformationMain2Options = $this->createConfiguration(
            $components,
            'transformation',
            $configurationNameMain2,
            'Main 2',
        );
        $transformationDeletedOptions = $this->createConfiguration(
            $components,
            'transformation',
            $configurationNameDeleted,
            'Deleted',
        );

        $wrDbMain1Options = $this->createConfiguration(
            $components,
            'wr-db',
            $configurationNameMain2,
            'Main 2',
        );

        // add metadata to all configs
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain2Options))
            ->setMetadata([
                [
                    'key' => 'KBC.SomeEnity.metadataKey',
                    'value' => 'some-value',
                ],
                [
                    'key' => 'transformationMain2Key',
                    'value' => 'some-value',
                ],
            ]);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $configurationMetadataOptions = (new ConfigurationMetadata($transformationDeletedOptions))
            ->setMetadata([
                [
                    'key' => 'KBC.SomeEnity.metadataKey',
                    'value' => 'some-value',
                ],
                [
                    'key' => 'transformationMain2Key',
                    'value' => 'some-value',
                ],
            ]);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $configurationMetadataOptions = (new ConfigurationMetadata($wrDbMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $components->deleteConfiguration($transformationDeletedOptions->getComponentId(), $transformationDeletedOptions->getConfigurationId());
        //setup end

        // 1. test only componentId set
        $listConfigurationMetadata = $this->client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setComponentId('transformation')
            ->setInclude(['filteredMetadata']));
        self::assertNotCount(3, $listConfigurationMetadata, 'Deleted configuration should not be included');
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
                        'value' => 'some-value',
                    ],
                    [
                        'key' => 'transformationMain2Key',
                        'value' => 'some-value',
                    ],
                ],
            ],
        ], $listConfigurationMetadata);

        // 2. test only configurationId set
        $listConfigurationMetadata = $this->client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setConfigurationId($configurationNameMain2)
            ->setInclude(['filteredMetadata']));
        self::assertCount(2, $listConfigurationMetadata);

        $this->assertSearchResponseEquals([
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value',
                    ],
                    [
                        'key' => 'transformationMain2Key',
                        'value' => 'some-value',
                    ],
                ],
            ],
            [
                'idComponent' => 'wr-db',
                'configurationId' => $configurationNameMain2,
                'metadata' => self::TEST_METADATA,
            ],
        ], $listConfigurationMetadata);

        // 3. test only metadataKeys set
        $listConfigurationMetadata = $this->client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setMetadataKeys(['KBC.SomeEnity.metadataKey'])
            ->setInclude(['filteredMetadata']));
        self::assertCount(3, $listConfigurationMetadata);

        $this->assertSearchResponseEquals([
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain1,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value',
                    ],
                ],
            ],
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value',
                    ],
                ],
            ],
            [
                'idComponent' => 'wr-db',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value',
                    ],
                ],
            ],
        ], $listConfigurationMetadata);

        // 4. test multiple metadataKeys sets
        $listConfigurationMetadata = $this->client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setMetadataKeys(['KBC.SomeEnity.metadataKey', 'transformationMain2Key'])
            ->setInclude(['filteredMetadata']));
        self::assertCount(3, $listConfigurationMetadata);
        $this->assertSearchResponseEquals([
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain1,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value',
                    ],
                ],
            ],
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value',
                    ],
                    [
                        'key' => 'transformationMain2Key',
                        'value' => 'some-value',
                    ],
                ],
            ],
            [
                'idComponent' => 'wr-db',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'KBC.SomeEnity.metadataKey',
                        'value' => 'some-value',
                    ],
                ],
            ],
        ], $listConfigurationMetadata);

        // 5. test with componentId, configurationId, metadataKeys sets returns exactly 1 configuration
        $listConfigurationMetadata = $this->client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain2)
            ->setMetadataKeys(['transformationMain2Key'])
            ->setInclude(['filteredMetadata']));
        self::assertCount(1, $listConfigurationMetadata);

        $this->assertSearchResponseEquals([
            [
                'idComponent' => 'transformation',
                'configurationId' => $configurationNameMain2,
                'metadata' => [
                    [
                        'key' => 'transformationMain2Key',
                        'value' => 'some-value',
                    ],
                ],
            ],
        ], $listConfigurationMetadata);

        // 6. test return exactly 0 configurations
        $listConfigurationMetadata = $this->client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setComponentId('wr-db')
            ->setMetadataKeys(['transformationMain2Key'])
            ->setInclude(['filteredMetadata']));
        self::assertCount(0, $listConfigurationMetadata);

        // 7. test include
        $listConfigurationMetadata = $this->client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain2)
            ->setMetadataKeys(['transformationMain2Key'])
            ->setInclude(['']));
        self::assertCount(1, $listConfigurationMetadata);
        self::assertArrayNotHasKey('metadata', $listConfigurationMetadata[0]);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testSearchComponentsEvent(): void
    {
        $this->initEvents($this->client);
        $components = new Components($this->client);
        $configurationOptions = $this->createConfiguration(
            $components,
            'wr-db',
            'component-search-metadata-events-test',
            'Component metadata events',
        );
        $configurationMetadataOptions = (new ConfigurationMetadata($configurationOptions))
            ->setMetadata(self::TEST_METADATA);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $this->client->searchComponents((new SearchComponentConfigurationsOptions())
            ->setConfigurationId('component-search-metadata-events-test')
            ->setInclude(['filteredMetadata'])
            ->setMetadataKeys(['KBC.SomeEnity.metadataKey']));

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
            /** @var array $event */
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
                $event['params'],
            );
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentsSearched');
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
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

    private function assertSearchResponseEquals($expected, $actual)
    {
        $filteredListConfigurationMetadata = $this->filterIdAndTimestampFromMetadataArray($actual);
        $this->assertArrayEqualsSorted($expected, $filteredListConfigurationMetadata, 'configurationId');
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
}
