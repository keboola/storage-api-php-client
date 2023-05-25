<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationMetadata;
use Keboola\StorageApi\Options\Components\ListConfigurationMetadataOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\Utils\ComponentsConfigurationUtils;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\EventTesterUtils;
use Keboola\Test\Utils\MetadataUtils;

class ConfigurationMetadataTest extends StorageApiTestCase
{
    use ComponentsConfigurationUtils;
    use MetadataUtils;
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
     * @var \Keboola\StorageApi\BranchAwareClient
     */
    private $client;

    public function setUp(): void
    {
        parent::setUp();

        $this->cleanupConfigurations();

        $clientProvider = new ClientProvider($this);
        $this->client = $clientProvider->createBranchAwareClientForCurrentTest();

        $this->initEvents($this->client);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testAddMetadata(): void
    {
        $configurationNameMain1 = $this->generateUniqueNameForString('main-1');
        $configurationNameMain2 = $this->generateUniqueNameForString('main-2');

        $components = new Components($this->client);

        // prepare two configs
        $transformationMain1Options = $this->createConfiguration(
            $components,
            'transformation',
            $configurationNameMain1,
            'Main 1'
        );
        $this->createConfiguration(
            $components,
            'transformation',
            $configurationNameMain2,
            'Main 2'
        );

        // test if both return 0 metadata
        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(0, $listConfigurationMetadata);

        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain2));
        self::assertCount(0, $listConfigurationMetadata);

        //create second configs for other component to test add configuration metadata returns right count of metadata
        $this->createConfiguration(
            $components,
            'wr-db',
            $configurationNameMain1,
            'Main 1'
        );
        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(0, $listConfigurationMetadata);

        // add metadata to first configuration
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $newMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);
        self::assertCount(2, $newMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $newMetadata[0]);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $newMetadata[1]);
        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(2, $listConfigurationMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $listConfigurationMetadata[0]);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $listConfigurationMetadata[1]);

        // test if second configuration has still 0 metadata
        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain2));
        self::assertCount(0, $listConfigurationMetadata);

        // test if second component has still 0 metadata
        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(0, $listConfigurationMetadata);

        // delete configuration to test fail add/list metadata
        $components->deleteConfiguration('transformation', $configurationNameMain1);

        try {
            $components->addConfigurationMetadata($configurationMetadataOptions);
            $this->fail('configuration desn\'t exist');
        } catch (ClientException $e) {
            $this->assertSame(sprintf('Configuration %s not found', $configurationNameMain1), $e->getMessage());
            $this->assertSame(404, $e->getCode());
        }

        try {
            $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
                ->setComponentId('transformation')
                ->setConfigurationId($configurationNameMain1));
            $this->fail('configuration desn\'t exist');
        } catch (ClientException $e) {
            $this->assertSame(sprintf('Configuration %s not found', $configurationNameMain1), $e->getMessage());
            $this->assertSame(404, $e->getCode());
        }

        // test after restore component can add or list metadata
        $components->restoreComponentConfiguration('transformation', $configurationNameMain1);

        // test can list metadata after restore configuration
        $metadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(2, $metadata);

        // test can add metadata after restore configuration
        $afterRestoreOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata([
                [
                    'key' => 'KBC.SomeEnity.afterRestore',
                    'value' => 'new-value',
                ],
            ]);
        $newMetadata = $components->addConfigurationMetadata($afterRestoreOptions);
        self::assertCount(3, $newMetadata);

        $metadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(3, $metadata);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testUpdateMetadata(): void
    {
        $components = new Components($this->client);

        $configurationNameMain1 = $this->generateUniqueNameForString('main-1');

        $transformationMain1Options = $this->createConfiguration(
            $components,
            'transformation',
            $configurationNameMain1,
            'Main 1'
        );

        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(0, $listConfigurationMetadata);

        // add metadata to first configuration
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $components->addConfigurationMetadata($configurationMetadataOptions);
        // add new metadata with same key but different value
        $updatedMetadata = [
            [
                'key' => 'someMetadataKey',
                'value' => 'updated-value',
            ],
        ];
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata($updatedMetadata);
        $newMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);
        self::assertCount(2, $newMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $newMetadata[0]);
        $this->assertMetadataEquals($updatedMetadata[0], $newMetadata[1]);

        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(2, $listConfigurationMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $listConfigurationMetadata[0]);
        $this->assertMetadataEquals($updatedMetadata[0], $listConfigurationMetadata[1]);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testAddMetadataEvent(): void
    {
        $components = new Components($this->client);
        $configurationOptions = $this->createConfiguration(
            $components,
            'wr-db',
            'component-metadata-events-test',
            'Component metadata event'
        );

        $configurationMetadataOptions = (new ConfigurationMetadata($configurationOptions))
            ->setMetadata(self::TEST_METADATA);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $assertCallback = function ($events) use ($configurationOptions) {
            $this->assertCount(1, $events);
            self::assertSame(self::TEST_METADATA, $events[0]['results']['metadata']);
            $this->assertEvent(
                $events[0],
                'storage.componentConfigurationMetadataSet',
                'Component configuration metadata set "Component metadata event" (wr-db)',
                $configurationOptions->getConfigurationId(),
                'Component metadata event',
                'componentConfiguration',
                [
                    'component' => 'wr-db',
                    'configurationId' => $configurationOptions->getConfigurationId(),
                    'name' => 'Component metadata event',
                    'version' => 1,
                ]
            );
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationMetadataSet')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    public function testCreateBranchCopyMetadataToTheDevBranch(): void
    {
        $configurationNameMain1 = $this->generateUniqueNameForString('main-1');

        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);

        $components = new Components($branchClient);
        $transformationMain1Options = $this->createConfiguration(
            $components,
            'transformation',
            $configurationNameMain1,
            'Main 1'
        );

        // add metadata to first configuration
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $newMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);
        self::assertCount(2, $newMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $newMetadata[0]);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $newMetadata[1]);

        // create new devbranch
        $branch = $this->createDevBranchForTestCase($this);
        $branchComponents = new Components($this->getBranchAwareDefaultClient($branch['id']));

        // metadata should be copied from default branch
        $listConfigurationMetadata = $branchComponents->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(2, $listConfigurationMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $listConfigurationMetadata[0]);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $listConfigurationMetadata[1]);

        // if I add metadata to default branch config development branch config shouldn't be affected
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata([
                [
                    'key' => 'createBranchCopyMetadataToTheDevBranch',
                    'value' => 'new value',
                ],
            ]);
        $newMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);
        self::assertCount(3, $newMetadata);

        $listConfigurationMetadata = $branchComponents->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(2, $listConfigurationMetadata);

        // if I add metadata to the development branch config, default config metadata shouldn't be affected
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata([
                [
                    'key' => 'newDevBranchKey',
                    'value' => 'new value',
                ],
            ]);
        $newMetadata = $branchComponents->addConfigurationMetadata($configurationMetadataOptions);
        self::assertCount(3, $newMetadata);

        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(3, $listConfigurationMetadata);

        $keys = [];
        foreach ($listConfigurationMetadata as $metadata) {
            $keys[] = $metadata['key'];
        }

        self::assertNotContains('newDevBranchKey', $keys);
    }

    public function testResetToDefault(): void
    {
        $configurationNameMain1 = $this->generateUniqueNameForString('main-1');
        $defaultBranchId = $this->getDefaultBranchId($this);

        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        // create new configurations in main branch
        $components = new Components($branchClient);
        $transformationMain1Options = (new Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1)
            ->setName('Main 1')
            ->setConfiguration(['test' => 'false']);

        $components->addConfiguration($transformationMain1Options);
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $newMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);
        self::assertCount(2, $newMetadata);

        $branch = $this->createDevBranchForTestCase($this);
        $branchComponents = new Components($this->getBranchAwareDefaultClient($branch['id']));

        $updatedMetadata = [
            [
                'key' => 'someMetadataKey',
                'value' => 'new value',
            ],
        ];

        // update default branch config metadata
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata($updatedMetadata);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $branchComponents->resetToDefault('transformation', $configurationNameMain1);

        // after resetToDefault development branch metadata should have new updated value
        $listConfigurationMetadata = $branchComponents->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(2, $listConfigurationMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $listConfigurationMetadata[0]);
        $this->assertMetadataEquals($updatedMetadata[0], $listConfigurationMetadata[1]);

        // add new metadata to the default branch
        $moreMetadata = [
            [
                'key' => 'KBC.addMetadata.thirdMetadata',
                'value' => 'my awesome next value',
            ],
        ];

        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata($moreMetadata);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        // update development branch config metadata
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata([
                [
                    'key' => 'someMetadataKey',
                    'value' => 'development branch value',
                ],
            ]);
        $branchComponents->addConfigurationMetadata($configurationMetadataOptions);

        $branchComponents->resetToDefault('transformation', $configurationNameMain1);

        $listConfigurationMetadata = $branchComponents->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(3, $listConfigurationMetadata);

        $this->assertMetadataEquals(self::TEST_METADATA[0], $listConfigurationMetadata[0]);
        $this->assertMetadataEquals($updatedMetadata[0], $listConfigurationMetadata[1]);
        $this->assertMetadataEquals($moreMetadata[0], $listConfigurationMetadata[2]);
    }

    public function testConfigMetadataRestrictionsForReadOnlyUser(): void
    {
        $defaultBranchId = $this->getDefaultBranchId($this);

        $guestClient = $this->getBranchAwareClient($defaultBranchId, [
            'token' => STORAGE_API_READ_ONLY_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);

        $configurationNameMain1 = $this->generateUniqueNameForString('main-1');

        // create new configurations in main branch
        $components = new Components($this->_client);
        $transformationMain1Options = (new Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1)
            ->setName('Main 1')
            ->setConfiguration(['test' => 'false']);

        $components->addConfiguration($transformationMain1Options);
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata(self::TEST_METADATA);

        try {
            $components = new Components($guestClient);
            $components->addConfigurationMetadata($configurationMetadataOptions);
            $this->fail('should fail, insufficiently permission');
        } catch (ClientException $e) {
            $this->assertStringContainsString('Configuration manipulation is restricted for your user role', $e->getMessage());
            $this->assertSame(403, $e->getCode());
        }
    }

    public function testDeleteMetadata(): void
    {
        $configurationNameMain1 = $this->generateUniqueNameForString('main-1');

        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        $components = new Components($branchClient);

        $transformationMain1Options = $this->createConfiguration(
            $components,
            'transformation',
            $configurationNameMain1,
            'Main 1'
        );
        $wrDbMain1Options = $this->createConfiguration(
            $components,
            'wr-db',
            $configurationNameMain1,
            'Main 1'
        );

        // add metadata to first configuration
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $newMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);
        self::assertCount(2, $newMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $newMetadata[0]);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $newMetadata[1]);

        $components->deleteConfigurationMetadata('transformation', $configurationNameMain1, $newMetadata[0]['id']);

        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(1, $listConfigurationMetadata);

        // try delete notexisted metadata
        try {
            $components->deleteConfigurationMetadata('transformation', $configurationNameMain1, $newMetadata[0]['id']);
            $this->fail('should fail, metadata does not exist');
        } catch (ClientException $e) {
            $this->assertStringContainsString('Metadata with id ', $e->getMessage());
            $this->assertSame(404, $e->getCode());
        }

        $configurationMetadataOptions = (new ConfigurationMetadata($wrDbMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $wrDbMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);

        // can't delete existing metadata for other component
        try {
            $components->deleteConfigurationMetadata('transformation', $configurationNameMain1, $wrDbMetadata[0]['id']);
            $this->fail('should fail, don\'t have access to the resource');
        } catch (ClientException $e) {
            $this->assertMatchesRegularExpression(
                '/^Metadata with id "[0-9]+" not found for "transformation" configuration '.
                '"[a-z0-9]+\\\main-1" in branch "[0-9]+"$/',
                $e->getMessage()
            );
            $this->assertSame(404, $e->getCode());
        }

        // cannot delete metadata in development branch
        $branch = $this->createDevBranchForTestCase($this);
        $devBranchComponents = new Components($this->getBranchAwareDefaultClient($branch['id']));

        try {
            $devBranchComponents->deleteConfigurationMetadata('transformation', $configurationNameMain1, $newMetadata[1]['id']);
            $this->fail('should fail, not allowed for devBranch');
        } catch (ClientException $e) {
            $this->assertStringContainsString('Delete metadata is not implemented for development branch', $e->getMessage());
            $this->assertSame(501, $e->getCode());
        }

        // if I delete metadata in default, it still exist in development branch
        $components->deleteConfigurationMetadata('transformation', $configurationNameMain1, $newMetadata[1]['id']);

        // does not exist in default branch
        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(0, $listConfigurationMetadata);

        // still exist in development branch
        $listConfigurationMetadata = $devBranchComponents->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(1, $listConfigurationMetadata);

        // can't delete metadata in development branch using default branch client
        try {
            $components->deleteConfigurationMetadata('transformation', $configurationNameMain1, $listConfigurationMetadata[0]['id']);
            $this->fail('should fail, not allowed delete metadata in dev branch using default branch client');
        } catch (ClientException $e) {
            $this->assertMatchesRegularExpression(
                '/^Metadata with id "[0-9]+" not found for "transformation" configuration '.
                '"[a-z0-9]+\\\+main-1" in branch "[0-9]+"$/',
                $e->getMessage()
            );
            $this->assertSame(404, $e->getCode());
        }
    }

    public function testDeleteMetadataEvent(): void
    {
        $defaultBranchId = $this->getDefaultBranchId($this);

        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        $this->initEvents($branchClient);
        $components = new Components($branchClient);

        $configurationOptions = $this->createConfiguration(
            $components,
            'wr-db',
            'component-metadata-events-test',
            'Component metadata events'
        );

        $configurationMetadataOptions = (new ConfigurationMetadata($configurationOptions))
            ->setMetadata(self::TEST_METADATA);
        $newMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);

        $components->deleteConfigurationMetadata('wr-db', 'component-metadata-events-test', $newMetadata[0]['id']);

        $assertCallback = function ($events) use ($configurationOptions, $newMetadata) {
            $this->assertCount(1, $events);
            $this->assertEvent(
                $events[0],
                'storage.componentConfigurationMetadataDeleted',
                sprintf(
                    'Deleted component configuration metadata id "%s" with key "KBC.SomeEnity.metadataKey"',
                    (int) $newMetadata[0]['id']
                ),
                $configurationOptions->getConfigurationId(),
                'Component metadata events',
                'componentConfiguration',
                [
                    'component' => 'wr-db',
                    'configurationId' => $configurationOptions->getConfigurationId(),
                    'name' => 'Component metadata events',
                    'version' => 1,
                    'metadataId' => (int) $newMetadata[0]['id'],
                    'key' => 'KBC.SomeEnity.metadataKey',
                ]
            );
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationMetadataDeleted')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($branchClient, $assertCallback, $query);
    }
}
