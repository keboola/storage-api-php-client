<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationMetadata;
use Keboola\StorageApi\Options\Components\ListConfigurationMetadataOptions;
use Keboola\Test\StorageApiTestCase;

class ConfigurationMetadataTest extends StorageApiTestCase
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

    public function setUp()
    {
        parent::setUp();

        $this->cleanupConfigurations();
    }

    /**
     * @dataProvider provideBranchAwareComponentsClient
     */
    public function testAddMetadata(callable $getClient)
    {
        $configurationNameMain1 = $this->generateUniqNameForString('main-1');
        $configurationNameMain2 = $this->generateUniqNameForString('main-2');

        $client = $getClient($this);
        $components = new Components($client);

        // prepare two configs
        $transformationMain1Options = $this->createConfiguration($components, 'transformation', $configurationNameMain1);
        $this->createConfiguration($components, 'transformation', $configurationNameMain2);

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
        $this->createConfiguration($components, 'wr-db', $configurationNameMain1);
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
                ]
            ]);
        $newMetadata = $components->addConfigurationMetadata($afterRestoreOptions);
        self::assertCount(3, $newMetadata);

        $metadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(3, $metadata);
    }

    /**
     * @dataProvider provideBranchAwareComponentsClient
     */
    public function testUpdateMetadata(callable $getClient)
    {
        $client = $getClient($this);
        $components = new Components($client);

        $configurationNameMain1 = $this->generateUniqNameForString('main-1');

        $transformationMain1Options = $this->createConfiguration($components, 'transformation', $configurationNameMain1);

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
     * @dataProvider provideBranchAwareComponentsClient
     */
    public function testAddMetadataEvent(callable $getClient)
    {
        $client = $getClient($this);

        $components = new Components($client);
        $configurationOptions = $this->createConfiguration($components, 'wr-db', 'component-metadata-events-test');

        $configurationMetadataOptions = (new ConfigurationMetadata($configurationOptions))
            ->setMetadata(self::TEST_METADATA);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $events = $this->listEvents($client, 'storage.componentConfigurationMetadataCreated');

        self::assertSame(self::TEST_METADATA, $events[0]['results']['metadata']);

        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationMetadataSet',
            'Component configuration metadata set "New Config" (wr-db)',
            $configurationOptions->getConfigurationId(),
            'New Config',
            'componentConfiguration',
            [
                'component' => 'wr-db',
                'configurationId' => $configurationOptions->getConfigurationId(),
                'name' => 'New Config',
                'version' => 1,
            ]
        );
    }

    public function testCreateBranchCopyMetadataToTheDevBranch()
    {
        $configurationNameMain1 = $this->generateUniqNameForString('main-1');

        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);

        $components = new Components($branchClient);
        $transformationMain1Options = $this->createConfiguration($components, 'transformation', $configurationNameMain1);

        // add metadata to first configuration
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $newMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);
        self::assertCount(2, $newMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $newMetadata[0]);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $newMetadata[1]);

        // create new devbranch
        $branch = $this->createOrReuseDevBranch($this);
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
                    'value' => 'new value'
                ],
            ]);
        $newMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);
        self::assertCount(3, $newMetadata);

        $listConfigurationMetadata = $branchComponents->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationNameMain1));
        self::assertCount(2, $listConfigurationMetadata);
    }

    public function testResetToDefault()
    {
        $configurationNameMain1 = $this->generateUniqNameForString('main-1');
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

        $branch = $this->createOrReuseDevBranch($this);
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
            ]
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
                ]
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

    public function testConfigMetadataRestrictionsForReadOnlyUser()
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

        $configurationNameMain1 = $this->generateUniqNameForString('main-1');

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
            $this->assertContains('Configuration manipulation is restricted for your user role', $e->getMessage());
            $this->assertSame(403, $e->getCode());
        }
    }

    private function assertMetadataEquals(array $expected, array $actual)
    {
        foreach ($expected as $key => $value) {
            self::assertArrayHasKey($key, $actual);
            self::assertSame($value, $actual[$key]);
        }
        self::assertArrayHasKey('timestamp', $actual);
    }

    private function createConfiguration($components, $componentId, $configurationId, $name = 'New Config')
    {
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName($name);

        $components->addConfiguration($configurationOptions);
        return $configurationOptions;
    }
}
