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

    public function testAddMetadata()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new \Keboola\StorageApi\DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];

        $branchIds = $this->prepareBranchesForTests($devBranch, $branchName);
        foreach ($branchIds as $branchId) {
            $branchClient = $this->getBranchAwareDefaultClient($branchId);
            $components = new Components($branchClient);

            // prepare two configs
            $transformationMain1Options = $this->createConfiguration($components, 'transformation', 'main-1');
            $this->createConfiguration($components, 'transformation', 'main-2');

            // test if both return 0 metadata
            $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
                ->setComponentId('transformation')
                ->setConfigurationId('main-1'));
            self::assertCount(0, $listConfigurationMetadata);

            $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
                ->setComponentId('transformation')
                ->setConfigurationId('main-2'));
            self::assertCount(0, $listConfigurationMetadata);

            //create second configs for other component to test add configuration metadata returns right count of metadata
            $this->createConfiguration($components, 'wr-db', 'main-1');
            $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1'));
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
                ->setConfigurationId('main-1'));
            self::assertCount(2, $listConfigurationMetadata);
            $this->assertMetadataEquals(self::TEST_METADATA[0], $listConfigurationMetadata[0]);
            $this->assertMetadataEquals(self::TEST_METADATA[1], $listConfigurationMetadata[1]);

            // test if second configuration has still 0 metadata
            $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
                ->setComponentId('transformation')
                ->setConfigurationId('main-2'));
            self::assertCount(0, $listConfigurationMetadata);

            // test if second component has still 0 metadata
            $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1'));
            self::assertCount(0, $listConfigurationMetadata);

            // delete configuration to test fail add/list metadata
            $components->deleteConfiguration('transformation', 'main-1');

            try {
                $components->addConfigurationMetadata($configurationMetadataOptions);
                $this->fail('configuration desn\'t exist');
            } catch (ClientException $e) {
                $this->assertSame('Configuration main-1 not found', $e->getMessage());
                $this->assertSame(404, $e->getCode());
            }

            try {
                $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
                    ->setComponentId('transformation')
                    ->setConfigurationId('main-1'));
                $this->fail('configuration desn\'t exist');
            } catch (ClientException $e) {
                $this->assertSame('Configuration main-1 not found', $e->getMessage());
                $this->assertSame(404, $e->getCode());
            }

            // test after restore component can add or list metadata
            $components->restoreComponentConfiguration('transformation', 'main-1');

            // test can list metadata after restore configuration
            $metadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
                ->setComponentId('transformation')
                ->setConfigurationId('main-1'));
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
                ->setConfigurationId('main-1'));
            self::assertCount(3, $metadata);
        }
    }

    public function testUpdateMetadata()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new \Keboola\StorageApi\DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];

        $branchIds = $this->prepareBranchesForTests($devBranch, $branchName);
        foreach ($branchIds as $branchId) {
            $branchClient = $this->getBranchAwareDefaultClient($branchId);
            $components = new Components($branchClient);
            $transformationMain1Options = $this->createConfiguration($components, 'transformation', 'main-1');

            $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
                ->setComponentId('transformation')
                ->setConfigurationId('main-1'));
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
                ->setConfigurationId('main-1'));
            self::assertCount(2, $listConfigurationMetadata);
            $this->assertMetadataEquals(self::TEST_METADATA[0], $listConfigurationMetadata[0]);
            $this->assertMetadataEquals($updatedMetadata[0], $listConfigurationMetadata[1]);
        }
    }

    public function testAddMetadataEvent()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new \Keboola\StorageApi\DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];

        $branchIds = $this->prepareBranchesForTests($devBranch, $branchName);
        foreach ($branchIds as $branchId) {
            $branchClient = $this->getBranchAwareDefaultClient($branchId);
            $components = new Components($branchClient);
            $configurationOptions = $this->createConfiguration($components, 'wr-db', 'component-metadata-events-test');

            $configurationMetadataOptions = (new ConfigurationMetadata($configurationOptions))
                ->setMetadata(self::TEST_METADATA);
            $components->addConfigurationMetadata($configurationMetadataOptions);

            $events = $this->listEvents('storage.componentConfigurationMetadataCreated');

            self::assertSame(self::TEST_METADATA, $events[0]['results']['metadata']);

            $this->assertEvent(
                $events[0],
                'storage.componentConfigurationMetadataSet',
                'Component configuration metadata set "Main-1" (wr-db)',
                $configurationOptions->getConfigurationId(),
                'Main-1',
                'componentConfiguration',
                [
                    'component' => 'wr-db',
                    'configurationId' => $configurationOptions->getConfigurationId(),
                    'name' => 'Main-1',
                    'version' => 1,
                ]
            );
        }
    }

    public function testCreateBranchCopyMetadataToTheDevBranch()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new \Keboola\StorageApi\DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];

        $branchIds = $this->prepareBranchesForTests($devBranch, $branchName, false);

        $branchClient = $this->getBranchAwareDefaultClient($branchIds[0]);
        $components = new Components($branchClient);
        $transformationMain1Options = $this->createConfiguration($components, 'transformation', 'main-1');

        // add metadata to first configuration
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $newMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);
        self::assertCount(2, $newMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $newMetadata[0]);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $newMetadata[1]);

        // create new devbranch
        $branch = $devBranch->createBranch($branchName);
        $branchComponents = new Components($this->getBranchAwareDefaultClient($branch['id']));

        // metadata should be copied from default branch
        $listConfigurationMetadata = $branchComponents->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId('main-1'));
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
            ->setConfigurationId('main-1'));
        self::assertCount(2, $listConfigurationMetadata);
    }

    public function testResetToDefault()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new \Keboola\StorageApi\DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $this->prepareBranchesForTests($devBranch, $branchName, false);

        // create new configurations in main branch
        $components = new Components($this->_client);
        $transformationMain1Options = (new Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('main-1')
            ->setName('Main 1')
            ->setConfiguration(['test' => 'false']);

        $components->addConfiguration($transformationMain1Options);
        $configurationMetadataOptions = (new ConfigurationMetadata($transformationMain1Options))
            ->setMetadata(self::TEST_METADATA);
        $newMetadata = $components->addConfigurationMetadata($configurationMetadataOptions);
        self::assertCount(2, $newMetadata);

        $branch = $devBranch->createBranch($branchName);
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

        $branchComponents->resetToDefault('transformation', 'main-1');

        // after resetToDefault development branch metadata should have new updated value
        $listConfigurationMetadata = $branchComponents->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId('main-1'));
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

        $branchComponents->resetToDefault('transformation', 'main-1');

        $listConfigurationMetadata = $branchComponents->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId('transformation')
            ->setConfigurationId('main-1'));
        self::assertCount(3, $listConfigurationMetadata);

        $this->assertMetadataEquals(self::TEST_METADATA[0], $listConfigurationMetadata[0]);
        $this->assertMetadataEquals($updatedMetadata[0], $listConfigurationMetadata[1]);
        $this->assertMetadataEquals($moreMetadata[0], $listConfigurationMetadata[2]);
    }

    public function testConfigMetadataRestrictionsForReadOnlyUser()
    {
        $guestClient = $this->getClientForToken(STORAGE_API_READ_ONLY_TOKEN);

        // create new configurations in main branch
        $components = new Components($this->_client);
        $transformationMain1Options = (new Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('main-1')
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

    private function createConfiguration($components, $componentId, $configurationId, $name = 'Main-1')
    {
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName($name);

        $components->addConfiguration($configurationOptions);
        return $configurationOptions;
    }

    private function prepareBranchesForTests(DevBranches $devBranch, $branchName, $createDevBranch = true)
    {
        //clean devbranch
        $this->deleteBranchesByPrefix($devBranch, $branchName);

        $branchesList = $devBranch->listBranches();

        //get default branchId
        $defaultBranchId = null;
        foreach ($branchesList as $branch) {
            if ($branch['isDefault'] === true) {
                $defaultBranchId = $branch['id'];
            }
        }

        if ($createDevBranch) {
            $developmentBranch = $devBranch->createBranch($branchName)['id'];
            return [$defaultBranchId, $developmentBranch];
        } else {
            return [$defaultBranchId];
        }
    }
}
