<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ConfigurationRowState;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use Keboola\Test\StorageApiTestCase;

class BranchComponentTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();

        $components = new \Keboola\StorageApi\Components($this->_client);
        foreach ($components->listComponents() as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // erase all deleted configurations
        foreach ($components->listComponents((new ListComponentsOptions())->setIsDeleted(true)) as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }
    }

    private function isVersionsListImplementedForDevBranch()
    {
        if (IS_VERSIONS_LIST_IMPLEMENTED_FOR_DEV_BRANCH === "true") {
            return true;
        } else {
            return false;
        }
    }

    public function testManipulationWithComponentConfigurations()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $branch = $this->deleteBranchesByPrefix($devBranch, $branchName);

        // create new configurations in main branch
        $componentId = 'transformation';
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configurationData = ['x' => 'y'];
        $configurationOptions = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1')
            ->setName('Main 1')
            ->setConfiguration($configurationData);

        $components->addConfiguration($configurationOptions);
        $components->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Main 1 Row 1')
                ->setRowId('main-1-row-1')
        );

        $deletedConfigurationOptions = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('deleted-main')
            ->setName('Deleted Main');
        $components->addConfiguration($deletedConfigurationOptions);
        $components->addConfigurationRow(
            (new ConfigurationRow($deletedConfigurationOptions))
                ->setName('Deleted Main Row 1')
                ->setRowId('deleted-main-row-1')
        );
        // configuration exists
        $components->getConfiguration($componentId, 'deleted-main');
        $components->deleteConfiguration($componentId, 'deleted-main');
        // configuration is deleted
        try {
            $components->getConfiguration($componentId, 'deleted-main');
            $this->fail('Configuration should be deleted in the main branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration deleted-main not found', $e->getMessage());
        }

        // dummy branch to highlight potentially forgotten where on branch
        $devBranch->createBranch($branchName . '-dummy');

        $branch = $devBranch->createBranch($branchName);

        $branchComponents = new \Keboola\StorageApi\Components($this->getBranchAwareDefaultClient($branch['id']));

        try {
            $branchComponents->getConfiguration($componentId, 'deleted-main');
            $this->fail('Configuration deleted in the main branch shouldn\'t exist in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration deleted-main not found', $e->getMessage());
        }

        try {
            $branchComponents->deleteConfigurationRow($componentId, 'main-1', 'main-1-row-1');
            $this->fail('Configuration row cannot be deleted in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('notImplemented', $e->getStringCode());
            $this->assertContains('Not implemented', $e->getMessage());
        }

        $configFromMain = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertSame(1, $configFromMain['version']);

        if ($this->isVersionsListImplementedForDevBranch()) {
            // test is version created for devBranch configuration after create branch
            $configurationVersions = $branchComponents->listConfigurationVersions(
                (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                    ->setComponentId($componentId)
                    ->setConfigurationId('main-1')
            );

            $this->assertCount(1, $configurationVersions);
            $configurationVersion = $branchComponents->getConfigurationVersion($componentId, 'main-1', 1);

            $this->assertArrayHasKey('version', $configurationVersion);
            $this->assertSame(1, $configurationVersion['version']);
            $this->assertIsInt($configurationVersion['creatorToken']['id']);
            $this->assertArrayNotHasKey('state', $configurationVersion);
            $this->assertArrayHasKey('configuration', $configurationVersion);
            $this->assertSame($configurationData, $configurationVersion['configuration']);
            $this->assertSame(
                'Copied from default branch configuration version "Main 1" (configuration id: main-1) version 2',
                $configurationVersion['changeDescription']
            );
        } else {
            try {
                $branchComponents->listConfigurationVersions(
                    (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                        ->setComponentId($componentId)
                        ->setConfigurationId('main-1')
                );
                $this->fail('Configuration versions list should not be implemented');
            } catch (ClientException $e) {
                $this->assertSame(501, $e->getCode());
                $this->assertSame('notImplemented', $e->getStringCode());
                $this->assertContains('Not implemented', $e->getMessage());
            }

            try {
                $branchComponents->getConfigurationVersion($componentId, 'main-1', 1);
                $this->fail('Configuration versions detail should not be implemented');
            } catch (ClientException $e) {
                $this->assertSame(501, $e->getCode());
                $this->assertSame('notImplemented', $e->getStringCode());
                $this->assertContains('Not implemented', $e->getMessage());
            }
        }

        // test config time created is different for branch config
        $configMain = $components->getConfiguration($componentId, 'main-1');
        $this->assertNotEquals($configMain['created'], $configFromMain['created']);
        $this->assertEquals('Copied from default branch configuration "Main 1" (main-1) version 2', $configFromMain['changeDescription']);

        $currentVersion = $configFromMain['currentVersion'];
        $this->assertEquals('Copied from default branch configuration "Main 1" (main-1) version 2', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

        try {
            $branchComponents->deleteConfiguration($componentId, 'main-1');
            $this->fail('Configuration cannot be deleted in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('notImplemented', $e->getStringCode());
            $this->assertContains('Not implemented', $e->getMessage());
        }

        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));

        // Create new dev branch should clone all configurations and theirs config row
        // in this case is one row in main configuration
        $this->assertCount(1, $rows);

        $row = $branchComponents->getConfigurationRow(
            $componentId,
            'main-1',
            'main-1-row-1'
        );

        $this->assertEquals('main-1-row-1', $row['id']);
        $this->assertEquals(1, $row['version']);
        $this->assertEquals('Copied from default branch configuration row "Main 1 Row 1" (main-1-row-1) version 1', $row['changeDescription']);

        $mainBranchRow = $components->getConfigurationRow(
            $componentId,
            'main-1',
            'main-1-row-1'
        );

        // test config row time created is different for branch config
        $this->assertNotEquals($mainBranchRow['created'], $row['created']);

        $branchConfigs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );

        // There is only the one configuration that was copied from production
        $this->assertCount(1, $branchConfigs);

        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('main-2')
            ->setName('Main 2'));

        // Add new configuration row to main branch shouldn't create new configuration row in dev branch
        $components->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Main 1 Row 2')
                ->setRowId('main-1-row-2')
        );

        // Check new config rows added to main branch
        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(2, $rows);

        $configs = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );
        // two configuration was created in main branch
        $this->assertCount(2, $configs);

        $branchConfigs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );

        // Creating new configuration row in main branch shouldn't create new configuration row in dev branch
        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(1, $rows);

        $row = $branchComponents->getConfigurationRow(
            $componentId,
            'main-1',
            'main-1-row-1'
        );

        $this->assertEquals('main-1-row-1', $row['id']);

        try {
            $branchComponents->getConfigurationRow(
                $componentId,
                'main-1',
                'main-1-row-2'
            );
            $this->fail('Configuration row created in main branch shouldn\'t exist in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Row main-1-row-2 not found', $e->getMessage());
        }

        // there should be the one config existing in production before creating the branch
        $this->assertCount(1, $branchConfigs);

        $mainComponentDetail = $components->getConfiguration($componentId, 'main-1');
        $this->assertNotEmpty($mainComponentDetail);

        $branchMain1Detail = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertNotEmpty($branchMain1Detail);
        // versions are reset to 1 when copied to dev branch
        $this->assertSame(3, $mainComponentDetail['version']);
        $this->assertSame(1, $branchMain1Detail['version']);

        try {
            $branchComponents->getConfiguration($componentId, 'main-2');
            $this->fail('Configuration created in main branch after branching shouldn\'t exist in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration main-2 not found', $e->getMessage());
        }

        $mainComponentDetail  = $components->getConfiguration($componentId, 'main-1');
        $this->assertSame('main-1', $mainComponentDetail['id']);

        // add two config rows to test rowsSortOrder
        $branchComponents->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Dev 1 Row 1')
                ->setRowId('dev-1-row-1')
        );

        if ($this->isVersionsListImplementedForDevBranch()) {
            // test is version created for devBranch after add new config row
            $configurationVersions = $branchComponents->listConfigurationVersions(
                (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                    ->setComponentId($componentId)
                    ->setConfigurationId('main-1')
            );

            $this->assertCount(2, $configurationVersions);

            $configurationVersion = $branchComponents->getConfigurationVersion($componentId, 'main-1', 'latestPublished');

            $this->assertArrayHasKey('version', $configurationVersion);
            $this->assertSame(2, $configurationVersion['version']);
            $this->assertIsInt($configurationVersion['creatorToken']['id']);
            $this->assertArrayNotHasKey('state', $configurationVersion);
            $this->assertSame('Row dev-1-row-1 added', $configurationVersion['changeDescription']);

            // test get other version
            $configurationVersion = $branchComponents->getConfigurationVersion($componentId, 'main-1', 1);

            $this->assertArrayHasKey('version', $configurationVersion);
            $this->assertSame(1, $configurationVersion['version']);
            $this->assertIsInt($configurationVersion['creatorToken']['id']);
            $this->assertArrayNotHasKey('state', $configurationVersion);
            $this->assertSame(
                'Copied from default branch configuration version "Main 1" (configuration id: main-1) version 2',
                $configurationVersion['changeDescription']
            );
        }

        $configFromMain = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertEquals('Row dev-1-row-1 added', $configFromMain['changeDescription']);
        $this->assertSame(2, $configFromMain['version']);

        $currentVersion = $configFromMain['currentVersion'];
        $this->assertEquals('Row dev-1-row-1 added', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

        $configurationOptions->setRowsSortOrder(['main-1-row-1', 'dev-1-row-1']);
        $branchComponents->updateConfiguration($configurationOptions);

        $configFromMain = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertEquals('Configuration updated', $configFromMain['changeDescription']);

        $this->assertSame(3, $configFromMain['version']);
        $currentVersion = $configFromMain['currentVersion'];
        $this->assertEquals('Configuration updated', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

        $branchComponents->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Dev 1 Row 3')
                ->setRowId('dev-1-row-3')
                ->setChangeDescription('Custom change desc')
        );

        $configFromMain = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertEquals('Custom change desc', $configFromMain['changeDescription']);
        $this->assertSame(4, $configFromMain['version']);
        $currentVersion = $configFromMain['currentVersion'];
        $this->assertEquals('Custom change desc', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

        if ($this->isVersionsListImplementedForDevBranch()) {
            // test is version created for devBranch after add new config row with custom change description
            $configurationVersions = $branchComponents->listConfigurationVersions(
                (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                    ->setComponentId($componentId)
                    ->setConfigurationId('main-1')
            );

            $this->assertCount(4, $configurationVersions);

            $configurationVersion = $branchComponents->getConfigurationVersion($componentId, 'main-1', 'latestPublished');

            $this->assertArrayHasKey('version', $configurationVersion);
            $this->assertSame(4, $configurationVersion['version']);
            $this->assertIsInt($configurationVersion['creatorToken']['id']);
            $this->assertArrayNotHasKey('state', $configurationVersion);
            $this->assertSame('Custom change desc', $configurationVersion['changeDescription']);
        }

        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));

        $this->assertEquals('main-1-row-1', $rows[0]['id']);
        $this->assertEquals('dev-1-row-1', $rows[1]['id']);
        $this->assertEquals('dev-1-row-3', $rows[2]['id']);
        $this->assertCount(3, $rows);

        $this->assertEquals('Copied from default branch configuration row "Main 1 Row 1" (main-1-row-1) version 1', $rows[0]['changeDescription']);
        $this->assertEquals(1, $rows[0]['version']);
        $this->assertEquals(1, $rows[1]['version']);
        $this->assertEquals('Row dev-1-row-1 added', $rows[1]['changeDescription']);
        $this->assertEquals(1, $rows[2]['version']);
        $this->assertEquals('Custom change desc', $rows[2]['changeDescription']);
        $devBranchConfiguration = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertEquals(4, $devBranchConfiguration['version']); // add rows should update config version

        $branchComponents->updateConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setRowId('dev-1-row-1')
                ->setName('Renamed Dev 1 Row 1')
                ->setConfiguration('{"id":"10","stuff":"true"}')
                ->setChangeDescription('Test change dev-1-row-1')
        );

        $updatedRow = $branchComponents->getConfigurationRow(
            $componentId,
            'main-1',
            'dev-1-row-1'
        );
        $configurationAssociatedWithUpdatedRow = $branchComponents->getConfiguration('transformation', 'main-1');

        $this->assertEquals('Renamed Dev 1 Row 1', $updatedRow['name']);
        $this->assertEquals('Test change dev-1-row-1', $updatedRow['changeDescription']);
        $this->assertEquals('Test change dev-1-row-1', $configurationAssociatedWithUpdatedRow['changeDescription']);
        $this->assertEquals('{"id":"10","stuff":"true"}', $updatedRow['configuration'][0]);
        $this->assertEquals(2, $updatedRow['version']);

        $currentVersion = $configurationAssociatedWithUpdatedRow['currentVersion'];
        $this->assertEquals('Test change dev-1-row-1', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

        if ($this->isVersionsListImplementedForDevBranch()) {
            // test is version created for devBranch after update config row with custom change description
            $configurationVersions = $branchComponents->listConfigurationVersions(
                (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                    ->setComponentId('transformation')
                    ->setConfigurationId('main-1')
            );

            $this->assertCount(5, $configurationVersions);

            $configurationVersion = $branchComponents->getConfigurationVersion('transformation', 'main-1', 'latestPublished');

            $this->assertArrayHasKey('version', $configurationVersion);
            $this->assertSame(5, $configurationVersion['version']);
            $this->assertIsInt($configurationVersion['creatorToken']['id']);
            $this->assertArrayNotHasKey('state', $configurationVersion);
            $this->assertSame('Test change dev-1-row-1', $configurationVersion['changeDescription']);
        }

        $branchComponents->updateConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setRowId('dev-1-row-1')
                ->setName('Renamed Dev 1 Row 1')
                ->setConfiguration('{"id":"10","stuff":"true"}')
        );

        $updatedRow = $branchComponents->getConfigurationRow(
            $componentId,
            'main-1',
            'dev-1-row-1'
        );
        $configurationAssociatedWithUpdatedRow = $branchComponents->getConfiguration('transformation', 'main-1');

        $this->assertEquals('Renamed Dev 1 Row 1', $updatedRow['name']);
        $this->assertEquals('Row dev-1-row-1 changed', $updatedRow['changeDescription']);
        $this->assertEquals('Row dev-1-row-1 changed', $configurationAssociatedWithUpdatedRow['changeDescription']);
        $this->assertEquals('{"id":"10","stuff":"true"}', $updatedRow['configuration'][0]);
        $this->assertEquals(3, $updatedRow['version']); // update row should update version for row

        $currentVersion = $configurationAssociatedWithUpdatedRow['currentVersion'];
        $this->assertEquals('Row dev-1-row-1 changed', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

        if ($this->isVersionsListImplementedForDevBranch()) {
            // test is version created for devBranch after add new config row with custom change description
            $configurationVersions = $branchComponents->listConfigurationVersions(
                (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                    ->setComponentId('transformation')
                    ->setConfigurationId('main-1')
            );

            $this->assertCount(6, $configurationVersions);

            $configurationVersion = $branchComponents->getConfigurationVersion('transformation', 'main-1', 'latestPublished');

            $this->assertArrayHasKey('version', $configurationVersion);
            $this->assertSame(6, $configurationVersion['version']);
            $this->assertIsInt($configurationVersion['creatorToken']['id']);
            $this->assertArrayNotHasKey('state', $configurationVersion);
            $this->assertSame('Row dev-1-row-1 changed', $configurationVersion['changeDescription']);
        }

        // restrict row state change on configuration update
        try {
            $branchComponents->updateConfigurationRow(
                (new ConfigurationRow($configurationOptions))
                    ->setRowId('dev-1-row-1')
                    ->setChangeDescription('state update')
                    ->setState([
                        'cache' => true,
                    ])
            );
            $this->fail('Update of row state should be restricted.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals(
                'Using \'state\' parameter on configuration row update is restricted for dev/branch context. Use direct API call.',
                $e->getMessage()
            );
        }

        // row state update
        $state = [
            'cache' => false,
        ];

        $rowState = (new ConfigurationRowState($configurationOptions))
            ->setRowId('dev-1-row-1')
            ->setState($state)
        ;

        $updatedRow = $branchComponents->updateConfigurationRowState($rowState);
        $this->assertEquals($state, $updatedRow['state']);

        $row = $branchComponents->getConfigurationRow(
            $rowState->getComponentConfiguration()->getComponentId(),
            $rowState->getComponentConfiguration()->getConfigurationId(),
            $rowState->getRowId()
        );

        $this->assertEquals($state, $row['state']);
        $this->assertEquals(3, $row['version']); // update state shouldn't update version

        $updatedConfiguration = $branchComponents->getConfiguration(
            $componentId,
            'main-1'
        );
        $this->assertEquals('Row dev-1-row-1 changed', $updatedConfiguration['changeDescription']);

        try {
            $components->getConfigurationRow(
                $componentId,
                'main-1',
                'dev-1-row-1'
            );
            $this->fail('Configuration row created in dev branch shouldn\'t exist in main branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Row dev-1-row-1 not found', $e->getMessage());
        }

        //create
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('dev-branch-1')
            ->setName('Dev Branch 1')
            ->setDescription('Test configuration created');

        // create new configuration in dev branch
        $branchComponents->addConfiguration($config);

        // new configuration must exist in dev branch
        $branchComponentDetail = $branchComponents->getConfiguration('transformation', 'dev-branch-1');
        $this->assertEquals('Dev Branch 1', $branchComponentDetail['name']);
        $this->assertEmpty($branchComponentDetail['configuration']);
        $this->assertSame('Test configuration created', $branchComponentDetail['description']);
        $this->assertEquals('Configuration created', $branchComponentDetail['changeDescription']);
        $this->assertEquals(1, $branchComponentDetail['version']);
        $this->assertIsInt($branchComponentDetail['version']);
        $this->assertIsInt($branchComponentDetail['creatorToken']['id']);

        $currentVersion = $branchComponentDetail['currentVersion'];
        $this->assertEquals('Configuration created', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

        if ($this->isVersionsListImplementedForDevBranch()) {
            // test create new config create new version for configuration
            $configurationVersions = $branchComponents->listConfigurationVersions(
                (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                    ->setComponentId($componentId)
                    ->setConfigurationId('dev-branch-1')
            );
            $this->assertCount(1, $configurationVersions);

            $configurationVersion = $branchComponents->getConfigurationVersion($componentId, 'dev-branch-1', 1);

            $this->assertArrayHasKey('version', $configurationVersion);
            $this->assertSame(1, $configurationVersion['version']);
            $this->assertIsInt($configurationVersion['creatorToken']['id']);
            $this->assertArrayNotHasKey('state', $configurationVersion);
            $this->assertEmpty($branchComponentDetail['configuration']);
            $this->assertSame(
                'Configuration created',
                $configurationVersion['changeDescription']
            );
        }

        $configs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );

        $this->assertCount(2, $configs);

        try {
            $components->getConfiguration('transformation', 'dev-branch-1');
            $this->fail('Configuration created in dev branch shouldn\'t exist in main branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration dev-branch-1 not found', $e->getMessage());
        }

        //Update
        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = ['x' => 'y'];
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $config->setRowsSortOrder([]);
        $branchComponents->updateConfiguration($config);

        // if updated twice, the version is incremented each time by 1
        $newName = 'neco-nove';
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $config->setRowsSortOrder([]);
        $branchComponents->updateConfiguration($config);

        $configuration = $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $this->assertEquals($newName, $configuration['name']);
        $this->assertEquals($newDesc, $configuration['description']);
        $this->assertEquals($config->getConfiguration(), $configuration['configuration']);
        $this->assertEquals(3, $configuration['version']);
        $this->assertEquals('Configuration updated', $configuration['changeDescription']);

        if ($this->isVersionsListImplementedForDevBranch()) {
            // test is version created for devBranch after update configuration
            $configurationVersions = $branchComponents->listConfigurationVersions(
                (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                    ->setComponentId($config->getComponentId())
                    ->setConfigurationId($config->getConfigurationId())
            );

            $this->assertCount(3, $configurationVersions);
            $configurationVersion = $branchComponents->getConfigurationVersion($config->getComponentId(), $config->getConfigurationId(), 'latestPublished');

            $this->assertArrayHasKey('version', $configurationVersion);
            $this->assertSame(3, $configurationVersion['version']);
            $this->assertIsInt($configurationVersion['creatorToken']['id']);
            $this->assertArrayNotHasKey('state', $configurationVersion);
            $this->assertArrayHasKey('configuration', $configurationVersion);
            $this->assertSame($configurationData, $configurationVersion['configuration']);
            $this->assertSame('Configuration updated', $configurationVersion['changeDescription']);
        }

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('dev-branch-1')
            ->setDescription('neco')
            ->setChangeDescription('Custom change desc')
        ;

        $updatedConfig = $branchComponents->updateConfiguration($config);
        $this->assertEquals($newName, $updatedConfig['name'], 'Name should not be changed after description update');
        $this->assertEquals('neco', $updatedConfig['description']);
        $this->assertEquals($configurationData, $updatedConfig['configuration']);
        $this->assertEquals([], $updatedConfig['state']);
        $configuration = $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $this->assertEquals($newName, $configuration['name'], 'Name should not be changed after description update');
        $this->assertEquals('neco', $configuration['description']);

        $this->assertEquals($configurationData, $configuration['configuration']);
        $this->assertEquals([], $configuration['state']);
        $this->assertSame('Custom change desc', $configuration['changeDescription']);
        $this->assertEquals(4, $configuration['version']);

        $configFromMain = $branchComponents->getConfiguration('transformation', 'dev-branch-1');
        $currentVersion = $configFromMain['currentVersion'];
        $this->assertEquals('Custom change desc', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

        if ($this->isVersionsListImplementedForDevBranch()) {
            // test is version created for devBranch after config update with custom change description
            $configurationVersions = $branchComponents->listConfigurationVersions(
                (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                    ->setComponentId('transformation')
                    ->setConfigurationId('dev-branch-1')
            );

            $this->assertCount(4, $configurationVersions);
            $configurationVersion = $branchComponents->getConfigurationVersion('transformation', 'dev-branch-1', 'latestPublished');

            $this->assertArrayHasKey('version', $configurationVersion);
            $this->assertSame(4, $configurationVersion['version']);
            $this->assertIsInt($configurationVersion['creatorToken']['id']);
            $this->assertArrayNotHasKey('state', $configurationVersion);
            $this->assertArrayHasKey('configuration', $configurationVersion);
            $this->assertSame($configurationData, $configurationVersion['configuration']);
            $this->assertSame('Custom change desc', $configurationVersion['changeDescription']);
        }

        $state = [
            'cache' => false,
        ];

        $configState = (new \Keboola\StorageApi\Options\Components\ConfigurationState())
            ->setComponentId('transformation')
            ->setConfigurationId('dev-branch-1')
            ->setState($state)
        ;

        $updatedConfig = $branchComponents->updateConfigurationState($configState);
        $this->assertEquals($state, $updatedConfig['state']);

        $configuration = $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals($state, $configuration['state']);
        $this->assertEquals(4, $configuration['version']); // update state shouldn't change version

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('dev-branch-1')
            ->setDescription('');

        $branchComponents->updateConfiguration($config);
        $configuration = $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals('', $configuration['description'], 'Description can be set empty');
        $this->assertEquals(5, $configuration['version']);

        // List components test
        $configs = $branchComponents->listComponents();
        $this->assertCount(1, $configs);

        $branchComponents->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('branch-1')
            ->setChangeDescription('create custom desc')
            ->setName('Dev Branch'));

        $configurationCustomDesc = $branchComponents->getConfiguration('wr-db', 'branch-1');
        $this->assertEquals('create custom desc', $configurationCustomDesc['changeDescription']);

        $currentVersion = $configurationCustomDesc['currentVersion'];
        $this->assertEquals('create custom desc', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

        if ($this->isVersionsListImplementedForDevBranch()) {
            // test is version created for devBranch configuration after create new config
            $configurationVersions = $branchComponents->listConfigurationVersions(
                (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                    ->setComponentId('wr-db')
                    ->setConfigurationId('branch-1')
            );

            $this->assertCount(1, $configurationVersions);
            $configurationVersion = $branchComponents->getConfigurationVersion('wr-db', 'branch-1', 1);

            $this->assertArrayHasKey('version', $configurationVersion);
            $this->assertSame(1, $configurationVersion['version']);
            $this->assertIsInt($configurationVersion['creatorToken']['id']);
            $this->assertArrayNotHasKey('state', $configurationVersion);
            $this->assertArrayHasKey('configuration', $configurationVersion);
            $this->assertSame(
                'create custom desc',
                $configurationVersion['changeDescription']
            );
        }

        $branchComponents->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('branch-2')
            ->setConfiguration(array('x' => 'y'))
            ->setName('Dev branch'));
        $branchComponents->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('provisioning')
            ->setConfigurationId('branch-1')
            ->setName('Dev Branch'));

        $configs = $branchComponents->listComponents();
        $this->assertCount(3, $configs);

        $configs = $branchComponents->listComponents((new ListComponentsOptions())
            ->setComponentType('writer'));

        $this->assertCount(2, $configs[0]['configurations']);
        $this->assertCount(1, $configs);

        $configuration = $configs[0]['configurations'][0];
        $this->assertArrayNotHasKey('configuration', $configuration);

        // list with configuration body
        $configs = $branchComponents->listComponents((new ListComponentsOptions())
            ->setComponentType('writer')
            ->setInclude(array('configuration')));

        $this->assertCount(2, $configs[0]['configurations']);
        $this->assertCount(1, $configs);

        $configuration = $configs[0]['configurations'][0];
        $this->assertArrayHasKey('configuration', $configuration);

        // restrict state change on configuration update
        $configuration = $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('dev-branch-1')
            ->setChangeDescription('updated state')
            ->setState([
                'cache' => true,
            ])
        ;

        try {
            $branchComponents->updateConfiguration($config);
            $this->fail('Update of configuration state should be restricted.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals(
                'Using \'state\' parameter on configuration update is restricted for dev/branch context. Use direct API call.',
                $e->getMessage()
            );
        }

        $this->assertEquals(
            $configuration,
            $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId())
        );
    }

    /**
     * @param string $branchPrefix
     */
    protected function deleteBranchesByPrefix(DevBranches $devBranches, $branchPrefix)
    {
        $branchesList = $devBranches->listBranches();
        $branchesCreatedByThisTestMethod = array_filter(
            $branchesList,
            function ($branch) use ($branchPrefix) {
                return strpos($branch['name'], $branchPrefix) === 0;
            }
        );
        foreach ($branchesCreatedByThisTestMethod as $branch) {
            $devBranches->deleteBranch($branch['id']);
        }
    }
}
