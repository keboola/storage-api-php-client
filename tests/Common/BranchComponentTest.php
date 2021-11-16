<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Event;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ConfigurationRowState;
use Keboola\StorageApi\Options\Components\ConfigurationState;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions;
use Keboola\Test\StorageApiTestCase;

class BranchComponentTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();

        $this->cleanupConfigurations();
    }

    public function testResetToDefault()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranchClient = new \Keboola\StorageApi\DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $this->deleteBranchesByPrefix($devBranchClient, $branchName);

        // create new configurations in main branch
        $componentId = 'transformation';
        $configurationId = 'main-1';
        $components = new Components($this->_client);
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main 1')
            ->setConfiguration(['test' => 'false']);

        $components->addConfiguration($configurationOptions);
        $row = $components->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Main 1 Row 1')
                ->setRowId('main-1-row-1')
        );
        $rowId = $row['id'];

        $originalConfiguration = $components->getConfiguration($componentId, $configurationId);
        $this->assertSame(2, $originalConfiguration['version']);

        $branch = $devBranchClient->createBranch($branchName);
        $branchClient = $this->getBranchAwareDefaultClient($branch['id']);
        $branchComponents = new Components($branchClient);
        $originalConfigurationInBranch = $branchComponents->getConfiguration($componentId, $configurationId);

        $this->assertEquals(
            $this->withoutKeysChangingInBranch($originalConfiguration),
            $this->withoutKeysChangingInBranch($originalConfigurationInBranch)
        );

        $components->updateConfiguration(
            $configurationOptions
                ->setName('Main updated')
                ->setConfiguration(['test' => 'true'])
        );

        $branchComponents->updateConfiguration(
            $configurationOptions
                ->setName('Main updated in branch')
                ->setConfiguration(['test' => 'true in branch'])
        );

        $updatedConfiguration = $components->getConfiguration($componentId, $configurationId);
        $updatedConfigurationInBranch = $branchComponents->getConfiguration($componentId, $configurationId);

        $this->assertSame(3, $updatedConfiguration['version']);
        $this->assertSame(2, $updatedConfigurationInBranch['version']);
        $this->assertNotEquals(
            $this->withoutKeysChangingInBranch($originalConfigurationInBranch),
            $this->withoutKeysChangingInBranch($updatedConfiguration)
        );
        $this->assertNotEquals(
            $this->withoutKeysChangingInBranch($updatedConfigurationInBranch),
            $this->withoutKeysChangingInBranch($updatedConfiguration)
        );

        try {
            $components->resetToDefault($componentId, $configurationId);
            $this->fail('Reset should not work for main branch');
        } catch (ClientException $e) {
            $this->assertSame('Reset to default branch is not implemented for default branch', $e->getMessage());
        }

        $branchComponents->updateConfiguration(
            $configurationOptions
                ->setName('Main updated')
                ->setConfiguration(['test' => 'true'])
        );

        $configurationVersionsInBranch = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId)
        );

        // can get configuration version 2
        $branchComponents->getConfigurationVersion($componentId, $configurationId, 2);

        $this->assertCount(3, $configurationVersionsInBranch);

        $branchComponents->resetToDefault($componentId, $configurationId);

        $resetConfigurationInBranch = $branchComponents->getConfiguration($componentId, $configurationId);

        $this->assertSame(1, $resetConfigurationInBranch['version']);

        try {
            $branchComponents->getConfigurationVersion($componentId, $configurationId, 2);
            $this->fail('Configuration version 2 should not be present, as it was reset to v1');
        } catch (ClientException $e) {
            $this->assertSame('Version 2 not found', $e->getMessage());
        }

        $this->assertArrayHasKey('created', $resetConfigurationInBranch);
        $this->assertNotEquals($updatedConfiguration['created'], $resetConfigurationInBranch['created']);

        $this->assertSame(1, $resetConfigurationInBranch['version']);
        $this->assertSame('Copied from default branch configuration "Main updated" (main-1) version 3', $resetConfigurationInBranch['changeDescription']);
        $this->assertSame('Copied from default branch configuration "Main updated" (main-1) version 3', $resetConfigurationInBranch['currentVersion']['changeDescription']);

        $this->assertCount(1, $resetConfigurationInBranch['rows']);
        $row = $resetConfigurationInBranch['rows'][0];
        $this->assertArrayHasKey('created', $row);
        $this->assertSame('Copied from default branch configuration row "Main 1 Row 1" (main-1-row-1) version 1', $row['changeDescription']);

        $this->assertSame(
            $this->withoutKeysChangingInBranch($updatedConfiguration),
            $this->withoutKeysChangingInBranch($resetConfigurationInBranch)
        );

        $resetConfigurationVersionsInBranch = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId)
        );

        $this->assertCount(1, $resetConfigurationVersionsInBranch);

        // delete config in dev branch

        $branchComponents->updateConfiguration(
            (new \Keboola\StorageApi\Options\Components\Configuration())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId)
                ->setName('Main updated')
                ->setConfiguration(['test' => 'true in branch'])
        );
        $configurationVersionsInBranch = $branchComponents->listConfigurationVersions(
            (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId)
        );
        $this->assertCount(2, $configurationVersionsInBranch);
        $branchComponents->getConfigurationVersion($componentId, $configurationId, 2);

        $branchComponents->deleteConfiguration($componentId, $configurationId);
        try {
            $branchComponents->getConfiguration($componentId, $configurationId);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame('Configuration main-1 not found', $e->getMessage());
        }

        $branchComponents->resetToDefault($componentId, $configurationId);
        $configurationAfterReset = $branchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame(1, $configurationAfterReset['version']);
        try {
            $branchComponents->getConfigurationVersion($componentId, $configurationId, 2);
            $this->fail('Configuration version 2 should not be present, as it was reset to v1');
        } catch (ClientException $e) {
            $this->assertSame('Version 2 not found', $e->getMessage());
        }

        // delete config in production

        // assert there is a row in both
        $row = $components->getConfigurationRow($componentId, $configurationId, $rowId);
        $this->assertSame($row['id'], $rowId);
        $branchRow = $branchComponents->getConfigurationRow($componentId, $configurationId, $rowId);
        $this->assertSame($row['id'], $rowId);

        // delete row in production and reset branch version
        $components->deleteConfigurationRow($componentId, $configurationId, $rowId);
        $branchComponents->resetToDefault($componentId, $configurationId);
        $configurationAfterReset = $branchComponents->getConfiguration($componentId, $configurationId);

        $this->assertCount(0, $configurationAfterReset['rows']);

        // make configuration in branch updated again and test reset to deleted production configuration
        $options = $configurationOptions->setName('Main updated in branch second time')
            ->setConfiguration(['test' => 'true in branch second time']);
        $branchComponents->updateConfiguration($options);
        $updatedConfigurationInBranch = $branchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame(2, $updatedConfigurationInBranch['version']);
        $components->deleteConfiguration($componentId, $configurationId);
        $branchComponents->resetToDefault($componentId, $configurationId);
        try {
            $updatedConfigurationInBranch = $branchComponents->getConfiguration($componentId, $configurationId);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame('Configuration main-1 not found', $e->getMessage());
        }

        // can be reset when existing default and deleted branch (new config in default scenario)
        // first restore branch soft deleted above so that it can be reset back to branch
        $components->restoreComponentConfiguration($componentId, $configurationId);
        // assert does not exist in branch
        try {
            $branchComponents->getConfiguration($componentId, $configurationId);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame('Configuration main-1 not found', $e->getMessage());
        }
        $branchComponents->resetToDefault($componentId, $configurationId);
        // assert that exists in branch (won't throw 404)
        $branchComponents->getConfiguration($componentId, $configurationId);

        // purge the deleted configuration
        // delete
        $components->deleteConfiguration($componentId, $configurationId);
        // purge
        $components->deleteConfiguration($componentId, $configurationId);

        // reset to purged version
        $branchComponents->resetToDefault($componentId, $configurationId);

        try {
            $updatedConfigurationInBranch = $branchComponents->getConfiguration($componentId, $configurationId);
            $this->fail('Should have thrown as reset to purged means deleted');
        } catch (ClientException $e) {
            $this->assertSame('Configuration main-1 not found', $e->getMessage());
        }
    }

    private function withoutKeysChangingInBranch($branch)
    {
        unset(
            $branch['created'],
            $branch['version'],
            $branch['changeDescription'],
            $branch['currentVersion']['changeDescription']
        );
        foreach ($branch['rows'] as &$row) {
            unset(
                $row['created'],
                $row['changeDescription']
            );
        }
        return $branch;
    }

    public function testManipulationWithComponentConfigurations()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $branch = $this->deleteBranchesByPrefix($devBranch, $branchName);

        // create new configurations in main branch
        $componentId = 'transformation';
        $components = new Components($this->_client);
        $configurationData = ['x' => 'y'];
        $configurationOptions = (new Configuration())
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

        $deletedConfigurationOptions = (new Configuration())
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

        $branchComponents = new Components($this->getBranchAwareDefaultClient($branch['id']));

        try {
            $branchComponents->getConfiguration($componentId, 'deleted-main');
            $this->fail('Configuration deleted in the main branch shouldn\'t exist in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration deleted-main not found', $e->getMessage());
        }

        $configFromMain = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertSame(1, $configFromMain['version']);

        // test is version created for devBranch configuration after create branch
        $configurationVersions = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
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

        // test config time created is different for branch config
        $configMain = $components->getConfiguration($componentId, 'main-1');
        $this->assertNotEquals($configMain['created'], $configFromMain['created']);
        $this->assertEquals('Copied from default branch configuration "Main 1" (main-1) version 2', $configFromMain['changeDescription']);

        $currentVersion = $configFromMain['currentVersion'];
        $this->assertEquals('Copied from default branch configuration "Main 1" (main-1) version 2', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

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

        $components->addConfiguration((new Configuration())
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

        // test is version created for devBranch after add new config row
        $configurationVersions = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId('main-1')
        );

        $this->assertCount(2, $configurationVersions);

        $configurationVersion = $branchComponents->getConfigurationVersion($componentId, 'main-1', 2);

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

        // test is version created for devBranch after add new config row with custom change description
        $configurationVersions = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId('main-1')
        );

        $this->assertCount(4, $configurationVersions);

        $configurationVersion = $branchComponents->getConfigurationVersion($componentId, 'main-1', 4);

        $this->assertArrayHasKey('version', $configurationVersion);
        $this->assertSame(4, $configurationVersion['version']);
        $this->assertIsInt($configurationVersion['creatorToken']['id']);
        $this->assertArrayNotHasKey('state', $configurationVersion);
        $this->assertSame('Custom change desc', $configurationVersion['changeDescription']);

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

        // test is version created for devBranch after update config row with custom change description
        $configurationVersions = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId('transformation')
                ->setConfigurationId('main-1')
        );

        $this->assertCount(5, $configurationVersions);

        $configurationVersion = $branchComponents->getConfigurationVersion('transformation', 'main-1', 5);

        $this->assertArrayHasKey('version', $configurationVersion);
        $this->assertSame(5, $configurationVersion['version']);
        $this->assertIsInt($configurationVersion['creatorToken']['id']);
        $this->assertArrayNotHasKey('state', $configurationVersion);
        $this->assertSame('Test change dev-1-row-1', $configurationVersion['changeDescription']);

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

        // test is version created for devBranch after add new config row with custom change description
        $configurationVersions = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId('transformation')
                ->setConfigurationId('main-1')
        );

        $this->assertCount(6, $configurationVersions);

        $configurationVersion = $branchComponents->getConfigurationVersion('transformation', 'main-1', 6);

        $this->assertArrayHasKey('version', $configurationVersion);
        $this->assertSame(6, $configurationVersion['version']);
        $this->assertIsInt($configurationVersion['creatorToken']['id']);
        $this->assertArrayNotHasKey('state', $configurationVersion);
        $this->assertSame('Row dev-1-row-1 changed', $configurationVersion['changeDescription']);

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
        $config = (new Configuration())
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

        // test create new config create new version for configuration
        $configurationVersions = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
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

        // test is version created for devBranch after update configuration
        $configurationVersions = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId($config->getComponentId())
                ->setConfigurationId($config->getConfigurationId())
        );

        $this->assertCount(3, $configurationVersions);
        $configurationVersion = $branchComponents->getConfigurationVersion($config->getComponentId(), $config->getConfigurationId(), 3);

        $this->assertArrayHasKey('version', $configurationVersion);
        $this->assertSame(3, $configurationVersion['version']);
        $this->assertIsInt($configurationVersion['creatorToken']['id']);
        $this->assertArrayNotHasKey('state', $configurationVersion);
        $this->assertArrayHasKey('configuration', $configurationVersion);
        $this->assertSame($configurationData, $configurationVersion['configuration']);
        $this->assertSame('Configuration updated', $configurationVersion['changeDescription']);

        $config = (new Configuration())
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

        // test is version created for devBranch after config update with custom change description
        $configurationVersions = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId('transformation')
                ->setConfigurationId('dev-branch-1')
        );

        $this->assertCount(4, $configurationVersions);
        $configurationVersion = $branchComponents->getConfigurationVersion('transformation', 'dev-branch-1', 4);

        $this->assertArrayHasKey('version', $configurationVersion);
        $this->assertSame(4, $configurationVersion['version']);
        $this->assertIsInt($configurationVersion['creatorToken']['id']);
        $this->assertArrayNotHasKey('state', $configurationVersion);
        $this->assertArrayHasKey('configuration', $configurationVersion);
        $this->assertSame($configurationData, $configurationVersion['configuration']);
        $this->assertSame('Custom change desc', $configurationVersion['changeDescription']);

        $state = [
            'cache' => false,
        ];

        $configState = (new ConfigurationState())
            ->setComponentId('transformation')
            ->setConfigurationId('dev-branch-1')
            ->setState($state)
        ;

        $updatedConfig = $branchComponents->updateConfigurationState($configState);
        $this->assertEquals($state, $updatedConfig['state']);

        $configuration = $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals($state, $configuration['state']);
        $this->assertEquals(4, $configuration['version']); // update state shouldn't change version

        $config = (new Configuration())
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

        $branchComponents->addConfiguration((new Configuration())
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

        // test is version created for devBranch configuration after create new config
        $configurationVersions = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
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

        $branchComponents->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('branch-2')
            ->setConfiguration(array('x' => 'y'))
            ->setName('Dev branch'));
        $branchComponents->addConfiguration((new Configuration())
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

        $config = (new Configuration())
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

    public function testDeleteBranchConfiguration()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $this->deleteBranchesByPrefix($devBranch, $branchName);

        // create new configurations in main branch
        $componentId = 'transformation';
        $components = new Components($this->_client);

        $configurationData = ['x' => 'y'];
        $configurationOptions = (new Configuration())
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

        // dummy branch to highlight potentially forgotten where on branch
        $devBranch->createBranch($branchName . '-dummy');

        // create dev branch
        $branch = $devBranch->createBranch($branchName);

        $branchComponents = new Components($this->getBranchAwareDefaultClient($branch['id']));

        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(1, $rows);

        $configurations = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );
        $this->assertCount(1, $configurations);

        // delete dev branch configuration
        $branchComponents->deleteConfiguration($componentId, 'main-1');

        $listConfigurationOptions = (new ListComponentConfigurationsOptions())->setComponentId($componentId);
        $configurations = $branchComponents->listComponentConfigurations($listConfigurationOptions);
        $this->assertCount(0, $configurations);

        $listConfigurationOptions->setIsDeleted(true);
        $configurations = $branchComponents->listComponentConfigurations($listConfigurationOptions);
        $this->assertCount(1, $configurations);

        // check dev branch
        try {
            $branchComponents->getConfiguration($componentId, 'main-1');
            $this->fail('Configuration should be deleted in the dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration main-1 not found', $e->getMessage());
        }

        try {
            $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId('main-1'));
            $this->fail('Configuration rows should not be listed in the dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration main-1 not found', $e->getMessage());
        }

        // check main branch
        $mainConfig = $components->getConfiguration($componentId, 'main-1');
        $this->assertSame(2, $mainConfig['version']);

        $mainConfigRows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(1, $mainConfigRows);

        // delete soft-deleted dev branch configuration
        try {
            $branchComponents->deleteConfiguration($componentId, 'main-1');
            $this->fail('Configuration should not be deleted in the dev branch');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertSame('storage.components.cannotDeleteConfiguration', $e->getStringCode());
            $this->assertSame('Deleting configuration from trash is not allowed in development branches.', $e->getMessage());
        }
    }

    public function testRestoreBranchConfiguration()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $this->deleteBranchesByPrefix($devBranch, $branchName);

        // create new configurations in main branch
        $componentId = 'transformation';
        $components = new Components($this->_client);

        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1')
            ->setName('Main 1')
            ->setConfiguration(['x' => 'y']);
        $components->addConfiguration($configurationOptions);
        $components->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Main 1 Row 1')
                ->setRowId('main-1-row-1')
        );

        // dummy branch to highlight potentially forgotten where on branch
        $devBranch->createBranch($branchName . '-dummy');

        // create dev branch
        $branch = $devBranch->createBranch($branchName);

        $branchComponents = new Components($this->getBranchAwareDefaultClient($branch['id']));

        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(1, $rows);

        $configurations = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );
        $this->assertCount(1, $configurations);

        // delete dev branch configuration
        $branchComponents->deleteConfiguration($componentId, 'main-1');

        $listConfigurationOptions = (new ListComponentConfigurationsOptions())->setComponentId($componentId);
        $configurations = $branchComponents->listComponentConfigurations($listConfigurationOptions);
        $this->assertCount(0, $configurations);

        $listConfigurationOptions->setIsDeleted(true);
        $configurations = $branchComponents->listComponentConfigurations($listConfigurationOptions);
        $this->assertCount(1, $configurations);

        // restore dev branch configuration
        $branchComponents->restoreComponentConfiguration($componentId, 'main-1');

        $listConfigurationOptions = (new ListComponentConfigurationsOptions())->setComponentId($componentId);
        $configurations = $branchComponents->listComponentConfigurations($listConfigurationOptions);
        $this->assertCount(1, $configurations);

        $listConfigurationOptions->setIsDeleted(true);
        $configurations = $branchComponents->listComponentConfigurations($listConfigurationOptions);
        $this->assertCount(0, $configurations);

        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(1, $rows);

        // try to restore again
        try {
            $branchComponents->restoreComponentConfiguration($componentId, 'main-1');
            $this->fail('Configuration should not be restored again in the dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Deleted configuration main-1 not found', $e->getMessage());
        }

        // delete dev branch configuration
        $branchComponents->deleteConfiguration($componentId, 'main-1');

        $listConfigurationOptions = (new ListComponentConfigurationsOptions())->setComponentId($componentId);
        $configurations = $branchComponents->listComponentConfigurations($listConfigurationOptions);
        $this->assertCount(0, $configurations);

        $listConfigurationOptions->setIsDeleted(true);
        $configurations = $branchComponents->listComponentConfigurations($listConfigurationOptions);
        $this->assertCount(1, $configurations);

        try {
            $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId('main-1'));
            $this->fail('Configuration rows for deleted configuration should not be listed');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration main-1 not found', $e->getMessage());
        }

        // restore dev branch configuration with create same configuration id
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1')
            ->setName('Main 1 restored')
            ->setConfiguration(['a' => 'b'])
            ->setChangeDescription('Config restored...');
        $branchComponents->addConfiguration($configurationOptions);

        $listConfigurationOptions = (new ListComponentConfigurationsOptions())->setComponentId($componentId);
        $configurations = $branchComponents->listComponentConfigurations($listConfigurationOptions);
        $this->assertCount(1, $configurations);

        $listConfigurationOptions->setIsDeleted(true);
        $configurations = $branchComponents->listComponentConfigurations($listConfigurationOptions);
        $this->assertCount(0, $configurations);

        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(0, $rows);

        $configuration = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertSame('main-1', $configuration['id']);
        $this->assertSame('Main 1 restored', $configuration['name']);
        $this->assertSame(['a' => 'b'], $configuration['configuration']);
        $this->assertSame('Config restored...', $configuration['changeDescription']);
        $this->assertSame(5, $configuration['version']);
    }

    public function testDeleteBranchConfigurationRow()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $this->deleteBranchesByPrefix($devBranch, $branchName);

        // create new configurations in main branch
        $componentId = 'transformation';
        $components = new Components($this->_client);

        $configurationData = ['x' => 'y'];
        $configurationOptions = (new Configuration())
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

        $mainConfig = $components->getConfiguration($componentId, 'main-1');
        $this->assertSame(2, $mainConfig['version']);

        $mainConfigRow = $components->getConfigurationRow($componentId, 'main-1', 'main-1-row-1');
        $this->assertSame(1, $mainConfigRow['version']);

        // dummy branch to highlight potentially forgotten where on branch
        $devBranch->createBranch($branchName . '-dummy');

        // create dev branch
        $branch = $devBranch->createBranch($branchName);

        $branchComponents = new Components($this->getBranchAwareDefaultClient($branch['id']));

        $branchConfig = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertSame(1, $branchConfig['version']);

        $branchConfigRow = $branchComponents->getConfigurationRow($componentId, 'main-1', 'main-1-row-1');
        $this->assertSame(1, $branchConfigRow['version']);

        $branchConfigRows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(1, $branchConfigRows);

        $branchConfig = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );
        $this->assertCount(1, $branchConfig);

        // delete dev branch configuration row
        $branchComponents->deleteConfigurationRow($componentId, 'main-1', 'main-1-row-1');

        // check dev branch
        $branchConfig = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertSame(2, $branchConfig['version']);

        try {
            $branchComponents->getConfigurationRow($componentId, 'main-1', 'main-1-row-1');
            $this->fail('Configuration row should be deleted in the dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Row main-1-row-1 not found', $e->getMessage());
        }

        $branchConfigRows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(0, $branchConfigRows);

        // check main branch
        $mainConfig = $components->getConfigurationRow($componentId, 'main-1', 'main-1-row-1');
        $this->assertSame(1, $mainConfig['version']);

        $mainConfigRows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(1, $mainConfigRows);

        // delete already deleted dev branch configuration row
        try {
            $branchComponents->deleteConfigurationRow($componentId, 'main-1', 'main-1-row-1');
            $this->fail('Configuration row should not be deleted in the dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Row main-1-row-1 not found', $e->getMessage());
        }

        // create second configuration row
        $configurationData = ['x' => 'y'];
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1')
            ->setName('Main 1')
            ->setConfiguration($configurationData);

        $branchComponents->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Main 1 Row 2')
                ->setRowId('main-1-row-2')
        );

        $mainConfig = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertSame(3, $mainConfig['version']);

        $mainConfigRow = $branchComponents->getConfigurationRow($componentId, 'main-1', 'main-1-row-2');
        $this->assertSame(1, $mainConfigRow['version']);

        // delete dev branch configuration row with change description
        $deleteRowChangeDescription = 'Delete a row...';
        $branchComponents->deleteConfigurationRow($componentId, 'main-1', 'main-1-row-2', $deleteRowChangeDescription);

        // check dev branch
        $branchConfig = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertSame(4, $branchConfig['version']);
        $this->assertSame($deleteRowChangeDescription, $branchConfig['changeDescription']);

        $branchConfigRows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(0, $branchConfigRows);
    }

    public function testComponentConfigRowVersionRollback()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $this->deleteBranchesByPrefix($devBranch, $branchName);
        $branch = $devBranch->createBranch($branchName);

        $componentsApi = new Components($this->getBranchAwareDefaultClient($branch['id']));

        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');
        $componentsApi->addConfiguration($configuration);

        $configurationRowV1 = new ConfigurationRow($configuration);
        $configurationRowV1->setRowId('main-1-1');
        $configurationRowV1->setConfiguration([
            'my-value' => 666,
        ]);
        $componentsApi->addConfigurationRow($configurationRowV1);

        $componentsApi->getConfiguration('wr-db', 'main-1');

        // update row 1st - without change
        $componentsApi->updateConfigurationRow($configurationRowV1);

        // update row V2
        $configurationRowV1
            ->setConfiguration([
                'test' => 1,
            ])
            ->setChangeDescription('some change');
        $configurationRowV2 = $componentsApi->updateConfigurationRow($configurationRowV1);

        // update row V3
        $configurationRowV1
            ->setConfiguration([
                'test' => 2
            ])
            ->setChangeDescription(null);
        $configurationRowV3 = $componentsApi->updateConfigurationRow($configurationRowV1);

        // rollback to V2 -> V4
        $configurationRowV4 = $componentsApi->rollbackConfigurationRow(
            'wr-db',
            'main-1',
            $configurationRowV1->getRowId(),
            2
        );

        $this->assertEquals(4, $configurationRowV4['version'], 'Rollback creates new version of the configuration');
        $this->assertEquals('Rollback to version 2', $configurationRowV4['changeDescription'], 'Rollback creates automatic description');
        $this->assertArrayEqualsExceptKeys($configurationRowV2, $configurationRowV4, [
            'version',
            'changeDescription'
        ]);

        // try same assert but load row from api
        $configurationRowV4 = $componentsApi->getConfigurationRow(
            'wr-db',
            'main-1',
            $configurationRowV1->getRowId()
        );
        $this->assertEquals(4, $configurationRowV4['version'], 'Rollback creates new version of the configuration');
        $this->assertEquals('Rollback to version 2', $configurationRowV4['changeDescription'], 'Rollback creates automatic description');
        $this->assertArrayEqualsExceptKeys($configurationRowV2, $configurationRowV4, [
            'version',
            'changeDescription',
        ]);

        $configuration = $componentsApi->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(5, $configuration['version']);
        $this->assertEquals('Row main-1-1 version 2 rollback', $configuration['changeDescription'], 'Rollback creates automatic description');

        // rollback to version 3
        $configurationRowV5 = $componentsApi->rollbackConfigurationRow(
            'wr-db',
            'main-1',
            $configurationRowV1->getRowId(),
            3,
            'Custom rollback message'
        );

        $this->assertEquals(5, $configurationRowV5['version'], 'Rollback creates new version of the row');
        $this->assertEquals('Custom rollback message', $configurationRowV5['changeDescription']);
        $this->assertArrayEqualsExceptKeys($configurationRowV3, $configurationRowV5, ['version', 'changeDescription']);

        $configuration = $componentsApi->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(6, $configuration['version']);
        $this->assertEquals('Custom rollback message', $configuration['changeDescription'], 'Rollback creates automatic description');

        $versions = $componentsApi->listConfigurationRowVersions(
            (new ListConfigurationRowVersionsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1')
                ->setRowId($configurationRowV1->getRowId())
        );

        $this->assertCount(5, $versions);
    }

    public function testVersionIncreaseWhenUpdate()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $this->deleteBranchesByPrefix($devBranch, $branchName);
        $branch = $devBranch->createBranch($branchName);

        $componentsApi = new Components($this->getBranchAwareDefaultClient($branch['id']));

        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $componentsApi->addConfiguration($configuration);

        $configurationRow = new ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $configurationRow->setConfiguration([
            'my-value' => 666,
        ]);
        $componentsApi->addConfigurationRow($configurationRow);

        $configurationRow = new ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');
        $configurationRow->setConfiguration([
            'my-value' => 666,
        ]);
        $componentsApi->addConfigurationRow($configurationRow);

        $componentConfiguration = $componentsApi->getConfiguration('wr-db', 'main-1');

        $this->assertEquals(3, $componentConfiguration['version']);

        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-1', 'main-1-2']);
        $componentsApi->updateConfiguration($configuration);

        $componentConfiguration = $componentsApi->getConfiguration('wr-db', 'main-1');

        $this->assertEquals(4, $componentConfiguration['version']);

        // calling the update once again without any change, the version should remain
        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-1', 'main-1-2']);
        $componentsApi->updateConfiguration($configuration);

        $componentConfiguration = $componentsApi->getConfiguration('wr-db', 'main-1');

        $this->assertEquals(4, $componentConfiguration['version']);
    }

    public function testConfigurationRollback()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $this->deleteBranchesByPrefix($devBranch, $branchName);
        $branch = $devBranch->createBranch($branchName);
        $branchClient = $this->getBranchAwareDefaultClient($branch['id']);
        $componentsApi = new Components($branchClient);

        // create configuration
        $configuration = (new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setConfiguration(['a' => 'b'])
            ->setName('Main');
        $configurationV1 = $componentsApi->addConfiguration($configuration);

        // add first row - conf V2
        $configurationRowOptions = new ConfigurationRow($configuration);
        $configurationRowOptions->setConfiguration(['first' => 1]);
        $configurationRowV1 = $componentsApi->addConfigurationRow($configurationRowOptions);

        $configurationV2 = $componentsApi->getConfiguration('wr-db', $configurationV1['id']);

        // add another row  - conf V3
        $configurationRowOptions = new ConfigurationRow($configuration);
        $configurationRowOptions->setConfiguration(['second' => 1]);
        $configurationRowV2 = $componentsApi->addConfigurationRow($configurationRowOptions);

        // update first row - conf V4
        $configurationRowOptions = new ConfigurationRow($configuration);
        $configurationRowOptions->setConfiguration(['first' => 22])->setRowId($configurationRowV1['id']);
        $configurationRowV3 = $componentsApi->updateConfigurationRow($configurationRowOptions);

        // update config - conf V5
        $componentsApi->updateConfiguration($configuration->setConfiguration(['d' => 'b']));
        $configurationV5 = $componentsApi->getConfiguration('wr-db', $configurationV1['id']);

        // wait a moment, rollbacked version should have different created date
        sleep(2);

        // rollback to version 2 - conf V6
        // second row should be missing, and first row should be rolled back to first version
        $componentsApi->rollbackConfiguration('wr-db', $configurationV1['id'], 2);
        $this->createAndWaitForEvent((new Event())->setComponent('dummy')->setMessage('dummy'));
        $events = $branchClient->listEvents([
            'component' => 'storage',
            'q' => 'storage.componentConfigurationRolledBack',
        ]);
        $lastEvent = $events[0];
        $this->assertEquals($branch['id'], $lastEvent['idBranch']);
        $rollbackedConfiguration = $componentsApi->getConfiguration('wr-db', $configurationV1['id']);

        // asserts about the configuration itself
        $this->assertEquals(6, $rollbackedConfiguration['version'], 'Rollback added new configuration version');
        $this->assertEquals('Rollback to version 2', $rollbackedConfiguration['changeDescription']);
        $this->assertCount(1, $rollbackedConfiguration['rows']);
        $this->assertEquals('Rollback to version 2', $rollbackedConfiguration['currentVersion']['changeDescription']);
        $this->assertArrayEqualsExceptKeys($configurationV2['currentVersion'], $rollbackedConfiguration['currentVersion'], [
            'created',
            'changeDescription',
        ]);
        $this->assertArrayEqualsExceptKeys($configurationV2, $rollbackedConfiguration, [
            'version',
            'changeDescription',
            'rows',
            'currentVersion',
        ]);

        // asserts about configuration's rows
        $this->assertCount(1, $rollbackedConfiguration['rows']);
        $rollbackedRow = $rollbackedConfiguration['rows'][0];
        $this->assertEquals(3, $rollbackedRow['version']);
        $this->assertEquals('Rollback to version 1 (via configuration rollback to version 2)', $rollbackedRow['changeDescription']);
        $this->assertArrayEqualsExceptKeys($configurationRowV1, $rollbackedRow, [
            'version',
            'changeDescription',
        ]);

        // rollback to version 5 - conf V7
        $componentsApi->rollbackConfiguration('wr-db', $configurationV1['id'], 5, 'custom description');
        $this->createAndWaitForEvent((new Event())->setComponent('dummy')->setMessage('dummy'));
        $events = $branchClient->listEvents([
            'component' => 'storage',
            'q' => 'storage.componentConfigurationRolledBack',
            'sinceId' => $lastEvent['id'],
        ]);
        $lastEvent = $events[0];
        $this->assertEquals($branch['id'], $lastEvent['idBranch']);
        $rollbackedConfiguration = $componentsApi->getConfiguration('wr-db', $configurationV1['id']);
        // asserts about the configuration itself
        $this->assertEquals(7, $rollbackedConfiguration['version'], 'Rollback added new configuration version');
        $this->assertEquals('custom description', $rollbackedConfiguration['changeDescription']);
        $this->assertCount(2, $rollbackedConfiguration['rows']);
        $this->assertEquals('custom description', $rollbackedConfiguration['currentVersion']['changeDescription']);
        $this->assertArrayEqualsExceptKeys($configurationV5['currentVersion'], $rollbackedConfiguration['currentVersion'], [
            'created',
            'changeDescription',
        ]);
        $this->assertArrayEqualsExceptKeys($configurationV5, $rollbackedConfiguration, [
            'version',
            'changeDescription',
            'rows',
            'currentVersion',
        ]);
    }

    public function testRowChangesAfterRowCopy()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $this->deleteBranchesByPrefix($devBranch, $branchName);
        $branch = $devBranch->createBranch($branchName);

        $components = new Components($this->getBranchAwareDefaultClient($branch['id']));

        // config version 1
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("name")
            ->setDescription("description");
        $components->addConfiguration($config);

        // config version 2, row version 1
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setState(['rowStateKey' => 'rowStateValue']);
        $createdRow = $components->addConfigurationRow($rowConfig);

        // config version 3, row version 2
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId($createdRow["id"]);
        $rowConfig->setName("name");
        $rowConfig->setDescription("description");
        $rowConfig->setIsDisabled(true);
        $components->updateConfigurationRow($rowConfig);

        // copy row version 1
        $createdRow2 = $components->createConfigurationRowFromVersion('wr-db', $config->getConfigurationId(), $createdRow["id"], 1);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertStringMatchesFormat('Row %d copied from configuration "name" (main-1) row %d version 1', $response['changeDescription']);

        $row1 = $response["rows"][0];
        $this->assertEquals($createdRow["id"], $row1["id"]);
        $this->assertEquals("name", $row1["name"]);
        $this->assertEquals("description", $row1["description"]);
        $this->assertEquals("Row {$createdRow["id"]} changed", $row1["changeDescription"]);
        $this->assertEquals(true, $row1["isDisabled"]);
        $this->assertNotEmpty($row1['state']);

        $row2 = $response["rows"][1];
        $this->assertEquals($createdRow2["id"], $row2["id"]);
        $this->assertEquals("", $row2["name"]);
        $this->assertEquals("", $row2["description"]);
        $this->assertStringMatchesFormat('Copied from configuration "name" (main-1) row %d version 1', $row2["changeDescription"]);
        $this->assertEquals(false, $row2["isDisabled"]);
        $this->assertEmpty($row2['state']);

        // copy row version 2
        $createdRow3 = $components->createConfigurationRowFromVersion('wr-db', $config->getConfigurationId(), $createdRow["id"], 2);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertStringMatchesFormat('Row %d copied from configuration "name" (main-1) row %d version 2', $response['changeDescription']);

        $row1 = $response["rows"][0];
        $this->assertEquals($createdRow["id"], $row1["id"]);
        $this->assertEquals("name", $row1["name"]);
        $this->assertEquals("description", $row1["description"]);
        $this->assertEquals("Row {$createdRow["id"]} changed", $row1["changeDescription"]);
        $this->assertEquals(true, $row1["isDisabled"]);
        $this->assertNotEmpty($row1['state']);

        $row2 = $response["rows"][1];
        $this->assertEquals($createdRow2["id"], $row2["id"]);
        $this->assertEquals("", $row2["name"]);
        $this->assertEquals("", $row2["description"]);
        $this->assertStringMatchesFormat('Copied from configuration "name" (main-1) row %d version 1', $row2["changeDescription"]);
        $this->assertEquals(false, $row2["isDisabled"]);
        $this->assertEmpty($row2['state']);

        $row3 = $response["rows"][2];
        $this->assertEquals($createdRow3["id"], $row3["id"]);
        $this->assertEquals("name", $row3["name"]);
        $this->assertEquals("description", $row3["description"]);
        $this->assertStringMatchesFormat('Copied from configuration "name" (main-1) row %d version 2', $row3["changeDescription"]);
        $this->assertEquals(true, $row3["isDisabled"]);
        $this->assertEmpty($row3['state']);
    }

    public function testRowChangesAfterConfigurationCopy()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $this->deleteBranchesByPrefix($devBranch, $branchName);
        $branch = $devBranch->createBranch($branchName);

        $components = new Components($this->getBranchAwareDefaultClient($branch['id']));

        // config version 1
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("name")
            ->setDescription("description");
        $components->addConfiguration($config);

        // config version 2 - create row 1
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $firstRow = $components->addConfigurationRow($rowConfig);

        // config version 3 - update row 1
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId($firstRow["id"]);
        $rowConfig->setName("first name");
        $rowConfig->setDescription("first description");
        $rowConfig->setIsDisabled(true);
        $components->updateConfigurationRow($rowConfig);

        // config version 4 - create row 2
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setName('second name');
        $rowConfig->setDescription('second description');
        $components->addConfigurationRow($rowConfig);

        // config version 5 - delete row 1
        $components->deleteConfigurationRow('wr-db', 'main-1', $firstRow['id']);

        // copy config version 2
        $copiedConfig = $components->createConfigurationFromVersion('wr-db', $config->getConfigurationId(), 2, 'test');
        $response = $components->getConfiguration('wr-db', $copiedConfig["id"]);
        $this->assertSame('test', $response['name']);
        $this->assertSame('description', $response['description']);
        $this->assertSame('Copied from configuration "name" (main-1) version 2', $response['changeDescription']);
        // check rows
        $this->assertCount(1, $response['rows']);
        $this->assertEquals('', $response["rows"][0]["name"]);
        $this->assertEquals('', $response["rows"][0]["description"]);
        $this->assertEquals('Copied from configuration "name" (main-1) version 2', $response["rows"][0]["changeDescription"]);
        $this->assertEquals(false, $response["rows"][0]["isDisabled"]);

        // copy config version 4
        $copiedConfig = $components->createConfigurationFromVersion('wr-db', $config->getConfigurationId(), 4, 'test', 'some description', 'some change descripton');
        $response = $components->getConfiguration('wr-db', $copiedConfig["id"]);
        $this->assertSame('test', $response['name']);
        $this->assertSame('some description', $response['description']);
        $this->assertSame('some change descripton', $response['changeDescription']);
        // check rows
        $this->assertCount(2, $response['rows']);
        $this->assertEquals('first name', $response["rows"][0]["name"]);
        $this->assertEquals('first description', $response["rows"][0]["description"]);
        $this->assertEquals('Copied from configuration "name" (main-1) version 4', $response["rows"][0]["changeDescription"]);
        $this->assertEquals(true, $response["rows"][0]["isDisabled"]);
        $this->assertEquals('second name', $response["rows"][1]["name"]);
        $this->assertEquals('second description', $response["rows"][1]["description"]);
        $this->assertEquals('Copied from configuration "name" (main-1) version 4', $response["rows"][1]["changeDescription"]);
    }
}
