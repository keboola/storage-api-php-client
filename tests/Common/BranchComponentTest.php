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
    public function setUp(): void
    {
        parent::setUp();

        $this->cleanupConfigurations($this->_client);
    }

    /**
     * @return void
     */
    public function testCreateFromVersionCreateRowsAndVersions(): void
    {
        $componentId = 'transformation';
        $configurationId = 'main-1';
        $components = new Components($this->_client);
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main 1')
            ->setConfiguration(['test' => 'false']);

        $components->addConfiguration($configurationOptions);
        $components->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Main 1 Row 1')
                ->setRowId('main-1-row-1'),
        );

        $components->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Main 1 Row 2')
                ->setRowId('main-1-row-2'),
        );

        $newConfig = $components->createConfigurationFromVersion($componentId, $configurationId, 3, 'Copy version 3');

        // check that new config has different version identifier
        $mainConfig = $components->getConfiguration($componentId, $configurationId);
        $mainVersionIdentifier = $mainConfig['currentVersion']['versionIdentifier'];
        $newConfigDetail = $components->getConfiguration($componentId, $newConfig['id']);
        $newConfigVuid1 = $newConfigDetail['currentVersion']['versionIdentifier'];
        $this->assertNotEquals($mainVersionIdentifier, $newConfigVuid1);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(2, $rows);

        $rowMain1Row1 = $components->getConfigurationRow($componentId, $newConfig['id'], 'main-1-row-1');
        $this->assertArrayHasKey('id', $rowMain1Row1);
        $this->assertSame(1, $rowMain1Row1['version']);

        $rowMain1Row2 = $components->getConfigurationRow($componentId, $newConfig['id'], 'main-1-row-2');
        $this->assertArrayHasKey('id', $rowMain1Row2);
        $this->assertSame(1, $rowMain1Row2['version']);

        $rowMain1Row1Version = $components->getConfigurationRowVersion($componentId, $newConfig['id'], 'main-1-row-1', 1);
        $this->assertNotNull($rowMain1Row1Version);
        $newConfig = $components->getConfiguration($componentId, $newConfig['id']);
        $this->assertSame(1, $newConfig['version']);

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($newConfig['id']);

        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId('main-1-row-2');
        $rowConfig->setDescription('Desc manually updated');
        $components->updateConfigurationRow($rowConfig);

        $configData = $components->getConfigurationVersion($componentId, $configurationId, 2);
        $newConfigVuid2 = $configData['versionIdentifier'];
        $this->assertNotEquals(
            $newConfigVuid1,
            $newConfigVuid2,
            'Updated configuration should have different version identifier',
        );
        $this->assertArrayHasKey('rows', $configData);
        foreach ($configData['rows'] as $row) {
            $this->assertArrayHasKey('configuration', $row);
        }

        $rowMain1Row2 = $components->getConfigurationRow($componentId, $newConfig['id'], 'main-1-row-2');
        $this->assertSame('Desc manually updated', $rowMain1Row2['description']);

        $components->deleteConfigurationRow($componentId, $newConfig['id'], 'main-1-row-2');

        try {
            $components->getConfigurationRow($componentId, $newConfig['id'], 'main-1-row-2');
            $this->fail('Configuration row should not be deleted in the dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertStringContainsString('Row main-1-row-2 not found', $e->getMessage());
        }
    }

    public function testResetToDefault(): void
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
                ->setRowId('main-1-row-1'),
        );
        $rowId = $row['id'];

        $originalConfiguration = $components->getConfiguration($componentId, $configurationId);
        $this->assertSame(2, $originalConfiguration['version']);
        // will fail after consolidate branches
        $this->assertNotContains('isDisabled', $originalConfiguration);

        // create dev branch
        $branch = $devBranchClient->createBranch($branchName);
        $branchClient = $this->getBranchAwareDefaultClient($branch['id']);
        $branchComponents = new Components($branchClient);
        $originalConfigurationInBranch = $branchComponents->getConfiguration($componentId, $configurationId);

        // If create a new branch, config should be the same, also version identifier
        $this->assertEquals(
            $this->withoutKeysChangingInBranch($originalConfiguration),
            $this->withoutKeysChangingInBranch($originalConfigurationInBranch),
        );

        // update configuration in main branch (version 3)
        $components->updateConfiguration(
            $configurationOptions
                ->setName('Main updated')
                ->setConfiguration(['test' => 'true']),
        );

        // update configuration in dev branch (version 2)
        $branchComponents->updateConfiguration(
            $configurationOptions
                ->setName('Main updated in branch')
                ->setConfiguration(['test' => 'true in branch']),
        );

        $updatedConfiguration = $components->getConfiguration($componentId, $configurationId);
        $updatedConfigurationInBranch = $branchComponents->getConfiguration($componentId, $configurationId);
        $this->assertNotSame(
            $originalConfigurationInBranch['currentVersion']['versionIdentifier'],
            $updatedConfigurationInBranch['currentVersion']['versionIdentifier'],
            'If update configuration in branch, version identifier should change',
        );

        $this->assertSame(3, $updatedConfiguration['version']);
        $this->assertSame(2, $updatedConfigurationInBranch['version']);
        $this->assertNotEquals(
            $this->withoutKeysChangingInBranch($originalConfigurationInBranch),
            $this->withoutKeysChangingInBranch($updatedConfiguration),
        );
        $this->assertNotEquals(
            $this->withoutKeysChangingInBranch($updatedConfigurationInBranch),
            $this->withoutKeysChangingInBranch($updatedConfiguration),
        );

        // reset to default does not work for main branch
        try {
            $components->resetToDefault($componentId, $configurationId);
            $this->fail('Reset should not work for main branch');
        } catch (ClientException $e) {
            $this->assertSame('Reset to default branch is not implemented for default branch', $e->getMessage());
        }

        // update configuration in dev branch (version 3)
        $branchComponents->updateConfiguration(
            $configurationOptions
                ->setName('Main updated')
                ->setConfiguration(['test' => 'true']),
        );

        $configurationVersionsInBranch = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId),
        );

        // can get configuration version 2
        $branchComponents->getConfigurationVersion($componentId, $configurationId, 2);

        $this->assertCount(3, $configurationVersionsInBranch);

        // reset to default
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
            $this->withoutKeysChangingInBranch($resetConfigurationInBranch),
        );

        $resetConfigurationVersionsInBranch = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId),
        );

        $this->assertCount(1, $resetConfigurationVersionsInBranch);

        // delete config in dev branch

        $branchComponents->updateConfiguration(
            (new \Keboola\StorageApi\Options\Components\Configuration())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId)
                ->setName('Main updated')
                ->setConfiguration(['test' => 'true in branch']),
        );
        $configurationVersionsInBranch = $branchComponents->listConfigurationVersions(
            (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId),
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

        $this->assertSame(
            $this->withoutKeysChangingInBranch($updatedConfiguration),
            $this->withoutKeysChangingInBranch($configurationAfterReset),
        );
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
        $updatedConfiguration = $components->getConfiguration($componentId, $configurationId);
        // assert does not exist in branch
        try {
            $branchComponents->getConfiguration($componentId, $configurationId);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame('Configuration main-1 not found', $e->getMessage());
        }
        $branchComponents->resetToDefault($componentId, $configurationId);
        // assert that exists in branch (won't throw 404)
        $configurationAfterReset = $branchComponents->getConfiguration($componentId, $configurationId);

        $this->assertSame(
            $this->withoutKeysChangingInBranch($updatedConfiguration),
            $this->withoutKeysChangingInBranch($configurationAfterReset),
        );

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
            $branch['isDisabled'],
            $branch['currentVersion']['changeDescription'],
        );
        foreach ($branch['rows'] as &$row) {
            unset(
                $row['created'],
                $row['changeDescription'],
            );
        }
        return $branch;
    }

    public function testManipulationWithComponentConfigurations(): void
    {
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
                ->setRowId('main-1-row-1'),
        );
        // be sure that create time of copied row is different
        sleep(1);

        $deletedConfigurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('deleted-main')
            ->setName('Deleted Main');
        $components->addConfiguration($deletedConfigurationOptions);
        $components->addConfigurationRow(
            (new ConfigurationRow($deletedConfigurationOptions))
                ->setName('Deleted Main Row 1')
                ->setRowId('deleted-main-row-1'),
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
            $this->assertStringContainsString('Configuration deleted-main not found', $e->getMessage());
        }

        $branch = $this->createDevBranchForTestCase($this);

        $branchComponents = new Components($this->getBranchAwareDefaultClient($branch['id']));

        try {
            $branchComponents->getConfiguration($componentId, 'deleted-main');
            $this->fail('Configuration deleted in the main branch shouldn\'t exist in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertStringContainsString('Configuration deleted-main not found', $e->getMessage());
        }

        $configFromMain = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertSame(1, $configFromMain['version']);

        // test is version created for devBranch configuration after create branch
        $configurationVersions = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId('main-1'),
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
            $configurationVersion['changeDescription'],
        );

        // test config time created is different for branch config
        $configMain = $components->getConfiguration($componentId, 'main-1');
        $this->assertNotEquals($configMain['created'], $configFromMain['created']);
        $this->assertEquals('Copied from default branch configuration "Main 1" (main-1) version 2', $configFromMain['changeDescription']);

        // but version identifier is same
        $this->assertSame(
            $this->withoutKeysChangingInBranch($configMain),
            $this->withoutKeysChangingInBranch($configFromMain),
        );

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
            'main-1-row-1',
        );

        $this->assertEquals('main-1-row-1', $row['id']);
        $this->assertEquals(1, $row['version']);
        $this->assertEquals('Copied from default branch configuration row "Main 1 Row 1" (main-1-row-1) version 1', $row['changeDescription']);

        $mainBranchRow = $components->getConfigurationRow(
            $componentId,
            'main-1',
            'main-1-row-1',
        );

        // test config row time created is different for branch config
        $this->assertNotEquals($mainBranchRow['created'], $row['created']);

        $branchConfigs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId),
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
                ->setRowId('main-1-row-2'),
        );

        // update version in main should generate new version identifier in main
        $configMain = $components->getConfiguration($componentId, 'main-1');
        $configFromMain = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertNotSame(
            $this->withoutKeysChangingInBranch($configMain),
            $this->withoutKeysChangingInBranch($configFromMain),
        );

        // Check new config rows added to main branch
        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(2, $rows);

        $configs = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId),
        );
        // two configuration was created in main branch
        $this->assertCount(2, $configs);

        $branchConfigs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId),
        );

        // Creating new configuration row in main branch shouldn't create new configuration row in dev branch
        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(1, $rows);

        $row = $branchComponents->getConfigurationRow(
            $componentId,
            'main-1',
            'main-1-row-1',
        );

        $this->assertEquals('main-1-row-1', $row['id']);

        try {
            $branchComponents->getConfigurationRow(
                $componentId,
                'main-1',
                'main-1-row-2',
            );
            $this->fail('Configuration row created in main branch shouldn\'t exist in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertStringContainsString('Row main-1-row-2 not found', $e->getMessage());
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
            $this->assertStringContainsString('Configuration main-2 not found', $e->getMessage());
        }

        $mainComponentDetail  = $components->getConfiguration($componentId, 'main-1');
        $this->assertSame('main-1', $mainComponentDetail['id']);

        // add two config rows to test rowsSortOrder
        $branchComponents->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Dev 1 Row 1')
                ->setRowId('dev-1-row-1'),
        );

        // test is version created for devBranch after add new config row
        $configurationVersions = $branchComponents->listConfigurationVersions(
            (new ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId('main-1'),
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
            $configurationVersion['changeDescription'],
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
                ->setChangeDescription('Custom change desc'),
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
                ->setConfigurationId('main-1'),
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
                ->setChangeDescription('Test change dev-1-row-1'),
        );

        $updatedRow = $branchComponents->getConfigurationRow(
            $componentId,
            'main-1',
            'dev-1-row-1',
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
                ->setConfigurationId('main-1'),
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
                ->setConfiguration('{"id":"10","stuff":"true"}'),
        );

        $updatedRow = $branchComponents->getConfigurationRow(
            $componentId,
            'main-1',
            'dev-1-row-1',
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
                ->setConfigurationId('main-1'),
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
                    ]),
            );
            $this->fail('Update of row state should be restricted.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals(
                'Using \'state\' parameter on configuration row update is restricted for dev/branch context. Use direct API call.',
                $e->getMessage(),
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
            $rowState->getRowId(),
        );

        $this->assertEquals($state, $row['state']);
        $this->assertEquals(3, $row['version']); // update state shouldn't update version

        $updatedConfiguration = $branchComponents->getConfiguration(
            $componentId,
            'main-1',
        );
        $this->assertEquals('Row dev-1-row-1 changed', $updatedConfiguration['changeDescription']);

        try {
            $components->getConfigurationRow(
                $componentId,
                'main-1',
                'dev-1-row-1',
            );
            $this->fail('Configuration row created in dev branch shouldn\'t exist in main branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertStringContainsString('Row dev-1-row-1 not found', $e->getMessage());
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
                ->setConfigurationId('dev-branch-1'),
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
            $configurationVersion['changeDescription'],
        );

        $configs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId),
        );

        $this->assertCount(2, $configs);

        try {
            $components->getConfiguration('transformation', 'dev-branch-1');
            $this->fail('Configuration created in dev branch shouldn\'t exist in main branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertStringContainsString('Configuration dev-branch-1 not found', $e->getMessage());
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
                ->setConfigurationId($config->getConfigurationId()),
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
                ->setConfigurationId('dev-branch-1'),
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
                ->setConfigurationId('branch-1'),
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
            $configurationVersion['changeDescription'],
        );

        $branchComponents->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('branch-2')
            ->setConfiguration(['x' => 'y'])
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
            ->setInclude(['configuration']));

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
                $e->getMessage(),
            );
        }

        $this->assertEquals(
            $configuration,
            $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId()),
        );
    }

    public function testDeleteBranchConfiguration(): void
    {
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
                ->setRowId('main-1-row-1'),
        );

        // create dev branch
        $branch = $this->createDevBranchForTestCase($this);

        $branchComponents = new Components($this->getBranchAwareDefaultClient($branch['id']));

        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(1, $rows);

        $configurations = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId),
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
            $this->assertStringContainsString('Configuration main-1 not found', $e->getMessage());
        }

        try {
            $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId('main-1'));
            $this->fail('Configuration rows should not be listed in the dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertStringContainsString('Configuration main-1 not found', $e->getMessage());
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
            $this->assertSame(
                'Deleting configuration from trash is not allowed in development branches.',
                $e->getMessage(),
            );
        }
    }

    public function testDeleteBranchConfigurationRow(): void
    {
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
                ->setRowId('main-1-row-1'),
        );

        $mainConfig = $components->getConfiguration($componentId, 'main-1');
        $this->assertSame(2, $mainConfig['version']);

        $mainConfigRow = $components->getConfigurationRow($componentId, 'main-1', 'main-1-row-1');
        $this->assertSame(1, $mainConfigRow['version']);

        $branch = $this->createDevBranchForTestCase($this);

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
            (new ListComponentConfigurationsOptions())->setComponentId($componentId),
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
            $this->assertStringContainsString('Row main-1-row-1 not found', $e->getMessage());
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
            $this->assertStringContainsString('Row main-1-row-1 not found', $e->getMessage());
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
                ->setRowId('main-1-row-2'),
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
}
