<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\Test\StorageApiTestCase;

class SOXCommonTest extends StorageApiTestCase
{
    public function testCreateBucketInDefaultBranch(): void
    {
        $client = $this->getDefaultBranchStorageApiClient();
        $token = $client->verifyToken();
        $this->assertArrayNotHasKey('admin', $token);
        $this->assertTrue($token['canManageProtectedDefaultBranch']);
        $this->dropBucketIfExists($client, 'in.c-test', true);
        $bucketId = $client->createBucket('test', 'in');
        $client->dropBucket($bucketId);

        $client = $this->getDefaultClient();
        $this->assertSame('productionManager', $client->verifyToken()['admin']['role']);
        try {
            $client->createBucket('test', 'in');
            $this->fail('Production manager can\'t create bucket in default branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
        $client = $this->getDeveloperStorageApiClient();
        $this->assertSame('developer', $client->verifyToken()['admin']['role']);
        try {
            $client->createBucket('test', 'in');
            $this->fail('Developer can\'t create bucket in default branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
        $client = $this->getReviewerStorageApiClient();
        $this->assertSame('reviewer', $client->verifyToken()['admin']['role']);
        try {
            $client->createBucket('test', 'in');
            $this->fail('Reviewer can\'t create bucket in default branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
        $client = $this->getReadOnlyStorageApiClient();
        $this->assertSame('readOnly', $client->verifyToken()['admin']['role']);
        try {
            $client->createBucket('test', 'in');
            $this->fail('Read only can\'t create bucket in default branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
    }

    public function testRowVersionIsCreatedByTokenWhichCreateBranch(): void
    {
        $privilegedClient = $this->getDefaultBranchStorageApiClient();
        $privilegedTokenInfo = $privilegedClient->verifyToken();
        $this->cleanupTestBranches($this->getDeveloperStorageApiClient());
        $this->cleanupConfigurations($privilegedClient);

        // create new config and row
        $componentId = 'transformation';
        $configurationId = 'main-1';
        $productionComponents = new Components($privilegedClient);
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main 1')
            ->setConfiguration(['test' => 'false']);

        $productionComponents->addConfiguration($configurationOptions);
        $productionRow = $productionComponents->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Main 1 Row 1')
                ->setRowId('main-1-row-1'),
        );

        // version should be created by privileged token
        $productionVersion = $productionComponents->getConfigurationRowVersion(
            'transformation',
            'main-1',
            $productionRow['id'],
            1,
        );
        $this->assertEquals($privilegedTokenInfo['id'], $productionVersion['creatorToken']['id']);
        $this->assertEquals($privilegedTokenInfo['description'], $productionVersion['creatorToken']['description']);

        $developerStorageApiClient = $this->getDeveloperStorageApiClient();
        $devBranchDeveloperClient = new DevBranches($developerStorageApiClient);

        // create new branch to copy all configs to branch
        $branch = $devBranchDeveloperClient->createBranch($this->generateDescriptionForTestObject());

        $branchComponents = new Components($developerStorageApiClient->getBranchAwareClient($branch['id']));
        $developerToken = $developerStorageApiClient->verifyToken();

        $branchRowVersion = $branchComponents->getConfigurationRowVersion(
            'transformation',
            'main-1',
            $productionRow['id'],
            1,
        );

        // version should be created by developer token in branch
        $this->assertEquals($developerToken['id'], $branchRowVersion['creatorToken']['id']);
        $this->assertEquals($developerToken['description'], $branchRowVersion['creatorToken']['description']);
    }
}
