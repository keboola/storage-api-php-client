<?php

namespace Keboola\Test\Backend\SOX;

use Generator;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\Test\StorageApiTestCase;

class MergeRequestsTest extends StorageApiTestCase
{
    private Client $developerClient;
    private Client $prodManagerClient;
    private DevBranches $branches;

    public function setUp(): void
    {
        parent::setUp();
        $this->prodManagerClient = $this->getDefaultClient();
        $this->developerClient = $this->getDeveloperStorageApiClient();
        $this->branches = new DevBranches($this->developerClient);
        foreach ($this->branches->listBranches() as $branch) {
            if ($branch['isDefault'] !== true) {
                $this->branches->deleteBranch($branch['id']);
            }
        }

        $this->cleanupConfigurations($this->getDefaultBranchStorageApiClient());
    }

    public function testCreateMergeRequest(): void
    {
        $oldBranches = $this->branches->listBranches();
        $this->assertCount(1, $oldBranches);

        $newBranch = $this->branches->createBranch('aaaa');

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $oldBranches[0]['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $mrData = $this->developerClient->getMergeRequest($mrId);

        $this->assertEquals('Change everything', $mrData['title']);
        // check that detail also containts content
        $this->assertArrayHasKey('content', $mrData);
    }

    public function testCreateMergeRequestFromInvalidBranches(): void
    {
        $this->expectExceptionMessage('Cannot create merge request. Branch not found.');
        $this->developerClient->createMergeRequest([
            'branchFromId' => 123,
            'branchIntoId' => 345,
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);
    }

    public function testCreateMergeRequestIntoDevBranch(): void
    {
        $this->expectExceptionMessage('Cannot create merge request. Target branch is not default.');

        $oldBranches = $this->branches->listBranches();
        $this->assertCount(1, $oldBranches);

        $newBranch = $this->branches->createBranch('aaaa');

        $this->developerClient->createMergeRequest([
            'branchFromId' => $oldBranches[0]['id'],
            'branchIntoId' => $newBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);
    }

    public function testPutInReview(): void
    {
        $oldBranches = $this->branches->listBranches();
        $this->assertCount(1, $oldBranches);

        $newBranch = $this->branches->createBranch('aaaa');

        $title = 'Change everything ' . time();
        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $oldBranches[0]['id'],
            'title' => $title,
            'description' => 'Fix typo',
        ]);

        $list = $this->developerClient->listMergeRequests();
        self::assertSame($title, $list[0]['title']);

        $mrData = $this->developerClient->mergeRequestPutToReview($mrId);

        $this->assertEquals('in_review', $mrData['state']);
    }

    public function testMRWorkflowFromDevelopmentToCancel(): void
    {
        $oldBranches = $this->branches->listBranches();
        $this->assertCount(1, $oldBranches);

        $newBranch = $this->branches->createBranch('aaaa');

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $oldBranches[0]['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $reviewerClient = $this->getReviewerStorageApiClient();
        $this->developerClient->mergeRequestPutToReview($mrId);

        $mrData = $reviewerClient->mergeRequestAddApproval($mrId);

        $this->assertEquals('in_review', $mrData['state']);
        $this->assertCount(1, $mrData['approvals']);

        $mrData = $this->getSecondReviewerStorageApiClient()->mergeRequestAddApproval($mrId);

        $this->assertEquals('approved', $mrData['state']);
        $this->assertCount(2, $mrData['approvals']);

        $mrData = $reviewerClient->rejectMergeRequest($mrId);
        $this->assertCount(0, $mrData['approvals']);
        $this->assertSame('development', $mrData['state']);

        $mrData = $reviewerClient->cancelMergeRequest($mrId);
        $this->assertCount(0, $mrData['approvals']);
        $this->assertSame('canceled', $mrData['state']);
        $this->assertNull($mrData['branches']['branchFromId']);
    }

    public function testAddSingleApprovalOnly(): void
    {
        $oldBranches = $this->branches->listBranches();
        $this->assertCount(1, $oldBranches);

        $newBranch = $this->branches->createBranch('aaaa');

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $oldBranches[0]['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $reviewerClient = $this->getReviewerStorageApiClient();
        $this->developerClient->mergeRequestPutToReview($mrId);

        $mrData = $reviewerClient->mergeRequestAddApproval($mrId);

        $this->assertEquals('in_review', $mrData['state']);
        $this->assertCount(1, $mrData['approvals']);

        try {
            $mrData = $reviewerClient->mergeRequestAddApproval($mrId);
        } catch (ClientException $e) {
            $this->assertSame('Operation canot be performed due: This reviewer has already approved this request.', $e->getMessage());
        }
    }

    public function testProManagerCannotPutBranchInReview(): void
    {
        $oldBranches = $this->branches->listBranches();
        $this->assertCount(1, $oldBranches);

        $newBranch = $this->branches->createBranch('aaaa');

        try {
            $this->prodManagerClient->createMergeRequest([
                'branchFromId' => $newBranch['id'],
                'branchIntoId' => $oldBranches[0]['id'],
                'title' => 'Change everything',
                'description' => 'Fix typo',
            ]);
            $this->fail('Prod manager should not be able to create merge request');
        } catch (ClientException $e) {
            $this->assertSame($e->getMessage(), 'You don\'t have access to the resource.');
        }

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $oldBranches[0]['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        try {
            $this->prodManagerClient->mergeRequestPutToReview($mrId);
            $this->fail('Prod manager should not be able to put merge request in review');
        } catch (ClientException $e) {
            $this->assertSame($e->getMessage(), 'You don\'t have access to the resource.');
        }
    }

    public function testUpdateMR(): void
    {
        $oldBranches = $this->branches->listBranches();
        $this->assertCount(1, $oldBranches);

        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new Components($this->getDefaultBranchStorageApiClient());

        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc');
        $components->addConfiguration($configuration);

        $newBranch = $this->branches->createBranch('aaaa');

        $devBranchComponents = new Components($this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));
        $devBranchComponents->addConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('firstRow')
            ->setConfiguration(['value' => 1]));

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $oldBranches[0]['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        try {
            $this->prodManagerClient->updateMergeRequest(
                $mrId,
                'Lalala',
                'Trololo',
            );
            $this->fail('Prod manager should not be able to create merge request');
        } catch (ClientException $e) {
            $this->assertSame($e->getMessage(), 'You don\'t have access to the resource.');
        }

        $this->developerClient->mergeRequestPutToReview($mrId);

        try {
            $this->developerClient->updateMergeRequest(
                $mrId,
                'Lalala',
                'Trololo',
            );
            $this->fail('MR in review should not be able to update');
        } catch (ClientException $e) {
            $this->assertSame($e->getMessage(), 'You don\'t have access to the resource.');
        }

        $this->developerClient->rejectMergeRequest($mrId);
        $mr = $this->developerClient->updateMergeRequest(
            $mrId,
            'Lalala',
            'Trololo',
        );

        $this->assertSame('Lalala', $mr['title']);
        $this->assertSame('Trololo', $mr['description']);

        // different user should also be able to update it
        $mr = $this->getReviewerStorageApiClient()->updateMergeRequest(
            $mrId,
            'By reviewer',
            'With love to developer',
        );

        $this->assertSame('By reviewer', $mr['title']);
        $this->assertSame('With love to developer', $mr['description']);
    }

    /** @dataProvider cantMergeTokenProviders */
    public function testSpecificRolesCantMerge(Client $client): void
    {
        $oldBranches = $this->branches->listBranches();
        $this->assertCount(1, $oldBranches);

        $newBranch = $this->branches->createBranch('aaaa');

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $oldBranches[0]['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $reviewerClient = $this->getReviewerStorageApiClient();
        $this->developerClient->mergeRequestPutToReview($mrId);

        $mrData = $reviewerClient->mergeRequestAddApproval($mrId);

        $this->assertEquals('in_review', $mrData['state']);
        $this->assertCount(1, $mrData['approvals']);

        $mrData = $this->getSecondReviewerStorageApiClient()->mergeRequestAddApproval($mrId);
        $this->assertCount(2, $mrData['approvals']);
        $this->assertSame('approved', $mrData['state']);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('You don\'t have access to the resource.');
        $client->mergeMergeRequest($mrId);
    }

    public function cantMergeTokenProviders(): Generator
    {
        yield 'developer' => [
            $this->getDeveloperStorageApiClient(),
        ];
        yield 'reviewer' => [
            $this->getReviewerStorageApiClient(),
        ];
        yield 'readOnly' => [
            $this->getReadOnlyStorageApiClient(),
        ];
    }

    public function testMrWithConflictCantBeMergedButAfterResetCan(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new Components($this->getDefaultBranchStorageApiClient());

        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc');
        $components->addConfiguration($configuration);

        [$mrId, $branchId] = $this->createBranchMergeRequestAndApproveIt();
        // in default and dev branch is the same config with the same versionIdentifier

        // make change in default branch to create conflict
        $components->addConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('firstRow')
            ->setConfiguration(['value' => 1]));

        try {
            $this->prodManagerClient->mergeMergeRequest($mrId);
            $this->fail('Should fail, MR has conflict.');
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf('Merge request %s cannot be merged. Problem with following configurations: componentId: "wr-db", configurationId: "main-1"', $mrId),
                $e->getMessage()
            );
        }
        $mr = $this->developerClient->getMergeRequest($mrId);
        $this->assertEquals('approved', $mr['state']);

        $branchAwareDeveloperStorageClient = $this->getBranchAwareClient($branchId, [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        $components = new Components($branchAwareDeveloperStorageClient);
        $components->resetToDefault($componentId, $configurationId);

        // todo now is works like this, but maybe it should go through approval process again
        $this->prodManagerClient->mergeMergeRequest($mrId);
        $mr = $this->developerClient->getMergeRequest($mrId);
        $this->assertEquals('published', $mr['state']);
    }

    public function testConfigIsUpdatedInDefaultButBothConfigsAreDeleted(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new Components($this->getDefaultBranchStorageApiClient());

        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc');
        $components->addConfiguration($configuration);

        [$mrId, $branchId] = $this->createBranchMergeRequestAndApproveIt();
        // in default and dev branch is the same config with the same versionIdentifier

        // make change in default branch to create conflict
        $components->addConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('firstRow')
            ->setConfiguration(['value' => 1]));

        // Delete in default branch
        $components->deleteConfiguration($componentId, $configurationId);

        try {
            $this->prodManagerClient->mergeMergeRequest($mrId);
            $this->fail('Should fail, MR has conflict.');
        } catch (ClientException $e) {
            $this->assertSame(
                $e->getMessage(),
                sprintf('Merge request %s cannot be merged. Problem with following configurations: componentId: "wr-db", configurationId: "main-1"', $mrId)
            );
        }

        $devBranchComponents = new Components($this->getBranchAwareClient($branchId, [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));

        $devBranchComponents->deleteConfiguration($componentId, $configurationId);

        $this->prodManagerClient->mergeMergeRequest($mrId);
        $mr = $this->developerClient->getMergeRequest($mrId);
        $this->assertEquals('published', $mr['state']);
    }

    private function createBranchMergeRequestAndApproveIt(): array
    {
        $oldBranches = $this->branches->listBranches();
        $this->assertCount(1, $oldBranches);

        $newBranch = $this->branches->createBranch('aaaa');

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $oldBranches[0]['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $reviewerClient = $this->getReviewerStorageApiClient();
        $this->developerClient->mergeRequestPutToReview($mrId);

        $reviewerClient->mergeRequestAddApproval($mrId);
        $this->getSecondReviewerStorageApiClient()->mergeRequestAddApproval($mrId);

        return [$mrId, $newBranch['id']];
    }
}
