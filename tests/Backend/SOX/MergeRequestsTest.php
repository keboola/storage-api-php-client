<?php

namespace Keboola\Test\Backend\SOX;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
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

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $oldBranches[0]['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

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

        $mrData = $reviewerClient->mergeRequestAddApproval($mrId);
        $this->assertCount(2, $mrData['approvals']);
        $this->assertSame('approved', $mrData['state']);

        $mrData = $reviewerClient->rejectMergeRequest($mrId);
        $this->assertCount(0, $mrData['approvals']);
        $this->assertSame('development', $mrData['state']);

        $mrData = $reviewerClient->cancelMergeRequest($mrId);
        $this->assertCount(0, $mrData['approvals']);
        $this->assertSame('canceled', $mrData['state']);
        $this->assertSame(null, $mrData['branches']['branchFromId']);
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
}
