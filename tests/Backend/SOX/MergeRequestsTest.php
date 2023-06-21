<?php

namespace Keboola\Test\Backend\SOX;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;

class MergeRequestsTest extends StorageApiTestCase
{
    private Client $developerClient;
    private DevBranches $branches;

    public function setUp(): void
    {
        parent::setUp();
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
}
