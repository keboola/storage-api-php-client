<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;

class MergeRequestsTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();

        $branches = new DevBranches($this->_client);
        foreach ($branches->listBranches() as $branch) {
            if ($branch['isDefault'] !== true) {
                $branches->deleteBranch($branch['id']);
            }
        }
    }

    public function testCreateMergeRequest(): void
    {
        $branches = new DevBranches($this->_client);
        $oldBranches = $branches->listBranches();
        $this->assertCount(1, $oldBranches);

        $newBranch = $branches->createBranch('aaaa');

        $this->_client->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $oldBranches[0]['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);
    }

    public function testCreateMergeRequestFromInvalidBranches(): void
    {
        $this->expectExceptionMessage('Cannot create merge request. Branch not found.');
        $this->_client->createMergeRequest([
            'branchFromId' => 123,
            'branchIntoId' => 345,
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);
    }
    public function testCreateMergeRequestIntoDevBranch(): void
    {
        $this->expectExceptionMessage('Cannot create merge request. Target branch is not default.');

        $branches = new DevBranches($this->_client);
        $oldBranches = $branches->listBranches();
        $this->assertCount(1, $oldBranches);

        $newBranch = $branches->createBranch('aaaa');

        $this->_client->createMergeRequest([
            'branchFromId' => $oldBranches[0]['id'],
            'branchIntoId' => $newBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);
    }
}
