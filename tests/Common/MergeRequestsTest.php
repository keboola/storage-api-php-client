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

        $newBranch = $branches->createBranch('aaaa');
        $branchesList = $branches->listBranches();
        $this->assertCount(2, $branchesList);

        $this->_client->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $branchesList[0]['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);
    }
}
