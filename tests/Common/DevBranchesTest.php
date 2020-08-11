<?php

namespace Keboola\Test\Common;

use Exception;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;

class DevBranchesTest extends StorageApiTestCase
{
    public function testCreateBranch()
    {
        $branches = new DevBranches($this->_client);

        $branchName = __CLASS__ . time();
        $branch = $branches->createBranch($branchName);
        $token = $this->_client->verifyToken();
        $adminId = $token['admin']['id'];
        $projectId = $token['owner']['id'];

        $this->assertArrayHasKey('created', $branch);
        unset($branch['created']);
        $this->assertArrayHasKey('id', $branch);
        unset($branch['id']);
        $this->assertSame($branchName, $branch['name']);
        $this->assertArrayHasKey('owner', $branch);
        $this->assertSame(['id', 'name'], array_keys($branch['owner']));
        $this->assertSame($projectId, $branch['owner']['id']);
        $this->assertArrayHasKey('admin', $branch);
        $this->assertSame(['id', 'name', 'email'], array_keys($branch['admin']));
        $this->assertSame($adminId, $branch['admin']['id']);
    }
}
