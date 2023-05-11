<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;

class DevBranchesTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();

        // branches cleanup
        $branches = new DevBranches($this->_client);
        foreach ($branches->listBranches() as $branch) {
            if ($branch['isDefault'] !== true) {
                $branches->deleteBranch($branch['id']);
            }
        }
    }

    public function testCannotDeleteDefaultBranch(): void
    {
        $branches = new DevBranches($this->_client);
        $branchesList = $branches->listBranches();

        $this->assertCount(1, $branchesList);
        $branch = reset($branchesList);

        $this->assertTrue($branch['isDefault']);

        try {
            $branches->deleteBranch($branch['id']);
            $this->fail('Removing default branch should be restricted.');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertSame('Cannot delete Main branch', $e->getMessage());
        }

        $this->assertCount(1, $branches->listBranches());
    }

    public function testAdminRoleBranchesPermissions(): void
    {
        $description = __CLASS__ . '\\' . $this->getName();

        $adminDevBranches = new DevBranches($this->_client);
        $branch = $adminDevBranches->createBranch($description);

        $branches = new DevBranches($this->getClientForToken(STORAGE_API_GUEST_TOKEN));
        $guestBranch = $branches->createBranch($description . '\\GuestRole');

        $this->assertCount(3, $adminDevBranches->listBranches());

        $this->assertSame($branch, $adminDevBranches->getBranch($branch['id']));
        $this->assertSame($guestBranch, $adminDevBranches->getBranch($guestBranch['id']));

        $adminDevBranches->deleteBranch($branch['id']);
        $adminDevBranches->deleteBranch($guestBranch['id']);

        $this->assertCount(1, $adminDevBranches->listBranches());
    }

    public function testBranchCreateAndDelete(): void
    {
        $this->initEvents($this->_client);
        $token = $this->_client->verifyToken();
        $branches = new DevBranches($this->_client);

        $branchName = __CLASS__ . '\\' . $this->getName();
        $branchDescription = __CLASS__ . '\\' . $this->getName() . ' - description';

        // can create branch
        $branch = $branches->createBranch($branchName . '-original', $branchDescription . '-original');
        $this->assertArrayHasKey('id', $branch);
        $this->assertArrayHasKey('name', $branch);
        $this->assertArrayHasKey('description', $branch);
        $this->assertArrayHasKey('created', $branch);
        $this->assertArrayHasKey('isDefault', $branch);
        $this->assertArrayNotHasKey('admin', $branch);
        $this->assertArrayHasKey('creatorToken', $branch);
        $this->assertArrayHasKey('id', $branch['creatorToken']);
        $this->assertEquals($token['id'], $branch['creatorToken']['id']);
        $this->assertArrayHasKey('name', $branch['creatorToken']);
        $this->assertSame($token['description'], $branch['creatorToken']['name']);
        $this->assertSame($branchName . '-original', $branch['name']);
        $this->assertSame($branchDescription . '-original', $branch['description']);
        $this->assertFalse($branch['isDefault']);
        $branchId = $branch['id'];

        // event is created for created branch
        $assertCallback = function ($events) use ($branchName) {
            $this->assertCount(1, $events);
            $this->assertSame($branchName . '-original', $events[0]['objectName']);
            $this->assertSame('devBranch', $events[0]['objectType']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.devBranchCreated')
            ->setObjectId($branchId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // update name and description
        $branch = $branches->updateBranch($branchId, $branchName, $branchDescription);
        $this->assertSame($branchName, $branch['name']);
        $this->assertSame($branchDescription, $branch['description']);

        // event is created for updated branch
        $assertCallback = function ($events) use ($branchName) {
            $this->assertCount(1, $events);
            $this->assertSame($branchName . '-original', $events[0]['objectName']);
            $this->assertSame('devBranch', $events[0]['objectType']);
            $this->assertSame($branchName, $events[0]['params']['devBranchName']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.devBranchUpdated')
            ->setObjectId($branchId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // can get branch detail
        $branchFromDetail = $branches->getBranch($branchId);
        $this->assertArrayHasKey('id', $branchFromDetail);
        $this->assertArrayHasKey('name', $branchFromDetail);
        $this->assertArrayHasKey('created', $branchFromDetail);
        $this->assertArrayHasKey('isDefault', $branch);
        $this->assertArrayNotHasKey('admin', $branchFromDetail);
        $this->assertArrayHasKey('creatorToken', $branchFromDetail);
        $this->assertArrayHasKey('id', $branchFromDetail['creatorToken']);
        $this->assertEquals($token['id'], $branchFromDetail['creatorToken']['id']);
        $this->assertArrayHasKey('name', $branchFromDetail['creatorToken']);
        $this->assertSame($token['description'], $branchFromDetail['creatorToken']['name']);
        $this->assertFalse($branchFromDetail['isDefault']);
        $this->assertSame($branchName, $branchFromDetail['name']);
        $this->assertSame($branchDescription, $branchFromDetail['description']);

        // can list branches and see created branch
        $branchesList = $branches->listBranches();
        $this->assertCount(2, $branchesList);
        $this->assertContains($branchFromDetail, $branchesList);

        $defaultBranchCount = 0;
        foreach ($branchesList as $branch) {
            if ($branch['isDefault'] === true) {
                $defaultBranchCount ++;
            }
        }

        $this->assertSame(1, $defaultBranchCount);

        // cannot create branch with same name
        try {
            $branches->createBranch($branchName);
            $this->fail('Sharing bucket to non organization member should fail.');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertSame(
                sprintf('There already is a branch with name "%s"', $branchName),
                $e->getMessage()
            );
            $this->assertSame('devBranch.duplicateName', $e->getStringCode());
        }

        // can delete branch
        $branches->deleteBranch($branchId);

        // there is event for deleted branch
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.devBranchDeleted')
            ->setObjectId($branchId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // cannot delete nonexistent branch
        try {
            $branches->deleteBranch($branchId);
            $this->fail('Delete non-existing branch should fail.');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame(sprintf('Branch not found'), $e->getMessage());
        }

        // now branch can be created with same name as deleted branch
        $branches->createBranch($branchName);
    }


    public function testGuestRoleBranchesPermissions(): void
    {
        $description = __CLASS__ . '\\' . $this->getName();

        $adminDevBranches = new DevBranches($this->_client);
        $branch = $adminDevBranches->createBranch($description);

        $this->assertCount(2, $adminDevBranches->listBranches());

        $branches = new DevBranches($this->getClientForToken(STORAGE_API_GUEST_TOKEN));
        $branches->createBranch($description . '\\GuestRole');

        $this->assertCount(3, $branches->listBranches());

        $this->assertSame($branch, $branches->getBranch($branch['id']));

        try {
            $branches->deleteBranch($branch['id']);
            $this->fail('Branch delete with guest role token should fail.');
        } catch (ClientException $e) {
            $this->assertAccessForbiddenException($e);
        }

        $this->assertCount(3, $adminDevBranches->listBranches());
    }

    public function testNonAdminTokenRestrictions(): void
    {
        $description = __CLASS__ . '\\' . $this->getName();

        $adminDevBranches = new DevBranches($this->_client);
        $branch = $adminDevBranches->createBranch($description);

        $options = (new TokenCreateOptions())
            ->setDescription($description)
            ->setCanManageBuckets(true)
            ->setExpiresIn(3600)
        ;

        $token = $this->tokens->createToken($options);

        $branches = new DevBranches($this->getClientForToken($token['token']));

        try {
            $branches->createBranch($description);
            $this->fail('Creating a new branch with non-admin token should fail.');
        } catch (ClientException $e) {
            $this->assertAccessForbiddenException($e);
        }

        $this->assertCount(2, $adminDevBranches->listBranches());

        try {
            $branches->getBranch($branch['id']);
            $this->fail('Branch detail with non-admin token should fail.');
        } catch (ClientException $e) {
            $this->assertAccessForbiddenException($e);
        }

        try {
            $branches->listBranches();
            $this->fail('List of branches with non-admin token should fail.');
        } catch (ClientException $e) {
            $this->assertAccessForbiddenException($e);
        }

        try {
            $branches->deleteBranch($branch['id']);
            $this->fail('Branch delete with non-admin token should fail.');
        } catch (ClientException $e) {
            $this->assertAccessForbiddenException($e);
        }

        $this->assertCount(2, $adminDevBranches->listBranches());
    }

    public function testReadOnlyRoleBranchesPermissions(): void
    {
        $description = __CLASS__ . '\\' . $this->getName();

        $adminDevBranches = new DevBranches($this->_client);
        $branch = $adminDevBranches->createBranch($description);

        $this->assertCount(2, $adminDevBranches->listBranches());

        $branches = new DevBranches($this->getClientForToken(STORAGE_API_READ_ONLY_TOKEN));

        try {
            $branches->createBranch($description);
            $this->fail('Creating a new branch with readOnly role token should fail.');
        } catch (ClientException $e) {
            $this->assertAccessForbiddenException($e);
        }

        $this->assertCount(2, $branches->listBranches());

        $this->assertSame($branch, $branches->getBranch($branch['id']));

        try {
            $branches->deleteBranch($branch['id']);
            $this->fail('Branch delete with readOnly role token should fail.');
        } catch (ClientException $e) {
            $this->assertAccessForbiddenException($e);
        }

        $this->assertCount(2, $adminDevBranches->listBranches());
    }

    private function assertAccessForbiddenException(ClientException $exception): void
    {
        $this->assertSame(403, $exception->getCode());
        $this->assertSame('You don\'t have access to the resource.', $exception->getMessage());
    }

    public function testCanUpdateMainBranchDescription(): void
    {
        $branches = new DevBranches($this->_client);
        $branchesList = $branches->listBranches();
        $this->assertCount(1, $branchesList);
        $branch = reset($branchesList);

        $this->assertTrue($branch['isDefault']);
        $updatedBranch = $branches->updateBranch($branch['id'], '', 'Updated description');
        $this->assertSame('Main', $updatedBranch['name']);
        $this->assertSame('Updated description', $updatedBranch['description']);
    }

    public function testCannotUpdateMainBranchName(): void
    {
        $branches = new DevBranches($this->_client);
        $branchesList = $branches->listBranches();
        $this->assertCount(1, $branchesList);
        $branch = reset($branchesList);

        $this->assertTrue($branch['isDefault']);
        try {
            $branches->updateBranch($branch['id'], 'NewBranchName');
            $this->fail('Update default branch name should not be possible.');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertSame('You can not update default branch name.', $e->getMessage());
        }
    }
}
