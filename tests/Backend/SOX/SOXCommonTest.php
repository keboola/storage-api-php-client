<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class SOXCommonTest extends StorageApiTestCase
{
    public function testCantCreateBucketInDefaultBranch(): void
    {
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
}
