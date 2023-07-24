<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;

class BranchStorageTest extends StorageApiTestCase
{
    private DevBranches $branches;

    public const SUPPORTED_BACKENDS = [
        self::BACKEND_SNOWFLAKE,
    ];

    public function setUp(): void
    {
        parent::setUp();
        $this->branches = new DevBranches($this->_client);
        foreach ($this->getBranchesForCurrentTestCase($this->branches) as $branch) {
            $this->branches->deleteBranch($branch['id']);
        }
    }

    public function testCanCreateBucket(): void
    {
        $description = $this->generateDescriptionForTestObject();
        $bucketId = self::STAGE_IN . 'c-' . $this->getTestBucketName($description);
        try {
            $this->_client->getBucket($bucketId);
            $this->_client->dropBucket($bucketId, ['force' => true]);
        } catch (ClientException $e) {
            // ignore if bucket not exists
        }
        $branch = $this->branches->createBranch($description);

        $backend = $this->_client->verifyToken()['owner']['defaultBackend'];
        if (!in_array(
            $backend,
            self::SUPPORTED_BACKENDS,
            true
        )) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage(sprintf(
                'Backend "%s" is not supported for development branch. Supported backends: "snowflake".',
                $backend
            ));
        } else {
            $this->expectNotToPerformAssertions();
        }
        $this->_client->getBranchAwareClient($branch['id'])
            ->createBucket(
                $this->getTestBucketName($description),
                self::STAGE_IN,
                $description
            );
    }
}
