<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\StorageApiTestCase;
use Throwable;

class BranchFileStorageTest extends StorageApiTestCase
{
    private DevBranches $branches;

    public function setUp(): void
    {
        parent::setUp();
        $developerClient = $this->getDeveloperStorageApiClient();
        $this->branches = new DevBranches($developerClient);
        $this->cleanupTestBranches($developerClient);
    }

    public function testDeleteBranchDeleteFiles(): void
    {
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject());
        $branchClient = $this->getDeveloperStorageApiClient()->getBranchAwareClient($newBranch['id']);
        $filePath = __DIR__ . '/../../_data/files.upload.txt';

        $fileId = $branchClient->uploadFile($filePath, (new FileUploadOptions())->setNotify(false)->setFederationToken(true)->setIsPublic(false));
        $file = $branchClient->getFile($fileId, (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));
        $this->assertEquals(file_get_contents($filePath), file_get_contents($file['url']));

        $s3Client = new \Aws\S3\S3Client([
            'version' => 'latest',
            'region' => $file['region'],
            'credentials' => [
                'key' => $file['credentials']['AccessKeyId'],
                'secret' => $file['credentials']['SecretAccessKey'],
                'token' => $file['credentials']['SessionToken'],
            ],
        ]);

        $object = $s3Client->getObject([
            'Bucket' => $file['s3Path']['bucket'],
            'Key' => $file['s3Path']['key'],
        ]);
        $this->assertEquals(file_get_contents($filePath), $object['Body']);

        $this->branches->deleteBranch($newBranch['id']);

        try {
            $s3Client->getObject([
                'Bucket' => $file['s3Path']['bucket'],
                'Key' => $file['s3Path']['key'],
            ]);
            $this->fail('File should not exist');
        } catch (\Aws\S3\Exception\S3Exception $e) {
            $this->assertEquals(404, $e->getStatusCode());
        }
    }
}
