<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Aws\S3\Exception\S3Exception;
use Aws\S3\S3Client;
use Generator;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
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
        $file = $branchClient->getFile($fileId, (new GetFileOptions())->setFederationToken(true));
        $this->assertEquals(file_get_contents($filePath), file_get_contents($file['url']));

        $s3Client = new S3Client([
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
        } catch (S3Exception $e) {
            $this->assertEquals(404, $e->getStatusCode());
        }
    }

    /**
     * @dataProvider crossClientProvider
     * @param array<string, Client> $client1 - alternately client which is from default branch and dev branch
     * @param array<string, Client> $client2 - alternately client which is from default branch and dev branch
     */
    public function testCrossCrudFile(array $client1, array $client2): void
    {
        $description = $this->generateDescriptionForTestObject();
        $branch = $this->branches->createBranch($description);
        [$client1, $client2] = $this->resolveBranchClients($client1, $branch['id'], $client2);

        $filePath = __DIR__ . '/../../_data/files.upload.txt';
        $file1Id = $client1->uploadFile($filePath, new FileUploadOptions());
        $this->assertNotEmpty($client1->getFile($file1Id, new GetFileOptions()));

        try {
            $client2->getFile($file1Id, new GetFileOptions());
            $this->fail('File should not exist');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
        }

        $file2Id = $client2->uploadFile($filePath, new FileUploadOptions());
        $this->assertNotEmpty($client2->getFile($file2Id, new GetFileOptions()));

        $client1->deleteFile($file1Id);
        try {
            $client1->getFile($file1Id, new GetFileOptions());
            $this->fail('File should not exist');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
        }

        $this->assertNotEmpty($client2->getFile($file2Id, new GetFileOptions()));

        $client2->deleteFile($file2Id);
        try {
            $client2->getFile($file2Id, new GetFileOptions());
            $this->fail('File should not exist');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
        }
    }
}
