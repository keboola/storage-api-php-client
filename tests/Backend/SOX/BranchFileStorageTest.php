<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Aws\S3\Exception\S3Exception;
use Aws\S3\S3Client;
use Generator;
use Google\Cloud\Core\Exception\NotFoundException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Downloader\DownloaderFactory;
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

        // Temporary folder to save downloaded files
        $workingDir = sys_get_temp_dir();
        $tmpFilePath = $workingDir . '/' . uniqid('testDeleteBranchDeleteFiles-', true);

        $downloader = DownloaderFactory::createDownloaderForFileResponse($file);
        $downloader->downloadFileFromFileResponse($file, $tmpFilePath);

        $this->assertEquals(file_get_contents($filePath), file_get_contents($tmpFilePath));

        $this->branches->deleteBranch($newBranch['id']);

        try {
            $downloader->downloadFileFromFileResponse($file, $tmpFilePath);
            $this->fail('File should not exist');
        } catch (S3Exception $e) {
            $this->assertEquals(404, $e->getStatusCode());
        } catch (NotFoundException $e) {
            $this->assertEquals(404, $e->getCode());
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

        if ($this->getDefaultBackend($this->_client) === self::BACKEND_BIGQUERY) {
            $refreshResponse1 = $client1->refreshFileCredentials($file1Id);
            $this->assertNotEmpty($refreshResponse1);

            try {
                $client2->refreshFileCredentials($file1Id);
                $this->fail('File should not exist');
            } catch (ClientException $e) {
                $this->assertEquals(404, $e->getCode());
                $this->assertSame('File not found.', $e->getMessage());
            }
        }

        try {
            $client2->getFile($file1Id, new GetFileOptions());
            $this->fail('File should not exist');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
        }

        $file2Id = $client2->uploadFile($filePath, new FileUploadOptions());
        $this->assertNotEmpty($client2->getFile($file2Id, new GetFileOptions()));

        if ($this->getDefaultBackend($this->_client) === self::BACKEND_BIGQUERY) {
            $refreshResponse2 = $client2->refreshFileCredentials($file2Id);
            $this->assertNotEmpty($refreshResponse2);

            try {
                $client1->refreshFileCredentials($file2Id);
                $this->fail('File should not exist');
            } catch (ClientException $e) {
                $this->assertEquals(404, $e->getCode());
                $this->assertSame('File not found.', $e->getMessage());
            }
        }

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

        if ($this->getDefaultBackend($this->_client) === self::BACKEND_BIGQUERY) {
            try {
                $client1->refreshFileCredentials($file1Id);
                $this->fail('File should not exist');
            } catch (ClientException $e) {
                $this->assertEquals(404, $e->getCode());
                $this->assertSame('File not found.', $e->getMessage());
            }

            try {
                $client2->refreshFileCredentials($file2Id);
                $this->fail('File should not exist');
            } catch (ClientException $e) {
                $this->assertEquals(404, $e->getCode());
                $this->assertSame('File not found.', $e->getMessage());
            }
        }
    }
}
