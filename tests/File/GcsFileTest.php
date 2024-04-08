<?php

namespace Keboola\Test\File;

use DateTime;
use DateTimeZone;
use GuzzleHttp\Client;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\GCSUploader;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\RefreshFileCredentialsWrapper;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\ClientProvider\TestSetupHelper;
use Keboola\Test\StorageApiTestCase;

class GcsFileTest extends StorageApiTestCase
{

    /** @var BranchAwareClient|StorageApiClient */
    private $_testClient;

    private ClientProvider $clientProvider;

    public function setUp(): void
    {
        parent::setUp();

        $this->clientProvider = new ClientProvider($this);
        [$devBranchType, $userRole] = $this->getProvidedData();
        [$this->_client, $this->_testClient] = (new TestSetupHelper())->setUpForProtectedDevBranch(
            $this->clientProvider,
            $devBranchType,
            $userRole,
        );

        if ($devBranchType === ClientProvider::DEV_BRANCH) {
            // buckets must be created in branch that the tests run in
            $this->initEmptyTestBucketsForParallelTests([self::STAGE_OUT, self::STAGE_IN], $this->_testClient);
        } elseif ($devBranchType === ClientProvider::DEFAULT_BRANCH) {
            $this->initEmptyTestBucketsForParallelTests();
        } else {
            throw new \Exception(sprintf('Unknown devBranchType "%s"', $devBranchType));
        }
        $token = $this->_testClient->verifyToken();
        $this->assertSame(
            'gcp',
            $token['owner']['fileStorageProvider'],
            'Project must have GCS file storage',
        );
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testUploadFileWithRefreshedCredentials(): void
    {
        $prepareFile = $this->_testClient->prepareFileUpload((new FileUploadOptions())
            ->setFileName('languages.csv')
            ->setFederationToken(true));

        $this->assertArrayHasKey('id', $prepareFile);

        $refreshedCredentials = $this->_testClient->refreshFileCredentials($prepareFile['id']);
        $uploadParams = $refreshedCredentials['gcsUploadParams'];
        $client = $this->getGcsClientClient($uploadParams);
        $bucket = $client->bucket($uploadParams['bucket']);
        $file = fopen(__DIR__ . '/../_data/languages.csv', 'r');
        if (!$file) {
            throw new ClientException("Cannot open file {$file}");
        }

        $bucket->upload(
            $file,
            [
                'name' => $uploadParams['key'],
            ],
        );

        $tableId = $this->_testClient->createTableAsyncDirect(
            $this->getTestBucketId(),
            [
                'name' => 'languages',
                'dataFileId' => $prepareFile['id'],
            ],
        );

        $table = $this->_testClient->getTable($tableId);
        $this->assertEquals(5, $table['rowsCount']);
    }

    /**
     * @dataProvider uploadData
     */
    public function testFileUpload(string $devBranchType, string $userRole, string $filePath, FileUploadOptions $options): void
    {
        $fileId = $this->_testClient->uploadFile($filePath, $options);
        $file = $this->_testClient->getFile($fileId, (new GetFileOptions())->setFederationToken(true));

        //gcs storage is always encrypted and private. Request params 'isEncrypted' and 'isPublic' is ignored
        $this->assertFalse($file['isPublic']);
        $this->assertTrue($file['isEncrypted']);

        $this->assertEquals(basename($filePath), $file['name']);
        $this->assertEquals(filesize($filePath), $file['sizeBytes']);
        $this->assertArrayHasKey('gcsCredentials', $file);
        $this->assertArrayHasKey('gcsPath', $file);
        sleep(1); // tests sometimes return 403, but file can be downloaded just a few seconds later
        $this->assertEquals(file_get_contents($filePath), file_get_contents($file['url']));

        $tags = $options->getTags();
        sort($tags);
        $fileTags = $file['tags'];
        sort($fileTags);
        $this->assertEquals($tags, $fileTags);

        $info = $this->_testClient->verifyToken();
        $this->assertEquals($file['creatorToken']['id'], (int) $info['id']);
        $this->assertEquals($file['creatorToken']['description'], $info['description']);

        if ($options->getIsPermanent()) {
            $this->assertNull($file['maxAgeDays']);
        } else {
            $this->assertIsInt($file['maxAgeDays']);
            $this->assertEquals(StorageApiTestCase::FILE_LONG_TERM_EXPIRATION_IN_DAYS, $file['maxAgeDays']);
        }

        // check attachment, download
        $client = new Client();
        $client->get($file['url']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testReUpload(): void
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $options = new FileUploadOptions();
        $options
            ->setFileName('upload.txt')
            ->setFederationToken(true)
            ->setIsEncrypted(false);

        $prepareResult = $this->_testClient->prepareFileUpload($options);
        $refreshCallableWrapper = new RefreshFileCredentialsWrapper($this->_testClient, $prepareResult['id']);
        $gcsUploader = new GCSUploader([
            'credentials' => [
                'access_token' => $prepareResult['gcsUploadParams']['access_token'],
                'expires_in' => $prepareResult['gcsUploadParams']['expires_in'],
                'token_type' => $prepareResult['gcsUploadParams']['token_type'],
            ],
            'projectId' => $prepareResult['gcsUploadParams']['projectId'],
        ], $refreshCallableWrapper);

        $gcsUploader->uploadFile(
            $prepareResult['gcsUploadParams']['bucket'],
            $prepareResult['gcsUploadParams']['key'],
            $filePath,
            false,
        );

        // re-upload should work
        $gcsUploader->uploadFile(
            $prepareResult['gcsUploadParams']['bucket'],
            $prepareResult['gcsUploadParams']['key'],
            $filePath,
            false,
        );
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testDeleteNonUploadedSlicedFile(): void
    {
        $options = new FileUploadOptions();
        $options
            ->setFileName('entries_')
            ->setFederationToken(true)
            ->setIsSliced(true)
            ->setIsEncrypted(true);

        $result = $this->_client->prepareFileUpload($options);

        $fileId = $result['id'];
        $file = $this->_client->getFile($fileId, (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));
        $this->assertNotNull($file);
        $this->_client->deleteFile($fileId);

        $this->expectException(\Keboola\StorageApi\ClientException::class);
        $this->expectExceptionMessage('File not found');
        $this->_client->getFile($fileId, (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));
    }

    public function uploadData(): \Generator
    {
        $path = __DIR__ . '/../_data/files.upload.txt';
        $largeFilePath = sys_get_temp_dir() . '/large_abs_upload.txt';
        $this->generateFile($largeFilePath, 16);

        $uploadData = [
            [
                $path,
                (new FileUploadOptions())->setIsPublic(true),
            ],
            [
                $path,
                (new FileUploadOptions())
                    ->setIsPublic(true),
            ],
            [
                $path,
                (new FileUploadOptions())
                    ->setIsEncrypted(false),
            ],
            [
                $path,
                (new FileUploadOptions())
                    ->setIsEncrypted(true),
            ],
            [
                $path,
                (new FileUploadOptions())
                    ->setNotify(false)
                    ->setCompress(false)
                    ->setIsPublic(false),
            ],
            [
                $path,
                (new FileUploadOptions())
                    ->setIsPublic(true)
                    ->setIsPermanent(true)
                    ->setTags(['sapi-import', 'martin']),
            ],
            'large file' => [
                $largeFilePath,
                (new FileUploadOptions())
                    ->setIsPublic(true)
                    ->setIsPermanent(true)
                    ->setTags(['sapi-import', 'martin']),
            ],
        ];

        $clientProvider = $this->provideComponentsClientTypeBasedOnSuite();

        return $this->combineProviders($uploadData, $clientProvider);
    }

    public function provideComponentsClientTypeBasedOnSuite(): array
    {
        return (new TestSetupHelper())->provideComponentsClientTypeBasedOnSuite($this);
    }

    private function generateFile(string $filepath, int $fileSizeMegabytes): void
    {
        if (file_exists($filepath)) {
            unlink($filepath);
        }
        $fp = fopen($filepath, 'a+');
        if ($fp === false) {
            throw new Exception(sprintf('Cannot open file "%s"', $filepath));
        }
        $i = 0;
        while ($i++ < $fileSizeMegabytes) {
            fwrite($fp, str_repeat('X', 1024 * 1024));
        }
        fclose($fp);
    }
}
