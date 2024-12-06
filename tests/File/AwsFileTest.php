<?php

namespace Keboola\Test\File;

use Aws\S3\S3Client;
use GuzzleHttp\Client;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\S3Uploader;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\ClientProvider\TestSetupHelper;
use Keboola\Test\StorageApiTestCase;

class AwsFileTest extends StorageApiTestCase
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
            'aws',
            $token['owner']['fileStorageProvider'],
            'Project must have S3 file storage',
        );
    }

    /**
     * @dataProvider uploadData
     */
    public function testFileUpload(string $devBranchType, string $userRole, $filePath, FileUploadOptions $options): void
    {
        $fileId = $this->_testClient->uploadFile($filePath, $options);
        $file = $this->_testClient->getFile($fileId);

        $this->assertEquals($options->getIsPublic(), $file['isPublic']);
        $this->assertEquals($options->getIsEncrypted(), $file['isEncrypted']);

        $this->assertEquals(basename($filePath), $file['name']);
        $this->assertEquals(filesize($filePath), $file['sizeBytes']);
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
        $response = $client->get($file['url']);
        $this->assertStringStartsWith('attachment;', (string) $response->getHeader('Content-Disposition')[0]);
    }

    public function uploadData()
    {
        $path = __DIR__ . '/../_data/files.upload.txt';
        $uploadData = [
            'isPublic: false' => [
                $path,
                (new FileUploadOptions())
                    ->setIsPublic(false),
            ],
            'isEncrypted: false' => [
                $path,
                (new FileUploadOptions())
                    ->setIsEncrypted(false),
            ],
            'isEncrypted: true' => [
                $path,
                (new FileUploadOptions())
                    ->setIsEncrypted(true),
            ],
            'notify: false, compress: false, isPublic:false' => [
                $path,
                (new FileUploadOptions())
                    ->setNotify(false)
                    ->setCompress(false)
                    ->setIsPublic(false),
            ],
            'isPublic: false, isPermanent: true, tags: \'sapi-import\', \'martin\'' => [
                $path,
                (new FileUploadOptions())
                    ->setIsPublic(false)
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

    /**
     * @dataProvider encryptedData
     * @param $encrypted
     */
    public function testFileUploadUsingFederationToken(string $devBranchType, string $userRole, $encrypted): void
    {
        $pathToFile = __DIR__ . '/../_data/files.upload.txt';
        $options = new FileUploadOptions();
        $options
            ->setFileName('upload.txt')
            ->setFederationToken(true)
            ->setIsEncrypted($encrypted);

        $result = $this->_testClient->prepareFileUpload($options);

        $uploadParams = $result['uploadParams'];
        $this->assertArrayHasKey('credentials', $uploadParams);

        $fileId = $this->_testClient->uploadFile($pathToFile, $options);

        $file = $this->_testClient->getFile($fileId);

        $this->assertEquals(file_get_contents($pathToFile), file_get_contents($file['url']));

        //all files on Azure are encrypted
        $this->assertEquals($result['isEncrypted'], $options->getIsEncrypted());
        $s3Client = new \Aws\S3\S3Client([
            'version' => 'latest',
            'region' => $result['region'],
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ],
        ]);
        try {
            $s3Client->putObject([                //all files on Azure are encrypted
                'Bucket' => $uploadParams['bucket'],
                'Key' => $uploadParams['key'] . '_part0001',
                'Body' => fopen($pathToFile, 'r+'),
            ]);
            $this->fail('Access denied exception should be thrown');
        } catch (\Aws\S3\Exception\S3Exception $e) {
            $this->assertEquals(403, $e->getStatusCode());
        }
    }

    public function encryptedData()
    {
        $encryptedData = [
            'encrypted: false' => [false],
            'encrypted: true' => [true],
        ];

        $clientProvider = $this->provideComponentsClientTypeBasedOnSuite();

        return $this->combineProviders($encryptedData, $clientProvider);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRequireEncryptionForSliced(): void
    {
        // sliced file
        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('entries_')
            ->setIsEncrypted(true)
            ->setIsSliced(true);
        $slicedFile = $this->_testClient->prepareFileUpload($uploadOptions);

        $uploadParams = $slicedFile['uploadParams'];

        $s3Client = new \Aws\S3\S3Client([
            'version' => 'latest',
            'region' => $slicedFile['region'],
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ],
        ]);

        try {
            // write without encryption header
            $s3Client->putObject([
                'Bucket' => $uploadParams['bucket'],
                'Key' => $uploadParams['key'] . 'part001.gz',
                'Body' => fopen(__DIR__ . '/../_data/sliced/neco_0000_part_00.gz', 'r+'),
            ])->get('ObjectURL');
            $this->fail('Write should not be allowed');
        } catch (\Aws\S3\Exception\S3Exception $e) {
            $this->assertEquals(403, $e->getStatusCode());
            $this->assertEquals('AccessDenied', $e->getAwsErrorCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testGetFileFederationToken(): void
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $fileId = $this->_testClient->uploadFile($filePath, (new FileUploadOptions())->setNotify(false)->setFederationToken(true)->setIsPublic(false));

        $file = $this->_testClient->getFile($fileId, (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));

        $this->assertArrayHasKey('credentials', $file);
        $this->assertArrayHasKey('s3Path', $file);
        $this->assertArrayHasKey('Expiration', $file['credentials']);

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

        $objects = $s3Client->listObjects([
            'Bucket' => $file['s3Path']['bucket'],
            'Prefix' => $file['s3Path']['key'],
        ]);

        $this->assertCount(1, $objects->get('Contents'), 'Only one file should be returned');

        try {
            $s3Client->listObjects([
                'Bucket' => $file['s3Path']['bucket'],
                'Prefix' => dirname($file['s3Path']['key']),
            ]);
            $this->fail('Access denied exception should be thrown');
        } catch (\Aws\S3\Exception\S3Exception $e) {
            $this->assertEquals(403, $e->getStatusCode());
        }

        try {
            $s3Client->listObjects([
                'Bucket' => $file['s3Path']['bucket'],
                'Prefix' => $file['s3Path']['key'] . 'manifest',
            ]);
            $this->fail('Access denied exception should be thrown');
        } catch (\Aws\S3\Exception\S3Exception $e) {
            $this->assertEquals(403, $e->getStatusCode());
        }
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

        $uploadParams = $prepareResult['uploadParams'];
        $s3options = [
            'version' => '2006-03-01',
            'retries' => 40,
            'region' => $prepareResult['region'],
            'http' => [
                'connect_timeout' => 10,
                'timeout' => 500,
            ],
            'debug' => false,
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ],
        ];

        $s3Client = new S3Client($s3options);
        $s3Uploader = new S3Uploader($s3Client);
        $s3Uploader->uploadFile(
            $uploadParams['bucket'],
            $uploadParams['key'],
            $uploadParams['acl'],
            $filePath,
            $prepareResult['name'],
            null,
        );

        // re-upload should work
        $s3Uploader->uploadFile(
            $uploadParams['bucket'],
            $uploadParams['key'],
            $uploadParams['acl'],
            $filePath,
            $prepareResult['name'],
            null,
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
}
