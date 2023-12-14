<?php

namespace Keboola\Test\File;

use Exception;
use Generator;
use GuzzleHttp\Client;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\ClientProvider\TestSetupHelper;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Symfony\Component\Filesystem\Filesystem;

class CommonFileTest extends StorageApiTestCase
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
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testFileList(): void
    {
        $options = new FileUploadOptions();
        $fileId = $this->createAndWaitForFile(__DIR__ . '/../_data/files.upload.txt', $options, $this->_testClient);
        $files = $this->_testClient->listFiles(new ListFilesOptions());
        $this->assertNotEmpty($files);

        $uploadedFile = reset($files);
        $this->assertEquals($fileId, $uploadedFile['id']);
        $this->assertArrayHasKey('region', $uploadedFile);
        $this->assertArrayNotHasKey('credentials', $uploadedFile);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testGetFileWithoutCredentials(): void
    {
        $fileId = $this->createAndWaitForFile(__DIR__ . '/../_data/files.upload.txt', (new FileUploadOptions()), $this->_testClient);
        $file = $this->_testClient->getFile($fileId, (new GetFileOptions())->setFederationToken(false));
        $this->assertArrayNotHasKey('credentials', $file);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testFilesListFilterByTags(): void
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';

        $this->createAndWaitForFile($filePath, new FileUploadOptions(), $this->_testClient);
        $tag = uniqid('tag-test');
        $fileId = $this->createAndWaitForFile($filePath, (new FileUploadOptions())->setTags([$tag]), $this->_testClient);

        $files = $this->_testClient->listFiles((new ListFilesOptions())->setTags([$tag]));

        $this->assertCount(1, $files);
        $file = reset($files);
        $this->assertEquals($fileId, $file['id']);

        $tag2 = uniqid('tag-test-2');
        $fileId2 = $this->createAndWaitForFile($filePath, (new FileUploadOptions())->setTags([$tag, $tag2]), $this->_testClient);

        $files = $this->_testClient->listFiles((new ListFilesOptions())->setTags([$tag, $tag2]));
        $this->assertCount(2, $files, 'files with one or more matching tags are returned');
        $file2 = array_shift($files);
        $file = array_shift($files);
        $this->assertEquals($fileId2, $file2['id']);
        $this->assertEquals($fileId, $file['id']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testFilesListFilterByInvalidValues(): void
    {
        try {
            $this->_testClient->apiGet('files?' . http_build_query([
                    'tags' => 'tag',
                ]));
            $this->fail('Validation error should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.validation', $e->getStringCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testSetTagsFromArrayWithGaps(): void
    {
        $file = $this->_testClient->prepareFileUpload((new FileUploadOptions())
            ->setFileName('test.json')
            ->setFederationToken(true)
            ->setTags([
                0 => 'neco',
                12 => 'another',
            ]));
        $this->assertEquals(['neco', 'another'], $file['tags']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testFileListSearch(): void
    {

        $fileId = $this->_testClient->uploadFile(__DIR__ . '/../_data/users.csv', new FileUploadOptions());
        $this->createAndWaitForFile(__DIR__ . '/../_data/files.upload.txt', new FileUploadOptions(), $this->_testClient);

        $files = $this->_testClient->listFiles((new ListFilesOptions())->setQuery('users')->setLimit(1));

        $this->assertCount(1, $files);
        $file = reset($files);
        $this->assertEquals($fileId, $file['id']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testSyntaxErrorInQueryShouldReturnUserError(): void
    {
        try {
            $this->_testClient->listFiles((new ListFilesOptions())->setQuery('tags[]:sd'));
            $this->fail('exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('query.syntax', $e->getStringCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testFileListFilterBySinceIdMaxId(): void
    {
        $lastFileId = $this->createAndWaitForFile(__DIR__ . '/../_data/files.upload.txt', new FileUploadOptions(), $this->_testClient);

        $firstFileId = $this->createAndWaitForFile(__DIR__ . '/../_data/users.csv', new FileUploadOptions(), $this->_testClient);
        $secondFileId = $this->createAndWaitForFile(__DIR__ . '/../_data/users.csv', new FileUploadOptions(), $this->_testClient);

        $files = $this->_testClient->listFiles((new ListFilesOptions())->setSinceId($lastFileId));
        $this->assertCount(2, $files);

        $this->assertEquals($firstFileId, $files[1]['id']);
        $this->assertEquals($secondFileId, $files[0]['id']);

        $files = $this->_testClient->listFiles((new ListFilesOptions())->setMaxId($secondFileId)->setLimit(1));
        $this->assertCount(1, $files);
        $this->assertEquals($firstFileId, $files[0]['id']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testFileListFilterByRunId(): void
    {
        $options = new FileUploadOptions();
        $options->setFileName('upload.txt')
            ->setFederationToken(true);

        $runId = $this->_client->generateRunId();
        $this->_testClient->setRunId($runId);

        $fileId = $this->createAndWaitForFile(__DIR__ . '/../_data/users.csv', $options, $this->_testClient);
        $file = $this->_testClient->getFile($fileId);
        $this->assertEquals($runId, $file['runId']);

        $files = $this->_testClient->listFiles((new ListFilesOptions())->setRunId($runId));

        $this->assertCount(1, $files);
        $this->assertEquals($file['id'], $files[0]['id']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testEmptyFileUpload(): void
    {
        $options = new FileUploadOptions();
        $filePath = __DIR__ . '/../_data/empty.csv';
        $fileId = $this->_testClient->uploadFile($filePath, $options);
        $file = $this->_testClient->getFile($fileId);

        $this->assertEquals($options->getIsPublic(), $file['isPublic']);
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
        $this->assertEquals($file['isEncrypted'], $options->getIsEncrypted());

        if ($options->getIsPermanent()) {
            $this->assertNull($file['maxAgeDays']);
        } else {
            $this->assertIsInt($file['maxAgeDays']);
            $this->assertEquals(StorageApiTestCase::FILE_LONG_TERM_EXPIRATION_IN_DAYS, $file['maxAgeDays']);
        }

        // check attachment, download
        $client = new Client();
        $response = $client->get($file['url']);
        $this->assertStringStartsWith('attachment', (string) $response->getHeader('Content-Disposition')[0]);
    }

    /**
     * with compress = true
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testFileUploadCompress(): void
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $fileId = $this->_testClient->uploadFile($filePath, (new FileUploadOptions())->setCompress(true));
        $file = $this->_testClient->getFile($fileId);

        $this->assertEquals(basename($filePath) . '.gz', $file['name']);

        $gzFile = gzopen($file['url'], 'r');
        if ($gzFile === false) {
            throw new Exception(sprintf('Cannot open file "%s"', $file['url']));
        }
        $this->assertEquals(file_get_contents($filePath), gzread($gzFile, 524288));
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testFileUploadLargeFile(): void
    {
        $filePath = __DIR__ . '/../_tmp/files.upload.large.csv';
        $fileHandle = fopen($filePath, 'w+');
        if ($fileHandle === false) {
            throw new Exception(sprintf('Cannot open file "%s"', $filePath));
        }
        for ($i = 0; $i < 5000000; $i++) {
            fputs($fileHandle, '0123456789');
        }
        fclose($fileHandle);
        $fileId = $this->_testClient->uploadFile($filePath, new FileUploadOptions());
        $file = $this->_testClient->getFile($fileId);

        $this->assertEquals(basename($filePath), $file['name']);
        $this->assertEquals(hash_file('md5', $filePath), hash_file('md5', $file['url']));
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testFileDelete(): void
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $options = new FileUploadOptions();

        $fileId = $this->_testClient->uploadFile($filePath, $options);
        $file = $this->_testClient->getFile($fileId);

        $this->_testClient->deleteFile($fileId);

        try {
            $this->_testClient->getFile($fileId);
            $this->fail('File should not exists');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.files.notFound', $e->getStringCode());
        }
        $this->expectExceptionCode(404);
        $this->expectException(\GuzzleHttp\Exception\ClientException::class);
        (new Client())->get($file['url']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testNotExistingFileUpload(): void
    {
        try {
            $this->_testClient->uploadFile('invalid.csv', new FileUploadOptions());
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('fileNotReadable', $e->getStringCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testsDuplicateTagsShouldBeDeduped(): void
    {
        $uploadOptions = new FileUploadOptions();
        $uploadOptions
            ->setFileName('test.txt')
            ->setFederationToken(true)
            ->setTags(['first', 'first', 'second']);
        $file = $this->_testClient->prepareFileUpload($uploadOptions);
        $file = $this->_testClient->getFile($file['id']);
        $this->assertEquals(['first', 'second'], $file['tags']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testDownloadFile(): void
    {
        $uploadOptions = (new FileUploadOptions())
            ->setFileName('testing_file_name');
        $sourceFilePath = __DIR__ . '/../_data/files.upload.txt';
        $fileId = $this->_testClient->uploadFile($sourceFilePath, $uploadOptions);
        $tmpDestination = __DIR__ . '/../_tmp/testing_file_name';
        if (file_exists($tmpDestination)) {
            $fs = new Filesystem();
            $fs->remove($tmpDestination);
        }

        $this->_testClient->downloadFile($fileId, $tmpDestination);

        $this->assertSame(
            file_get_contents($sourceFilePath),
            file_get_contents($tmpDestination),
        );
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testUploadAndDownloadSlicedFile(): void
    {
        $uploadOptions = (new FileUploadOptions())
            ->setFileName('sliced_testing_file_name')
            ->setIsSliced(true)
        ;
        $slices = [
            __DIR__ . '/../_data/sliced/neco_0000_part_00',
            __DIR__ . '/../_data/sliced/neco_0001_part_00',
            __DIR__ . '/../_data/sliced/neco_0002_part_00',
        ];
        $fileId = $this->_testClient->uploadSlicedFile($slices, $uploadOptions);
        $tmpDestinationFolder = __DIR__ . '/../_tmp/slicedUpload/';
        $fs = new Filesystem();
        if (file_exists($tmpDestinationFolder)) {
            $fs->remove($tmpDestinationFolder);
        }
        $fs->mkdir($tmpDestinationFolder);

        $donwloadFiles = $this->_testClient->downloadSlicedFile($fileId, $tmpDestinationFolder);
        $this->assertFileEquals($slices[0], $donwloadFiles[0]);
        $this->assertFileEquals($slices[1], $donwloadFiles[1]);
        $this->assertFileEquals($slices[2], $donwloadFiles[2]);

        $this->_testClient->deleteFile($fileId);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testTagging(): void
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $initialTags = ['gooddata', 'image'];
        $fileId = $this->_testClient->uploadFile($filePath, (new FileUploadOptions())->setFederationToken(true)->setTags($initialTags));

        $file = $this->_testClient->getFile($fileId);
        $this->assertEquals($initialTags, $file['tags']);

        $this->_testClient->deleteFileTag($fileId, 'gooddata');

        $file = $this->_testClient->getFile($fileId);
        $this->assertEquals(['image'], $file['tags']);

        $this->_testClient->addFileTag($fileId, 'new');
        $file = $this->_testClient->getFile($fileId);
        $this->assertEquals(['image', 'new'], $file['tags']);

        $this->_testClient->addFileTag($fileId, 'new');
        $file = $this->_testClient->getFile($fileId);
        $this->assertEquals(['image', 'new'], $file['tags'], 'duplicate tag add is ignored');
    }

    /** @dataProvider invalidIdDataProvider */
    public function testInvalidFileId(string $devBranchType, string $userRole, $fileId): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('File id cannot be empty');
        $this->_testClient->getFile($fileId);
    }

    public function invalidIdDataProvider(): Generator
    {
        $invalidData = [
            'null' => [null],
            'empty string' => [''],
        ];
        $clientProvider = $this->provideComponentsClientTypeBasedOnSuite();

        return $this->combineProviders($invalidData, $clientProvider);
    }

    /**
     * @dataProvider downloadFileNotFoundErrorHandlingProvider
     */
    public function testDownloadFileNotFoundErrorHandling(string $devBranchType, string $userRole, bool $isSliced): void
    {
        $uploadOptions = (new FileUploadOptions())
            ->setFileName('testing_file_name')
            ->setIsSliced($isSliced)
            ->setFederationToken(true)
        ;

        $file = $this->_testClient->prepareFileUpload($uploadOptions);
        $tmpDestination = __DIR__ . '/../_tmp/testing_file_name';

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(sprintf(
            'Cannot download file "testing_file_name" (ID %s) from Storage, '
            . 'please verify the contents of the file and that the file has not expired.',
            $file['id'],
        ));

        if (!$isSliced) {
            $this->_testClient->downloadFile($file['id'], $tmpDestination);
        } else {
            $this->_testClient->downloadSlicedFile($file['id'], $tmpDestination);
        }
    }

    public function downloadFileNotFoundErrorHandlingProvider(): Generator
    {
        $slicingProvider = [
            'non-sliced file' => [false],
            'sliced file' => [true],
        ];

        $clientProvider = $this->provideComponentsClientTypeBasedOnSuite();

        return $this->combineProviders($slicingProvider, $clientProvider);
    }

    public function provideComponentsClientTypeBasedOnSuite(): array
    {
        return (new TestSetupHelper())->provideComponentsClientTypeBasedOnSuite($this);
    }
}
