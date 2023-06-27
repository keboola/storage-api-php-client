<?php

namespace Keboola\Test\File;

use GuzzleHttp\Client;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\ClientProvider\TestSetupHelper;
use Keboola\Test\StorageApiTestCase;
use Symfony\Component\Filesystem\Filesystem;

class FileCrudTest extends StorageApiTestCase
{
    /** @var BranchAwareClient|Client */
    private $testClient;

    public function setUp(): void
    {
        parent::setUp();

        $this->clientProvider = new ClientProvider($this);
        [$devBranchType, $userRole] = $this->getProvidedData();
        [$this->_client, $this->testClient] = (new TestSetupHelper())->setUpForProtectedDevBranch(
            $this->clientProvider,
            $devBranchType,
            $userRole
        );
    }

    public function provideClientTypeBasedOnSuite()
    {
        $this->clientProvider = new ClientProvider($this);
        $this->_client = $this->getDefaultClient();
        $defaultAndBranchProvider = [
            'defaultBranch + production-mananger' => [
                ClientProvider::DEFAULT_BRANCH,
                'production-manager',
            ],
            'devBranch + developer' => [
                ClientProvider::DEV_BRANCH,
                'developer',
            ],
        ];
        $onlyDefaultProvider = [
            'defaultBranch + admin' => [
                ClientProvider::DEFAULT_BRANCH,
                'admin',
            ],
        ];

        if (SUITE_NAME === 'paratest-sox-snowflake') {
            return $defaultAndBranchProvider;
        }
        if (SUITE_NAME === 'sync-sox-snowflake') {
            return $defaultAndBranchProvider;
        }

        // it's not set - so it's likely local run
        if (SUITE_NAME === '' || SUITE_NAME === false) {
            // select based on feature
            $token = $this->getDefaultClient()->verifyToken();
            $this->assertArrayHasKey('owner', $token);
            if (in_array('protected-default-branch', $token['owner']['features'], true)) {
                return $defaultAndBranchProvider;
            }
        }

        return $onlyDefaultProvider;
    }

    /**
     * @dataProvider provideClientTypeBasedOnSuite
     */
    public function testFileList(string $devBranchType, string $userRole): void
    {
        $options = new FileUploadOptions();
        $fileId = $this->createAndWaitForFile(
            __DIR__ . '/../_data/files.upload.txt',
            $options,
            $this->testClient
        );
        $files = $this->testClient->listFiles(new ListFilesOptions());

        $this->assertNotEmpty($files);

        $uploadedFile = reset($files);
        $this->assertEquals($fileId, $uploadedFile['id']);
        $this->assertArrayHasKey('region', $uploadedFile);
        $this->assertArrayNotHasKey('credentials', $uploadedFile);
    }

    /**
     * @dataProvider provideClientTypeBasedOnSuite
     */
    public function testTagging(string $devBranchType, string $userRole): void
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $initialTags = ['gooddata', 'image'];
        $fileId = $this->testClient->uploadFile($filePath, (new FileUploadOptions())->setFederationToken(true)->setTags($initialTags));

        $file = $this->testClient->getFile($fileId);
        $this->assertEquals($initialTags, $file['tags']);

        $this->testClient->deleteFileTag($fileId, 'gooddata');

        $file = $this->testClient->getFile($fileId);
        $this->assertEquals(['image'], $file['tags']);

        $this->testClient->addFileTag($fileId, 'new');
        $file = $this->testClient->getFile($fileId);
        $this->assertEquals(['image', 'new'], $file['tags']);

        $this->testClient->addFileTag($fileId, 'new');
        $file = $this->testClient->getFile($fileId);
        $this->assertEquals(['image', 'new'], $file['tags'], 'duplicate tag add is ignored');
    }

    /**
     * @dataProvider provideClientTypeBasedOnSuite
     */
    public function testDownloadFile(string $devBranchType, string $userRole): void
    {
        $uploadOptions = (new FileUploadOptions())
            ->setFileName('testing_file_name');
        $sourceFilePath = __DIR__ . '/../_data/files.upload.txt';
        $fileId = $this->testClient->uploadFile($sourceFilePath, $uploadOptions);
        $tmpDestination = __DIR__ . '/../_tmp/testing_file_name';
        if (file_exists($tmpDestination)) {
            $fs = new Filesystem();
            $fs->remove($tmpDestination);
        }

        $this->testClient->downloadFile($fileId, $tmpDestination);

        $this->assertSame(
            file_get_contents($sourceFilePath),
            file_get_contents($tmpDestination)
        );
    }

    /**
     * @dataProvider provideClientTypeBasedOnSuite
     */
    public function testUploadAndDownloadSlicedFile(string $devBranchType, string $userRole): void
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
        $fileId = $this->testClient->uploadSlicedFile($slices, $uploadOptions);
        $tmpDestinationFolder = __DIR__ . '/../_tmp/slicedUpload/';
        $fs = new Filesystem();
        if (file_exists($tmpDestinationFolder)) {
            $fs->remove($tmpDestinationFolder);
        }
        $fs->mkdir($tmpDestinationFolder);

        $donwloadFiles = $this->testClient->downloadSlicedFile($fileId, $tmpDestinationFolder);
        $this->assertFileEquals($slices[0], $donwloadFiles[0]);
        $this->assertFileEquals($slices[1], $donwloadFiles[1]);
        $this->assertFileEquals($slices[2], $donwloadFiles[2]);

        $this->testClient->deleteFile($fileId);
    }

    /**
     * @dataProvider provideClientTypeBasedOnSuite
     */
    public function testFileDelete(string $devBranchType, string $userRole): void
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $options = new FileUploadOptions();

        $fileId = $this->testClient->uploadFile($filePath, $options);
        $file = $this->testClient->getFile($fileId);

        $this->testClient->deleteFile($fileId);

        try {
            $this->testClient->getFile($fileId);
            $this->fail('File should not exists');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.files.notFound', $e->getStringCode());
        }
        $this->expectExceptionCode(404);
        $this->expectException(\GuzzleHttp\Exception\ClientException::class);
        (new Client())->get($file['url']);
    }
}
