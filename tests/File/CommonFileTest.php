<?php

namespace Keboola\Test\File;

use GuzzleHttp\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use Keboola\Test\StorageApiTestCase;

use \Keboola\StorageApi\Options\FileUploadOptions;
use \Keboola\StorageApi\Options\ListFilesOptions;
use Symfony\Component\Filesystem\Filesystem;

class CommonFileTest extends StorageApiTestCase
{

    public function testFileList()
    {
        $options = new FileUploadOptions();
        $fileId = $this->createAndWaitForFile(__DIR__ . '/../_data/files.upload.txt', $options);
        $files = $this->_client->listFiles(new ListFilesOptions());
        $this->assertNotEmpty($files);

        $uploadedFile = reset($files);
        $this->assertEquals($fileId, $uploadedFile['id']);
        $this->assertArrayHasKey('region', $uploadedFile);
        $this->assertArrayNotHasKey('credentials', $uploadedFile);
    }

    public function testGetFileWithoutCredentials()
    {
        $fileId = $this->createAndWaitForFile(__DIR__ . '/../_data/files.upload.txt', (new FileUploadOptions()));
        $file = $this->_client->getFile($fileId, (new GetFileOptions())->setFederationToken(false));
        $this->assertArrayNotHasKey('credentials', $file);
    }

    public function testFilesListFilterByTags()
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';

        $this->createAndWaitForFile($filePath, new FileUploadOptions());
        $tag = uniqid('tag-test');
        $fileId = $this->createAndWaitForFile($filePath, (new FileUploadOptions())->setTags(array($tag)));

        $files = $this->_client->listFiles((new ListFilesOptions())->setTags(array($tag)));

        $this->assertCount(1, $files);
        $file = reset($files);
        $this->assertEquals($fileId, $file['id']);

        $tag2 = uniqid('tag-test-2');
        $fileId2 = $this->createAndWaitForFile($filePath, (new FileUploadOptions())->setTags(array($tag, $tag2)));

        $files = $this->_client->listFiles((new ListFilesOptions())->setTags(array($tag, $tag2)));
        $this->assertCount(2, $files, 'files with one or more matching tags are returned');
        $file2 = array_shift($files);
        $file = array_shift($files);
        $this->assertEquals($fileId2, $file2['id']);
        $this->assertEquals($fileId, $file['id']);
    }

    public function testFilesListFilterByInvalidValues()
    {
        try {
            $this->_client->apiGet('storage/files?' . http_build_query([
                    'tags' => 'tag',
                ]));
            $this->fail('Validation error should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.validation', $e->getStringCode());
        }
    }

    public function testSetTagsFromArrayWithGaps()
    {
        $file = $this->_client->prepareFileUpload((new FileUploadOptions())
            ->setFileName('test.json')
            ->setFederationToken(true)
            ->setTags([
                0 => 'neco',
                12 => 'another',
            ]));
        $this->assertEquals(['neco', 'another'], $file['tags']);
    }

    public function testFileListSearch()
    {

        $fileId = $this->_client->uploadFile(__DIR__ . '/../_data/users.csv', new FileUploadOptions());
        $this->createAndWaitForFile(__DIR__ . '/../_data/files.upload.txt', new FileUploadOptions());

        $files = $this->_client->listFiles((new ListFilesOptions())->setQuery('users')->setLimit(1));

        $this->assertCount(1, $files);
        $file = reset($files);
        $this->assertEquals($fileId, $file['id']);
    }

    public function testSyntaxErrorInQueryShouldReturnUserError()
    {
        try {
            $this->_client->listFiles((new ListFilesOptions())->setQuery('tags[]:sd'));
            $this->fail('exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('query.syntax', $e->getStringCode());
        }
    }

    public function testFileListFilterBySinceIdMaxId()
    {
        $files = $this->_client->listFiles((new ListFilesOptions())
            ->setLimit(1)
            ->setOffset(0));

        $lastFile = reset($files);
        $lastFileId = $lastFile['id'];

        $firstFileId = $this->createAndWaitForFile(__DIR__ . '/../_data/users.csv', new FileUploadOptions());
        $secondFileId = $this->createAndWaitForFile(__DIR__ . '/../_data/users.csv', new FileUploadOptions());

        $files = $this->_client->listFiles((new ListFilesOptions())->setSinceId($lastFileId));
        $this->assertCount(2, $files);

        $this->assertEquals($firstFileId, $files[1]['id']);
        $this->assertEquals($secondFileId, $files[0]['id']);

        $files = $this->_client->listFiles((new ListFilesOptions())->setMaxId($secondFileId)->setLimit(1));
        $this->assertCount(1, $files);
        $this->assertEquals($firstFileId, $files[0]['id']);
    }

    public function testFileListFilterByRunId()
    {
        $options = new FileUploadOptions();
        $options->setFileName('upload.txt')
            ->setFederationToken(true);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $fileId = $this->createAndWaitForFile(__DIR__ . '/../_data/users.csv', $options);
        $file = $this->_client->getFile($fileId);
        $this->assertEquals($runId, $file['runId']);

        $files = $this->_client->listFiles((new ListFilesOptions())->setRunId($runId));

        $this->assertCount(1, $files);
        $this->assertEquals($file['id'], $files[0]['id']);
    }

    public function testEmptyFileUpload()
    {
        $options = new FileUploadOptions();
        $filePath = __DIR__ . '/../_data/empty.csv';
        $fileId = $this->_client->uploadFile($filePath, $options);
        $file = $this->_client->getFile($fileId);

        $this->assertEquals($options->getIsPublic(), $file['isPublic']);
        $this->assertEquals(basename($filePath), $file['name']);
        $this->assertEquals(filesize($filePath), $file['sizeBytes']);
        $this->assertEquals(file_get_contents($filePath), file_get_contents($file['url']));

        $tags = $options->getTags();
        sort($tags);
        $fileTags = $file['tags'];
        sort($fileTags);
        $this->assertEquals($tags, $fileTags);

        $info = $this->_client->verifyToken();
        $this->assertEquals($file['creatorToken']['id'], (int)$info['id']);
        $this->assertEquals($file['creatorToken']['description'], $info['description']);
        $this->assertEquals($file['isEncrypted'], $options->getIsEncrypted());

        if ($options->getIsPermanent()) {
            $this->assertNull($file['maxAgeDays']);
        } else {
            $this->assertInternalType('integer', $file['maxAgeDays']);
            $this->assertEquals(StorageApiTestCase::FILE_LONG_TERM_EXPIRATION_IN_DAYS, $file['maxAgeDays']);
        }

        // check attachment, download
        $client = new Client();
        $response = $client->get($file['url']);
        $this->assertStringStartsWith('attachment', (string) $response->getHeader('Content-Disposition')[0]);
    }

    /**
     * with compress = true
     */
    public function testFileUploadCompress()
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $fileId = $this->_client->uploadFile($filePath, (new FileUploadOptions())->setCompress(true));
        $file = $this->_client->getFile($fileId);

        $this->assertEquals(basename($filePath) . ".gz", $file['name']);

        $gzFile = gzopen($file['url'], "r");
        $this->assertEquals(file_get_contents($filePath), gzread($gzFile, 524288));
    }

    public function testFileUploadLargeFile()
    {
        $filePath = __DIR__ . '/../_tmp/files.upload.large.csv';
        $fileHandle = fopen($filePath, "w+");
        for ($i = 0; $i < 5000000; $i++) {
            fputs($fileHandle, "0123456789");
        }
        fclose($fileHandle);
        $fileId = $this->_client->uploadFile($filePath, new FileUploadOptions());
        $file = $this->_client->getFile($fileId);

        $this->assertEquals(basename($filePath), $file['name']);
        $this->assertEquals(hash_file('md5', $filePath), hash_file('md5', $file['url']));
    }

    public function testFileDelete()
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $options = new FileUploadOptions();

        $fileId = $this->_client->uploadFile($filePath, $options);
        $file = $this->_client->getFile($fileId);

        $this->_client->deleteFile($fileId);

        try {
            $this->_client->getFile($fileId);
            $this->fail('File should not exists');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.files.notFound', $e->getStringCode());
        }
        $this->expectExceptionCode(404);
        $this->expectException(\GuzzleHttp\Exception\ClientException::class);
        (new Client())->get($file['url']);
    }

    public function testNotExistingFileUpload()
    {
        try {
            $this->_client->uploadFile('invalid.csv', new FileUploadOptions());
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('fileNotReadable', $e->getStringCode());
        }
    }

    public function testFilesPermissions()
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $uploadOptions = new FileUploadOptions();

        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('Files test')
        ;

        $newTokenId = $this->_client->createToken($tokenOptions);
        $newToken = $this->_client->getToken($newTokenId);

        $this->createAndWaitForFile($filePath, $uploadOptions);

        $totalFilesCount = count($this->_client->listFiles());
        $this->assertNotEmpty($totalFilesCount);

        // new token should not have access to any files
        $newTokenClient = $this->getClient([
            'token' => $newToken['token'],
            'url' => STORAGE_API_URL
        ]);

        $this->assertEmpty($newTokenClient->listFiles());

        $newFileId = $this->createAndWaitForFile($filePath, $uploadOptions, $newTokenClient);
        $files = $newTokenClient->listFiles();
        $this->assertCount(1, $files);
        $this->assertEquals($newFileId, reset($files)['id']);

        // new file should be visible for master token
        $files = $this->_client->listFiles();
        $this->assertEquals($newFileId, reset($files)['id']);

        $this->_client->dropToken($newTokenId);
    }

    public function testFilesPermissionsCanReadAllFiles()
    {
        $uploadOptions = new FileUploadOptions();
        $uploadOptions->setFileName('test.txt')
            ->setFederationToken(true);
        $file = $this->_client->prepareFileUpload($uploadOptions);

        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('Files test')
            ->setCanReadAllFileUploads(true)
        ;

        $newTokenId = $this->_client->createToken($tokenOptions);
        $newToken = $this->_client->getToken($newTokenId);

        // new token should not have access to any files
        $newTokenClient = $this->getClient([
            'token' => $newToken['token'],
            'url' => STORAGE_API_URL
        ]);

        $file = $newTokenClient->getFile($file['id']);
        $this->assertNotEmpty($file);

        $this->_client->updateToken(
            (new TokenUpdateOptions($newTokenId))
                ->setCanReadAllFileUploads(false)
        );

        $token = $this->_client->getToken($newTokenId);
        $this->assertFalse($token['canReadAllFileUploads']);

        try {
            $newTokenClient->getFile($file['id']);
            $this->fail('Access to file should be denied');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        $files = $newTokenClient->listFiles();
        $this->assertEmpty($files);
    }

    public function testsDuplicateTagsShouldBeDeduped()
    {
        $uploadOptions = new FileUploadOptions();
        $uploadOptions
            ->setFileName('test.txt')
            ->setFederationToken(true)
            ->setTags(['first', 'first', 'second']);
        $file = $this->_client->prepareFileUpload($uploadOptions);
        $file = $this->_client->getFile($file['id']);
        $this->assertEquals(['first', 'second'], $file['tags']);
    }



    public function testDownloadFile()
    {
        $uploadOptions = (new FileUploadOptions())
            ->setFileName('testing_file_name');
        $sourceFilePath = __DIR__ . '/../_data/files.upload.txt';
        $fileId = $this->_client->uploadFile($sourceFilePath, $uploadOptions);
        $tmpDestination = __DIR__ . '/../_tmp/testing_file_name';
        if (file_exists($tmpDestination)) {
            $fs = new Filesystem();
            $fs->remove($tmpDestination);
        }

        $this->_client->downloadFile($fileId, $tmpDestination);

        $this->assertSame(
            file_get_contents($sourceFilePath),
            file_get_contents($tmpDestination)
        );
    }

    public function testUploadAndDownloadSlicedFile()
    {
        $uploadOptions = (new FileUploadOptions())
            ->setFileName('sliced_testing_file_name')
            ->setIsSliced(true)
        ;
        $slices = [
            __DIR__ . '/../_data/sliced/neco_0000_part_00',
            __DIR__ . '/../_data/sliced/neco_0001_part_00',
        ];
        $fileId = $this->_client->uploadSlicedFile($slices, $uploadOptions);
        $tmpDestinationFolder = __DIR__ . '/../_tmp/slicedUpload/';
        $fs = new Filesystem();
        if (file_exists($tmpDestinationFolder)) {
            $fs->remove($tmpDestinationFolder);
        }
        $fs->mkdir($tmpDestinationFolder);

        $donwloadFiles = $this->_client->downloadSlicedFile($fileId, $tmpDestinationFolder);
        $this->assertFileEquals($slices[0], $donwloadFiles[0]);
        $this->assertFileEquals($slices[1], $donwloadFiles[1]);
    }

    public function testTagging()
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $initialTags = array('gooddata', 'image');
        $fileId = $this->_client->uploadFile($filePath, (new FileUploadOptions())->setFederationToken(true)->setTags($initialTags));

        $file = $this->_client->getFile($fileId);
        $this->assertEquals($initialTags, $file['tags']);

        $this->_client->deleteFileTag($fileId, 'gooddata');

        $file = $this->_client->getFile($fileId);
        $this->assertEquals(array('image'), $file['tags']);

        $this->_client->addFileTag($fileId, 'new');
        $file = $this->_client->getFile($fileId);
        $this->assertEquals(array('image', 'new'), $file['tags']);

        $this->_client->addFileTag($fileId, 'new');
        $file = $this->_client->getFile($fileId);
        $this->assertEquals(array('image', 'new'), $file['tags'], 'duplicate tag add is ignored');
    }

    public function testReadOnlyRoleFilesPermissions()
    {
        $readOnlyClient = $this->getClientForToken(STORAGE_API_READ_ONLY_TOKEN);

        $options = new FileUploadOptions();
        $fileId = $this->createAndWaitForFile(__DIR__ . '/../_data/files.upload.txt', $options);
        $file = $this->_client->getFile($fileId);

        $filesCount = count($this->_client->listFiles(new ListFilesOptions()));
        $this->assertGreaterThan(0, $filesCount);

        $this->assertCount($filesCount, $readOnlyClient->listFiles(new ListFilesOptions()));

        try {
            $readOnlyClient->addFileTag($fileId, 'test');
            $this->fail('Files API POST request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertContains('File manipulation is restricted for your user role', $e->getMessage());
        }

        try {
            $readOnlyClient->deleteFile($fileId);
            $this->fail('Files API DELETE request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertContains('File manipulation is restricted for your user role', $e->getMessage());
        }

        $this->assertSame($file, $this->_client->getFile($fileId));
    }


    /** @dataProvider invalidIdDataProvider */
    public function testInvalidFileId($fileId)
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('File id cannot be empty');
        $this->_client->getFile($fileId);
    }

    public function invalidIdDataProvider()
    {
        return [
            [null],
            [''],
        ];
    }
}
